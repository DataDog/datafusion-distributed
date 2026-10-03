use std::collections::{BTreeMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::physical_plan::metrics::MetricBuilder;
use url::Url;

use super::defaults::dial_with_failover;
use crate::{
    DistributedConfig, RouteTaskEvent, RouteTaskEventResponse, RouteTaskHandler, WorkUnitAffinity,
    ok_or_some_err,
};

/// Opt-in routing of tasks toward stable, connector-supplied data homes.
///
/// Each distinct affinity key elects a worker by rendezvous hashing. The worker
/// owning the greatest total hinted weight is preferred for the whole task. URL
/// ordering and query/task IDs do not affect the choice. Worker URLs must identify
/// stable cache owners, not a load balancer. Hash stability is within a build; an
/// upgrade may change cache placement without affecting query correctness.
///
/// No cache is inspected: this improves the opportunity for reuse but does not
/// guarantee cache hits or bound worker memory. The normal classified connection
/// retry/failover policy is retained (including overload responses). Embedders that
/// need load-aware admission can use `RouteTaskEvent::work_unit_affinity()` in their
/// own routing handler. No hints or no workers defers to the next routing handler.
///
/// Register with `with_distributed_route_task_handler(AffinityRouteTaskHandler)`.
#[derive(Debug, Default)]
pub struct AffinityRouteTaskHandler;

#[async_trait]
impl RouteTaskHandler for AffinityRouteTaskHandler {
    async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        let hints = ev.work_unit_affinity();
        if hints.is_empty() {
            return None;
        }
        let urls = ok_or_some_err!(ev.worker_resolver.get_urls());
        let candidates = Self::rank_workers(&hints, &urls);
        let url = candidates.first()?.clone();
        let config = ok_or_some_err!(DistributedConfig::from_task_context(ev.task_ctx));
        MetricBuilder::new(ev.metrics)
            .global_counter("work_unit_affinity_routed_tasks")
            .add(1);
        Some(dial_with_failover(ev.dialer, url, candidates, ev.metrics, config).await)
    }
}

impl AffinityRouteTaskHandler {
    /// Ranks unique worker URLs by weighted affinity. Useful for custom routing
    /// handlers that want the same data homes but different admission policies.
    /// Zero-weight hints are ignored. Duplicate keys use their maximum weight:
    /// repeated scans reuse bytes but do not share decoded/execution state.
    pub fn rank_workers(hints: &[WorkUnitAffinity], workers: &[Url]) -> Vec<Url> {
        let mut workers = workers.to_vec();
        workers.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
        workers.dedup();
        if workers.is_empty() {
            return workers;
        }
        let mut weights = BTreeMap::<&str, u64>::new();
        for hint in hints {
            if hint.weight > 0 {
                let weight = weights.entry(&hint.key).or_default();
                *weight = (*weight).max(hint.weight);
            }
        }
        if weights.is_empty() {
            return Vec::new();
        }
        let mut scores = vec![0_u128; workers.len()];
        for (key, weight) in weights {
            let owner = workers
                .iter()
                .enumerate()
                .max_by_key(|(_, url)| {
                    let mut hasher = DefaultHasher::new();
                    (key, url.as_str()).hash(&mut hasher);
                    hasher.finish()
                })
                .expect("nonempty workers")
                .0;
            scores[owner] += u128::from(weight);
        }
        let mut ranked: Vec<_> = workers.into_iter().zip(scores).collect();
        ranked.sort_by(|(a, a_score), (b, b_score)| {
            b_score.cmp(a_score).then(a.as_str().cmp(b.as_str()))
        });
        ranked.into_iter().map(|(url, _)| url).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn homes_are_order_independent_and_repeated_keys_do_not_inflate_weight() {
        let workers: Vec<_> = (0..5)
            .map(|i| Url::parse(&format!("http://worker-{i}")).unwrap())
            .collect();
        let hints = vec![
            WorkUnitAffinity::new("s3://bucket/a", 100),
            WorkUnitAffinity::new("s3://bucket/b", 10),
        ];
        let expected = AffinityRouteTaskHandler::rank_workers(&hints, &workers);
        let mut reversed = workers.clone();
        reversed.reverse();
        reversed.push(workers[0].clone());
        assert_eq!(
            expected,
            AffinityRouteTaskHandler::rank_workers(&hints, &reversed)
        );
        let mut repeated = hints.clone();
        repeated.extend(std::iter::repeat_n(hints[1].clone(), 100));
        assert_eq!(
            expected,
            AffinityRouteTaskHandler::rank_workers(&repeated, &workers)
        );
        assert_eq!(
            expected[0],
            AffinityRouteTaskHandler::rank_workers(&hints[..1], &workers)[0]
        );
        assert!(AffinityRouteTaskHandler::rank_workers(&[], &workers).is_empty());
    }

    #[test]
    fn removing_a_non_owner_keeps_the_home_and_removing_owner_fails_over() {
        let workers: Vec<_> = (0..4)
            .map(|i| Url::parse(&format!("http://worker-{i}")).unwrap())
            .collect();
        let hints = [WorkUnitAffinity::new("immutable-object", 1)];
        let ranked = AffinityRouteTaskHandler::rank_workers(&hints, &workers);
        let without_other: Vec<_> = workers
            .iter()
            .filter(|u| **u != ranked[1])
            .cloned()
            .collect();
        assert_eq!(
            ranked[0],
            AffinityRouteTaskHandler::rank_workers(&hints, &without_other)[0]
        );
        let without_owner: Vec<_> = workers
            .iter()
            .filter(|u| **u != ranked[0])
            .cloned()
            .collect();
        let next = AffinityRouteTaskHandler::rank_workers(&hints, &without_owner);
        assert_ne!(ranked[0], next[0]);
    }
}
