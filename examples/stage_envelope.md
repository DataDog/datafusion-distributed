# Opaque worker-stage wrappers

```sh
cargo run --features integration --example stage_envelope
```

The example starts two localhost Flight workers, creates a small Parquet input, and
wraps decoded worker stages using the existing worker-plan rewrite hook. It reuses
`TestStageEnvelope` instead of duplicating an execution-plan implementation.
Worker-reported root metrics show that the wrapper stays outside the producer head.

It sets `DistributedConfig::rewrite_producer_head_at_execution = false` on the
coordinating session using the existing configuration-extension API. Configuration
propagation carries this flag to workers. No wrapper-type registration is needed.

With this option disabled, the worker uses its base stage plan directly, including
any wrapper, repartition, or broadcast head. Standard static distributed planning must
have already built the correct producer head. Later custom transformations must
preserve the stage's schema and output partitioning, including the required hash
expressions, ordering, and partition count. Stale heads are not repaired.

The default remains `true`, preserving existing runtime head insertion/removal.
Disabling the option together with `dynamic_task_count = true` is rejected: adaptive
planning does not know the final consumer fanout when producers start sampling.
All participating workers must run a version supporting this option. Older workers
ignore unknown configuration options and would still rewrite the producer head.

This is a narrow opt-out from late producer-head rewriting. Task specialization,
fresh operator instances, network binding and worker rewrite hooks still occur.
It does not provide general plan immutability, state reset, generation framing, or
network flush semantics. This example adds its wrapper after worker decoding and
therefore needs no custom extension codec; serialized wrappers still need one.
