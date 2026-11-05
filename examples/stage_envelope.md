# Transparent worker-stage envelopes

```sh
cargo run --features integration --example stage_envelope
```

The example starts two localhost Flight workers, creates its own small Parquet input,
and wraps decoded worker stages using the existing worker-plan rewrite hook. It reuses
the integration fixture `TestStageEnvelope` instead of duplicating an execution-plan
implementation. Worker-reported root metrics show that the envelope remains outside
the runtime producer head.

Register your own unary envelope type using
`register_distributed_stage_envelope::<MyEnvelope>(&mut config)` on both coordinator
and worker sessions when applying wrappers after distributed planning. Registration
is local configuration, not wire metadata. If the wrapper is serialized, its existing
custom extension codec must also be registered on both sides. This example installs
wrappers only after worker decoding, so it needs no codec.

The envelope must preserve schema, partitioning, ordering, execution properties and
partition indices. Its child replacement must recompute those properties. Preparation
checks unary shape, schema and partition count before and after reconstruction. It
rebuilds the required repartition/broadcast inside registered wrappers, including
runtime fanout changes, and inserts dynamic-planning samplers under the exchange.
Unregistered nodes retain existing behavior; arbitrary unary operators are never
assumed to be transparent.

This is an execution envelope, not a generation executor. Owning the producer head
permits future per-generation state reset, but this example does not implement reset,
generation framing or network flush semantics.
