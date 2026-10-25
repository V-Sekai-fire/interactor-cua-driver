# Decider tools

Scripts that turn cua-tracer traces into decider-example datasets (RFD66606-2334,
`urn:oid:1.3.6.1.4.1.66606.1.2.2334`). They read a trace store of schema 5, whose sessions, spans
and views carry the UUIDv7 the tracer gave them, so a rebuild keeps every id.

| Script | What it does |
|---|---|
| `collect.exs <actions> [seed]` | Drives a running `cua-tracer serve` daemon: random Calculator keypad presses and TextEdit `set_value` / `type_text`, on macOS. Open Calculator and an untitled TextEdit document first. |
| `build_sample.exs <store> <stage> <seed> <driver commit>` | Writes `etnf/*.parquet` (the store's tables, keyed by UUIDv7 or by name) and `decider_denormalized.parquet` (one row per example, for the dataset viewer), all zstd. It refuses a store whose keys do not hold. |
| `check_sample.exs <stage> <store>` | Rechecks a built sample against its store from the files alone: zstd in every Parquet footer, UUIDv7 keys that are the store's, and view bytes and gold counts that match. Three planted faults must be caught. |

CI (`ci-trace-store`) writes a small fixture store with the tracer's own test
(`writes_a_fixture_store_for_the_decider_tools`), then builds and checks a sample from it.
