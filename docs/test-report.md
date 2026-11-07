# Test report

Generated from a fresh run on 2026-11-07 13:23 UTC, repository commit `e4f4aef` (`main`). Re-run with the commands below.

| Suite | Command | Runs against | Tests | Passed | Failed | Errors | Skipped | Time |
|---|---|---|---|---|---|---|---|---|
| Unit and benchmark-smoke tests | `uv run pytest tests/unit tests/eval -q` | no infrastructure (fakes) | 261 | 261 | 0 | 0 | 0 | 89 s |
| Integration tests | `uv run pytest tests/integration -q` | real Postgres and Kafka (Testcontainers) plus the local Milvus and OpenSearch | 63 | 63 | 0 | 0 | 0 | 393 s |
