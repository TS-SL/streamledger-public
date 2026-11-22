# StreamLedger

StreamLedger consumes commerce events (orders, payment captures, refunds) from Kafka
and keeps a financial projection in PostgreSQL that stays exact when the broker
redelivers records, a producer retries, a refund arrives before its payment, or the
consumer crashes between its database commit and its Kafka offset commit. A separate
step rebuilds the same state twice from a Parquet archive of the raw log (once in
plain Python, once in PySpark) and fails if any of the three disagree.

All data is synthetic. Money is integer minor units (cents). Everything runs locally.

## One transaction sequence

Partition 1 holds the events of order `ord_7`. The database fence for that partition
is the row `consumer_progress(topic, partition=1)`, currently `next_offset = 41`.

| step | what happens | durable result |
|---|---|---|
| 1 | Offset 41, `PAYMENT_CAPTURED evt_b` of 50.00, is processed in one PostgreSQL transaction: lock the progress row, lock the order, check `evt_b` is new, insert the payment, add 50.00 to `orders.captured_minor` and to the day's `financial_projection` row, set `next_offset = 42`, `COMMIT`. | payment applied once; fence at 42 |
| 2 | The process is killed before it commits offset 42 to Kafka. Kafka's committed offset for the group is still 40. | unchanged |
| 3 | The consumer restarts. On partition assignment it reads `next_offset = 42` from PostgreSQL and seeks there. It ignores Kafka's 40. Offset 41 is not read again. | unchanged |
| 3a | If anything does re-deliver offset 41 (a new group, a manual seek to 40), the transaction sees `41 < next_offset` under the row lock and returns `SKIPPED` without writing. | unchanged |
| 4 | Offset 42 is a producer retry: the same `evt_b` with the same payload. `received_events` already holds `evt_b` with that sha256 digest, so the outcome is `DUPLICATE`. One `duplicate_deliveries` row records the position; no money moves. `next_offset = 43`. | 50.00 captured, not 100.00 |
| 5 | Offset 43 reuses `evt_b` with a different amount. Digest differs, so the outcome is `CONFLICT`: both payloads are kept in `event_conflicts`, and nothing is applied. | evidence kept, total unchanged |

Steps 1–3 are asserted by `tests/recovery/test_crash_windows.py` (`after_commit`
variants: progress equals N, Kafka ≤ DB, and the restart re-reads nothing). Step 3a is
asserted by `test_progress_fence_skips_already_processed_position`. Steps 4–5 are
asserted by `test_identical_redelivery_is_counted_once` and
`test_conflicting_payload_under_same_event_id_is_kept_as_evidence_not_applied`, and by
the end-to-end comparisons against the reference.

## How it works

```
generator ─▶ Kafka topic (3 partitions, key = order_id, CreateTime, infinite retention)
               │                                   │
   consumer group (projector)            archive reader (assigned partitions)
               ▼                                   ▼
   consumer.py → Processor                archive.py → Parquet files + manifest (sha256, ranges)
   one PostgreSQL txn per record                   │
               ▼                                   ├─▶ reference/model.py   (Python rebuild)
   PostgreSQL projection                           └─▶ analytics/spark_job.py (PySpark rebuild)
               └──────── reconcile.py: snapshot at committed progress, three-way diff ────────┘
```

**Idempotent effect boundary.** Everything a log position changes is written in one
PostgreSQL transaction: its ledger row (`received_events`, `duplicate_deliveries`,
`event_conflicts` or `processing_errors`), the order, payment, refund and
`financial_projection` updates, the release of pending events it unblocks, and the
progress row. Either all of it commits or none of it does. Outside that boundary, and
therefore not idempotent or not authoritative: the Kafka offset commit, logs, the
consumer's in-memory stats, and the archive (a separate reader with its own
completeness check).

**Authoritative progress.** `consumer_progress.next_offset`, keyed by `(topic,
partition)` and not by consumer group, is the only record of what has been applied.
Kafka committed offsets are advisory: they feed lag monitoring and are never used as a
starting point. If the progress row were lost, the ledger tables still recognise an
already-recorded position (`REPLAYED`) and `event_id` uniqueness turns any repeat into a
`DUPLICATE`.

**Partition ownership.** Kafka's group assignment decides which consumer *reads* a
partition. PostgreSQL decides which one may *write*: each transaction takes `SELECT … FOR
UPDATE` on the partition's progress row. After a rebalance, a previous owner that is
still running ("zombie") either holds the lock, so the new owner waits and then skips
what was committed, or arrives later, sees `offset < next_offset` and skips. Positions
are never applied twice.

**Deterministic rules.** Every stateful rule (dedup, over-capture, over-refund, refund
ids, pending release) is local to one order, and all events of an order share a
partition. The result is therefore a pure function of each partition's log order.
Event time decides the reporting day, never admission. A refund that arrives before its
payment is stored as `PENDING` and re-evaluated in admission order when the payment is
applied. A refund (or capture) whose record timestamp is later than the end of its
event-time day plus 1 h grace still counts toward that day, and is also counted in the
day's `late_*` restatement columns.

The rules are in [docs/event-contract.md](docs/event-contract.md). The fault model,
crash-window table and test for each row are in
[docs/consistency-contract.md](docs/consistency-contract.md). Component roles are in
[docs/architecture.md](docs/architecture.md), and the verification approach is in
[docs/methodology.md](docs/methodology.md).

## Build, run, test

Requirements: Linux or WSL2 with Docker Engine and Compose v2, Python 3.11+, Java 17 for
Spark ([docs/environment.md](docs/environment.md)). Commands are run from the
repository root.

```bash
python3 -m venv .venv
.venv/bin/pip install "pytest==8.3.3" "pyarrow==21.0.0" "psycopg[binary]==3.2.10" \
    "confluent-kafka==2.11.1" "pandas==2.2.3" "pyspark==4.0.1"
export PYTHONPATH=src
```

Tests that need no services (rules, reference model, archive, reconciliation identity
checks):

```bash
.venv/bin/python -m pytest tests/unit tests/analytics/test_archive.py tests/analytics/test_reconcile_identity.py
```

The full suite against a local Kafka and PostgreSQL:

```bash
STREAMLEDGER_VENV=.venv scripts/stack.sh up    # Kafka 127.0.0.1:19092, PostgreSQL 127.0.0.1:55432, topic, migrations
.venv/bin/python scripts/wait_ready.py         # fails if a service, Java or PySpark is missing
.venv/bin/python -m pytest -m "not stack_kill"
STREAMLEDGER_ALLOW_STACK_KILL=1 .venv/bin/python -m pytest -m stack_kill   # SIGKILLs the containers
scripts/stack.sh down                          # `wipe` also deletes the data volumes
```

Without the services, the stack tests are skipped rather than failed. Read the `-rs`
summary, or use `scripts/check_test_report.py`, which fails on any skip.

Process a topic, archive it, and reconcile offline (Spark runs after the stack stops):

```bash
.venv/bin/python -m streamledger.consumer --topic commerce-events --until-settled
.venv/bin/python -m streamledger.archive write --topic commerce-events --dir results/raw/archive
.venv/bin/python -m streamledger.analytics.reconcile snapshot --topic commerce-events --out results/raw/snap.json
scripts/stack.sh down
.venv/bin/python -m streamledger.analytics.reconcile compare --topic commerce-events \
    --snapshot results/raw/snap.json --archive results/raw/archive --out results/raw/reconcile.json
```

`compare` exits 0 on an exact match, 1 on any difference (classified as
`orders:mismatch`, `positions:missing`, …), and 2 if the snapshot was taken for a
different topic. The database password in `compose.yaml` is a throwaway value for a
loopback-only container.

**CI** (`.github/workflows/ci.yml`) has two jobs. `unit` runs the service-free tests.
`stack` runs every test except the container kills against real Kafka, PostgreSQL,
Parquet and Spark in one job, after a readiness check. It records the collected and
executed test ids and fails on any skip, on any collected test that did not run, or if
the named headline tests did not pass. Observed result for commit `36b6c90` (the first
public commit), run
[45017132640](https://github.com/TS-SL/streamledger-public/actions/runs/45017132640):
`unit` passed; `stack` collected 131 tests and executed 131, 131 passed, 0 skipped,
2 container-kill tests deselected by design.

## Results

From [run 1](results/benchmarks/run-1-all-experiments.redacted.json),
[run 2](results/benchmarks/run-2-replay-rerun.redacted.json) and the
[offline reconciliation](results/reconciliation/offline-8397-records.reconcile.json).
They were measured on one laptop (Snapdragon X Elite, WSL2, about 0.5 GB of free host
RAM) at revision `1cdab24`, one run per configuration, and **not reproduced for this
release**.

Workload: seeded generator, 3,000 orders, 3 partitions. Durability for every row:
PostgreSQL `synchronous_commit=on`, `fsync=on`; one Kafka broker, replication factor 1,
idempotent producer with `acks=all`. Throughput = records processed ÷ wall time from
consumer start until DB progress reached the end offsets, with everything already
published. Latency = duration of one record's PostgreSQL transaction, not end-to-end
event latency.

| consumers | records processed | duplicates / conflicts / invalid | throughput | txn p50 / p99 | outcome |
|---|---|---|---|---|---|
| 1 | 8,465 | 404 / 77 / 83 | **94.2 records/s** | 9.39 / 28.91 ms | settled, 0 retries |
| 1 (same config, run 2) | 8,465 | 404 / 77 / 83 | **83.3 records/s** | 10.70 / 27.99 ms | settled |
| 2 (one group) | 8,465 | 404 / 77 / 83 | **155.4 records/s** | 8.69 / 21.25 ms | settled, 0 deadlocks |
| 1, full history replayed | 8,465 | 8,305 / 77 / 83 | 129.8 records/s | 6.80 / 16.08 ms | no new effects |
| offline reconciliation | 8,397 | 68 conflicts | 30.15 s wall | n/a | **exact match**, all three |

The 1-consumer configuration ran at 94.2, 92.5 and 83.3 records/s across three runs,
so the 2-consumer figure (1.65× of 94.2, 1.87× of 83.3) shows a real gain on this
machine but not a precise speed-up. The cost per record is about a dozen round trips
plus one synchronous commit (`SELECT 1` p50 0.30 ms, one durable write transaction
p50 3.42 ms). Batching several records per transaction is the obvious next step. It
was not done, to keep the per-record protocol simple to verify. The benchmark rows
check outcome counts; equality of the final state is asserted by the test suite and
the reconciliation row. More measurements, including recovery timings and the defects
found along the way, are in [docs/verification-history.md](docs/verification-history.md).

## Limitations

- **Effectively once per log position, not exactly-once delivery.** There is no atomic commit across Kafka and PostgreSQL, and Kafka transactions are not used.
- **No replication.** One broker with replication factor 1 and one PostgreSQL. Container SIGKILL is tested; disk loss is not.
- **Reconciliation proves consistency up to the database's committed progress**, not that the consumer has caught up with the topic. A database must project a single topic: the order and money tables are not keyed by topic.
- **Cross-order `event_id` collisions** are detected as `CONFLICT`, but which copy wins depends on cross-partition timing. They are excluded from the fault model and never generated.
- **Lateness trusts producer clocks** (`CreateTime`). A wrong clock mislabels the `late_*` columns; it cannot change admission or totals.
- **Shared parser.** All three implementations use the same wire parser, so a parser bug would not be caught by reconciliation.
- **The numbers are single runs on one laptop**, with ±12 % run-to-run spread. They are not a scalability claim.

Full list: [docs/limitations.md](docs/limitations.md).

## Layout

| path | contents |
|---|---|
| `src/streamledger/` | `domain/` (event contract, generator), `storage/` (processor, snapshot), `reference/` (Python rebuild), `analytics/` (PySpark rebuild, reconciliation), `consumer.py`, `archive.py`, `faults.py`, `instrumentation.py`, `verify.py` |
| `sql/migrations/`, `sql/queries/` | schema with invariants as CHECKs, FKs and deferred constraint triggers; reporting and integrity queries |
| `tests/` | `unit/`, `integration/`, `recovery/`, `analytics/` |
| `benchmarks/run_benchmarks.py` | the measurements above; raw output goes to the git-ignored `results/raw/` |
| `results/` | published, redacted copies of benchmark, reconciliation and query-plan output |
| `scripts/` | stack control, migrations, readiness check, test-report check, mutation check, EXPLAIN report |

## License

MIT, see [LICENSE](LICENSE).
