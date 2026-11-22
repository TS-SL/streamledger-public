# Verification history

What was observed while the project was built, kept so the numbers in the README
can be traced. **Nothing in this file was reproduced for this release** unless it
says so. Everything below comes from runs on the development machine described in
[environment.md](environment.md), at source revision `1cdab24` or earlier, one run
per configuration.

Labels: VERIFIED = a test or command observed it. MEASURED = a number from a run (n
stated). ESTIMATED = reasoned, not measured.

## Test-suite runs (retained)

| scope | result | notes |
|---|---|---|
| full suite with the stack up, container kills enabled (`STREAMLEDGER_ALLOW_STACK_KILL=1 pytest`) | exit 0, 120 tests, all passed, none skipped | the `-q` in addopts was doubled on the command line, which suppressed pytest's summary; the count was taken from the progress dots in the saved log |
| same, before reconciliation existed | 100 passed in 342 s | |
| Windows without Kafka/PostgreSQL | 44 passed, 2 skipped (module-level skips: no psycopg) | |

No CI run of any revision has been observed. See the README for what CI checks now.

## Measurements behind the README table (retained)

Workload for all streaming runs: generator seed 81, 3,000 orders = 8,465 records
(5 % duplicates, 1 % conflicts, 1 % invalid), 3 partitions. PostgreSQL with
`synchronous_commit=on` and `fsync=on`; Kafka producer `acks=all`, idempotent; one
broker, replication factor 1. Raw values:
[run 1](../results/benchmarks/run-1-all-experiments.redacted.json) (all experiments) and
[run 2](../results/benchmarks/run-2-replay-rerun.redacted.json) (replay only, started 448 s after run 1).

- **1 consumer:** 94.2 records/s; transaction p50 9.39 / p95 17.77 / p99 28.91 ms; 0 retries.
- **Same configuration again** (first pass of the replay experiments): 92.5 records/s in run 1 and **83.3 records/s in run 2**. Treat ±12 % as run-to-run noise on this machine; the 1-consumer figure is not more precise than that.
- **2 consumers, one group:** 155.4 records/s; p50 8.69 ms; 0 deadlocks; 0 of 109 lock-wait samples.
- **Replay of the full history** (every record republished, so the topic holds 16,930 records and the consumer processes the second 8,465): 129.8 records/s, p50 6.80 ms; outcomes 8,305 DUPLICATE, 77 CONFLICT, 83 INVALID, nothing APPLIED, REJECTED or PENDING. Run 1's replay figure of 281.3 records/s is **wrong**: it divided the full topic size by the wall time. Run 2 is the corrected rerun.
- **Hard kill:** `kill -9` after 3,386 of 8,465 records; after restart, first progress in 8.25 s and settled in 65.08 s. The 10 s `session.timeout.ms` of the dead group member accounts for most of the delay before first progress (ESTIMATED from the timing, not traced).
- **Latency floor:** `SELECT 1` p50 0.30 ms; one durable single-row write transaction p50 3.42 ms. About a dozen statements per record plus one commit is consistent with ~9.4 ms per record (ESTIMATED; not profiled per statement).
- **Offline reconciliation** of 8,397 records with the stack stopped: exact match on all three comparisons; 30.15 s wall, 218 MB max RSS; Python reference 1.31 s, Spark session 5.57 s, Spark rebuild 20.93 s ([report](../results/reconciliation/offline-8397-records.reconcile.json), [timing](../results/reconciliation/offline-8397-records.time.redacted.txt)). The archive was 440 KB.
- **Container memory under load:** Kafka 401–432 MiB of a 1 GiB limit, PostgreSQL 61–63 MiB of 512 MiB; benchmark process ≤ 85 MB RSS.
- **Recovery timings, smaller workloads** (~580–1,140 records): hard kill at `after_lock` / `before_commit` / `after_commit` then restart: 13.7 / 13.7 / 13.8 s to settled. PostgreSQL SIGKILL: consumer halted 4.3 s after the kill (bounded retries), 10.2 s from restart to settled. Kafka SIGKILL with restart 5 s later: the same consumer process settled 37.4 s after the broker came back.
- **Query plans** at 4,000 orders / 11,142 records ([explain.txt](../results/query-plans/explain.txt)): hot-path lookups are index scans at 0.04–0.06 ms; sequential scans only on 3-row and 132-row tables; reports 0.1–51 ms; `ledger_integrity.sql` returns no defect rows. One consumer took 134 s (≈83 records/s), median transaction 10.6 ms. Transaction time is ~200× the summed query execution time, so the cost is round trips and commit fsync, not plans.

## Defects found during development (retained)

- **Stale Kafka offset after recovery.** A partition that the killed process had already finished in the database kept its old Kafka offset (observed: Kafka 200, DB 217), so lag was over-reported forever. Correctness was unaffected. Fixed in `consumer._on_assign`; regression `test_partition_finished_by_previous_owner_gets_kafka_offset_caught_up` (deterministic; an earlier version depended on where a SIGKILL landed and let a mutant survive).
- **Spark job depended on an activated virtualenv.** Python workers started with the system interpreter, which lacked pandas/pyarrow. Fixed by setting `PYSPARK_PYTHON` to the driver's interpreter.
- **Test isolation.** Parity tests left state behind that collided with later schema tests (5 failures in one run). Fixed by resetting state after each test as well as before.
- **"Member leaves mid-stream" did not move partitions mid-stream.** Rewritten so the stayer joins first and the assignment goes all → subset → all.
- **Infinite retry loop hung the bounded-retry test** instead of failing it. The consumer now runs in a thread with a 60 s deadline in that test.
- **Invalid mutation run.** Shell word-splitting broke the pytest arguments, so every mutant "failed" with a usage error and was counted as killed. `scripts/mutation_check.sh` now requires a passing baseline and counts only exit code 1 as a kill; all mutants were rerun with it.
- **Benchmark rate calculation** (above): replay rate divided by topic size instead of records processed.

## Defects found while preparing this release

These were reproduced and fixed in this release; see the README's change list.

- `reconcile.compare()` accepted a snapshot of topic A when asked to compare topic B, and reported `match: true` when the two projections happened to be equal (reproduced with both empty and identical non-empty state, 118 records).
- The end-to-end reconciliation test carried both the `integration` and `spark` markers. The integration CI job excluded `spark`, and the Spark job only collected `tests/analytics/`, so no CI job ran it.
- The consumer appended every transaction duration to a list (about 33 bytes per record: 65 MB after 2 million records in a synthetic loop), so a long-running consumer grew without bound.

## Mutation checks (retained)

Hand-picked mutants, applied one at a time by `scripts/mutation_check.sh` and reverted.
About 40 in total; not a systematic mutation-testing tool.

- **Reference model:** 6/6 killed (repeated event_id treated as identical replay; doubled over-capture limit; no over-refund check; no late attribution; released pending events never removed; pending release not restarted after a state change). The last one first survived; `test_cascading_release_restarts_so_earlier_deferred_event_wins_capacity` was added.
- **Processor:** 8/8 killed. Treating conflicts as duplicates, disabling the over-capture check, disabling late attribution and removing the progress fence were caught by the pipeline tests. Not restarting the pending release *survived* those tests; `tests/integration/test_rule_parity.py` was added and kills it, together with disabling the pending limit, not updating a pending reason and disabling the currency check.
- **Consumer/recovery:** 6/6 killed (seek from 0 instead of the DB row; no Kafka catch-up; wrong transient exception class; infinite retry; progress +2; no transaction).
- **Batch admission:** 6/7 killed. The survivor (`w > off` → `w >= off`) is equivalent: a waiting event cannot be released by its own arrival. Two parity scenarios were added after two mutants first survived.
- **Spark aggregation:** 5/5 killed (no grace period; dedup order reversed; conflicts as duplicates; per-payment refunds dropped; cancellation ignored).

## Known coverage gaps (retained)

- The random workload never produces `CANCELLED_WITH_BALANCE` or a final `MISSING_ORDER`; unit tests cover both.
- Cross-order `event_id` collisions are excluded by the contract and are not generated.
- All three implementations share the wire parser (`domain/events.parse_event`), so validation is not cross-checked by reconciliation.
