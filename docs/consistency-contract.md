# Consistency contract

Sections 1–7 are implemented in `src/streamledger/storage/processor.py` and
`src/streamledger/consumer.py`. `tests/integration/` covers duplicates, conflicts,
republished history, replay with the progress fence removed, and a restart after
Kafka offsets were lost. `tests/recovery/` covers every row of the section 6
crash-window table, plus rebalances, a zombie owner, bounded retry, and SIGKILL of
the real PostgreSQL and Kafka containers. Section 8 is implemented in `archive.py`
and `analytics/`.

## 1. The guarantee, stated narrowly

Delivery from the broker is **at-least-once**. The system guarantees that the
**business effects** of each log position are applied to PostgreSQL **at most once**,
and that every position is eventually accounted for: applied, deferred, rejected,
or recorded as invalid. Together that makes the effects **effectively once per log
position**, and idempotent per `event_id` across positions.

It is **not** exactly-once message delivery. Kafka transactions are not used,
because they cannot make a PostgreSQL commit atomic with a Kafka offset commit.

## 2. Failures the design tolerates

| # | failure | mechanism |
|---|---------|-----------|
| F1 | at-least-once redelivery of the same record | per-partition progress fence + `event_id` dedup |
| F2 | producer retry re-sends an identical event at a new offset | `event_id` + digest dedup → DUPLICATE |
| F3 | reused `event_id` with a different payload | digest mismatch → CONFLICT, stored, no effect |
| F4 | causally out-of-order events within an order (refund before payment, payment before order) | durable pending dependencies, released in log order |
| F5 | events whose `event_time` is in an earlier reporting period | event-time reporting + `late_*` restatement columns |
| F6 | consumer process crash at any point | single DB transaction per record; DB-stored progress; replay |
| F7 | DB transaction rollback (error, deadlock, serialization failure) | nothing committed; record retried; bounded retries then halt |
| F8 | crash after DB commit, before Kafka offset commit | DB progress row is authoritative; replayed position is skipped |
| F9 | consumer restart and full or partial replay | same as F1/F8; replay from offset 0 is safe |
| F10 | rebalance / partition reassignment, including a "zombie" previous owner still running | row lock on the progress row serializes owners; stale positions are skipped |
| F11 | invalid / poison payloads | durable INVALID row with raw bytes and position, then advance |
| F12 | backlog growth | bounded poll size, bounded pending per order, lag metric |

## 3. Explicitly not tolerated

- Permanent loss or truncation of broker data (unclean leader election, retention expiring before consumption, deleting the topic). There is one local broker, so there is no replication.
- Loss or corruption of the PostgreSQL data directory. No backups are in scope.
- Malicious or manual changes to tables, offsets, or the archive.
- Producers that violate the key rule (key ≠ order_id) are *detected* as INVALID. Producers that publish events of one order to several topics are not supported.
- Reuse of an `event_id` across two **different orders**. That collision lands in two partitions, and which copy wins depends on cross-partition processing interleaving. It is detected as CONFLICT for the loser, but it is **not** deterministic. Reconciliation must classify it rather than expect a match.
- Any claim of distributed consensus, multi-node durability, or cross-system atomicity.

## 4. Admission order: why the result is deterministic

Every decision that depends on prior state (dedup, over-capture, over-refund,
duplicate ids, pending release) is local to one `order_id`. Events are keyed by
`order_id`, so all events of an order are in **one partition**, in a fixed offset
order. That per-order log order is the **admission order**:

- Capacity conflicts are resolved first-come in admission order. For example, two refunds that together exceed a payment: the earlier offset wins.
- Deferred events are released in their **original** admission order. Their release time never matters.
- `event_time` decides **reporting** (which period). It never decides **admission**. Wall-clock processing time decides neither.

**Record timestamps.** The topic uses `CreateTime`, so the timestamp is the producer's
send time, which the synthetic workload simulates. It is written into the log once and
never changes, and every reader (consumer, reference, archive) sees the same value.
That is all determinism needs. `LogAppendTime` would resist producer clock errors
better, but it stamps every record of a replayed historical workload with the
real-world append time, which makes every January event look "late". The trade-off:
a producer with a wrong clock can mislabel lateness. This is accepted because the
data is synthetic. Lateness only affects the `late_*` reporting columns, never admission.

So the final state is a pure function of the log contents. It does not depend on
when records were consumed, how many times, or by which consumer instance. The
reference model (`src/streamledger/reference/model.py`) computes that function
directly from the log, which makes it the oracle for streaming and batch results.

## 5. Settled state

For a snapshot of end offsets `E = {p: E_p}`:

> The system is **settled at E** when, for every partition `p`, the committed
> PostgreSQL progress row has `next_offset = E_p` and no consumer transaction is open.

When settled at E, the PostgreSQL projections must equal `reference(log[p][0:E_p] for all p)`
exactly: every order, payment, refund, period total, outcome count, conflict, and
unresolved pending event. "Unresolved pending" is a valid settled state. Those
events are reported, never silently dropped.

## 6. Processing protocol

**Position ledger.** Every processed log position leaves exactly one durable row, in
`received_events` (first occurrence), `duplicate_deliveries`, `event_conflicts`, or
`processing_errors`. The `position_ledger` view unions them. For a settled partition,
the ledger row count equals `next_offset`. Progress is keyed by `(topic, partition)`
only: it belongs to the projection, not to a consumer group. A new group replaying the
topic therefore hits the same fence. If the fence row is lost, a position already in
the ledger is recognised and returns `REPLAYED` without writing anything.

The commit order, per record (batches are a sequence of these within one partition):

1. Poll a record (bounded `max.poll.records`).
2. Validate in memory. No I/O.
3. `BEGIN` (READ COMMITTED).
4. `SELECT next_offset FROM consumer_progress WHERE topic=$1 AND partition=$2 FOR UPDATE`.
   - `offset < next_offset`: the position is already processed. `ROLLBACK`, skip (counted as a positional replay).
   - `offset > next_offset`: gap. This should be impossible. `ROLLBACK` and raise `GapError`. It is not a transient error, so the consumer stops without advancing anything; a restart seeks to `next_offset` again.
5. Lock the order: `pg_advisory_xact_lock(hash(order_id))`. A per-order lock covers orders that do not exist yet.
6. Look up `received_events(event_id)` → DUPLICATE / CONFLICT / new.
7. Decide APPLY / DEFER / REJECT / INVALID and write the effects. If something was applied, also re-evaluate this order's pending events in admission order.
8. `UPDATE consumer_progress SET next_offset = offset + 1`.
9. `COMMIT`.
10. Kafka `commit({tp: offset+1})`, synchronous. The implementation commits once per consumed batch, for the contiguous prefix whose DB transactions committed. It is **advisory**: it feeds lag monitoring only.

Steps 3–9 run in one PostgreSQL transaction per record. The deferred constraint
triggers (order totals) and the deferred pending FK are checked at step 9. A
violation there rolls back the whole record and halts the partition. That is the
schema acting as a backstop for a processor bug.

On partition assignment the consumer seeks to `consumer_progress.next_offset`.
If that row is missing, nothing of the partition is durable in the projection, and it
starts at offset 0. The Kafka committed offset is never used as a starting point.
On revocation it stops after the current transaction and commits the Kafka offsets
of finished positions only.

### Crash-window analysis

| crash point | durable state afterwards | recovery path |
|---|---|---|
| before BEGIN | nothing for this record | redelivered from the last committed position; processed normally |
| during the transaction | PostgreSQL rolls back on disconnect | redelivered; processed normally |
| after COMMIT, before Kafka commit | effects + progress committed; Kafka offset is behind | on restart, seek to DB `next_offset` (past it). If something seeks to the Kafka offset, step 4 skips the position |
| during Kafka commit | same as above; Kafka offset may or may not be updated | same as above. Kafka offsets are never trusted over the DB |
| after Kafka commit | fully consistent | nothing to do |
| during rebalance, old owner still in its transaction | old owner holds the progress row lock | new owner blocks on step 4, then sees an advanced `next_offset` and skips. If the old owner rolls back, the new owner applies the position |
| during rebalance, old owner (zombie) resumes later | the zombie's next transaction locks the row and sees `offset < next_offset` | the zombie's write is skipped. It cannot double-apply |

Independent second line of defence: even with the progress fence removed,
`received_events.event_id` uniqueness makes every replayed event a DUPLICATE.

### Which test covers which row

| crash point | test (`tests/recovery/`) | asserted |
|---|---|---|
| before BEGIN | `test_in_process_crash_at_boundary[before_begin]` | progress = N−1; ledger rows = progress; converges |
| during the transaction | `…[after_lock]`, `…[before_commit]`, `test_hard_process_death_at_boundary[after_lock / before_commit]` (os._exit) | nothing of record N persisted; no session left idle in a transaction; converges |
| after COMMIT, before Kafka commit | `…[after_commit]` (both variants) | progress = N; Kafka ≤ DB; restart re-reads nothing (0 SKIPPED, 0 REPLAYED) |
| after Kafka commit | `test_crash_right_after_kafka_commit` | Kafka = DB exactly |
| arbitrary point | `test_external_sigkill_mid_stream` | converges; Kafka = DB after settle |
| rebalance: new owner while old is mid-transaction | `test_new_owner_blocks_behind_zombie_transaction[zombie-commits / zombie-rolls-back]` | new owner waits on the row lock (`pg_stat_activity`), then SKIPPED or applies |
| zombie keeps writing | `test_two_writers_on_one_partition_apply_each_position_once` | each position non-SKIPPED exactly once |
| group rebalance | `test_two_members_share…`, `test_member_leaves_mid_stream…`, `test_member_dies…` | converges; partitions actually moved |
| crash during a cascading pending release | `test_crash_inside_cascading_release_rolls_back_the_whole_release` | release is all-or-nothing |
| PostgreSQL / Kafka SIGKILL | `test_postgres_killed_mid_stream`, `test_kafka_killed_mid_stream_consumer_survives` (opt-in) | bounded halt, or survival; converges |

**Kafka offset after recovery (a bug found during recovery testing).** If the killed process had
already finished a partition in the database, its last Kafka commit could still sit at
an earlier batch boundary. The restarted consumer seeks past all of it and never reads
a record there, so it never committed that partition. Lag stayed over-reported forever
(observed: Kafka 200 against DB 217). Correctness was unaffected. Fix: on assignment,
the DB position is queued for the next Kafka commit.

### Error classes

| class | example | handling | advances offset? |
|---|---|---|---|
| INVALID (deterministic) | malformed JSON, unknown version, bad amount | row in `processing_errors` with position, raw bytes, reason | yes, in the same transaction as the evidence |
| REJECTED (deterministic business rule) | OVER_REFUND, CURRENCY_MISMATCH | row in `received_events` with status REJECTED + reason | yes |
| DEFERRED | MISSING_PAYMENT | row in `received_events` (PENDING) + `pending_dependencies` | yes |
| transient | DB connection lost, deadlock, serialization failure | rollback, retry with bounded exponential backoff (max 5) | no |
| unexpected exception | a bug | rollback, bounded retries, then **halt the partition** (no skip) and report | no |

Advancing past a poison record is safe because its rejection is durable in the
same transaction as the progress update. Nothing is skipped without evidence.

## 7. Pending dependencies

- The dependency is `ORDER(order_id)` or `PAYMENT(order_id, payment_id)`.
- Deferred events are durable: `received_events` status PENDING, plus a `pending_dependencies` row with the admission position. A replay of a pending event is a DUPLICATE.
- After any APPLIED event on an order, that order's pending events are re-evaluated in admission order until a fixpoint. Release can cascade: payment waits for the order, then the refund waits for the payment.
- A release can end in REJECTED. For example, a refund of a payment that turned out FAILED. Every release ends in exactly one terminal status.
- Bound: 32 pending events per order. Past that, REJECTED `PENDING_LIMIT`, which is deterministic in admission order.
- There is no time-based expiry, because expiry would make the state depend on wall-clock time. Unresolved events stay visible in diagnostics and in the settled-state comparison.

## 8. Archive and reconciliation

The Parquet archive is written by a **separate reader** (assigned partitions, no shared group
with the projector) straight from the log (raw key, value, partition, offset, broker timestamp), in contiguous offset
ranges per partition. Each file is written to a temporary name, fsynced, renamed,
and listed in a manifest together with its range and sha256. The archive is
**complete up to E** when the manifest ranges cover `[0, E_p)` contiguously for every partition.

The archive is **not** atomic with PostgreSQL or with Kafka commits. Fairness comes
from comparing only at a common cutoff E: the archive is complete up to E, and the
streaming consumer is settled at E. Any difference is a test failure, classified by
field and order. It is never averaged away.

The comparison is three-way. The PostgreSQL projection is snapshotted at its own
committed progress, in one REPEATABLE READ transaction, and that progress is the
cutoff. The PySpark job and the Python reference each rebuild the state from the
archive below that cutoff. Spark runs after the streaming stack is stopped, or
alongside it on small test data. `reconcile.py compare` exits 1 on any difference
and reports each one by section and kind (`orders:mismatch`, `positions:missing`, ...).

### What a reconciliation does and does not prove

- **Identity.** A snapshot records the topic it was taken for. `compare` refuses to run (exit 2, `SnapshotMismatch`) when the snapshot's topic is missing or differs from the requested topic, before reading the archive or starting Spark. Equal projections are not evidence of the right snapshot: two topics can project to identical state, for example both empty.
- **Cutoff, not completeness.** The cutoff is the projection's own committed progress. A match proves that PostgreSQL equals both rebuilds of the log *below that cutoff*. It does not prove the consumer has caught up with the topic; compare the report's `cutoff` with the topic's end offsets for that (the end-to-end test asserts `records == sum(end offsets)`).
- **One topic per database.** Progress, positions and conflicts are keyed by topic, but `orders`, `payments`, `refunds`, `financial_projection` and `pending_dependencies` are not. A database that projects two topics fails reconciliation for both (orders of the other topic appear as `unexpected`). It fails closed; it cannot produce a false match.
