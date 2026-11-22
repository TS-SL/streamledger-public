# Architecture

```
 generator ──produce──▶ Kafka topic (3 partitions, key = order_id, CreateTime, infinite retention)
                           │                         │
            projector group│                         │ archive reader (assigned partitions)
                           ▼                         ▼
              consumer.py ── Processor          archive.py ──▶ Parquet files + manifest.json
              one DB txn per record                                   │
                           │                                          │
                           ▼                                          ▼
                 PostgreSQL projection          analytics/spark_job.py   reference/model.py
                 (orders, payments, refunds,        (PySpark rebuild)     (Python rebuild)
                  financial_projection, ledger)            │                    │
                           │                               │                    │
                           └──── reconcile.py: snapshot at committed progress = cutoff ────┘
                                     three-way diff, classified, exit 1 on any difference
```

## Components and ownership

| path | role |
|---|---|
| `domain/events.py` | wire contract v1: strict parsing, canonical JSON, sha256 digest. Shared by every reader |
| `domain/generator.py` | seeded synthetic workload: 13 scenarios plus duplicate, conflict and invalid faults |
| `storage/processor.py` | the transaction: fence, order lock, classify, apply, release pending, advance |
| `consumer.py` | Kafka loop: manual commits, DB-seeded assignment, bounded retry |
| `faults.py` | named fault hooks (test configuration only) |
| `reference/model.py` | oracle: pending-queue fold over the log; imports only `domain/events.py` |
| `analytics/admission.py`, `analytics/spark_job.py` | independent batch rebuild by ready offset, in PySpark |
| `archive.py` | Parquet archive with a verifiable completeness protocol |
| `analytics/reconcile.py`, `verify.py` | snapshot, three-way comparison, difference classification |
| `sql/migrations/` | schema; invariants as CHECKs, FKs, and deferred constraint triggers |
| `sql/queries/` | reporting and integrity queries |

## The one transaction

For each record, the processor takes the per-partition progress row lock (`FOR UPDATE`),
then a per-order advisory lock. It then classifies the event, writes the effects plus one
ledger row, advances `next_offset` and commits. The Kafka commit happens afterwards and is
only advisory. Every state read happens under the order lock, which is why
READ COMMITTED is enough. The full protocol and the crash-window table are in
`consistency-contract.md`.

## Three implementations of one contract

| | streaming processor | Python reference | PySpark rebuild |
|---|---|---|---|
| state | PostgreSQL rows | in-memory dicts | DataFrames |
| dedup | `received_events` PK + digest | dict by event_id | window over event_id |
| deferral | durable pending rows, released in admission order | in-memory queue, same rule | no queue: ready offset per event, then sort |
| money | incremental updates + projection upserts | incremental | SQL aggregation of accepted events |

The pending-queue formulation and the ready-offset formulation agree only if the
contract really is deterministic in admission order. Agreement between them is
evidence for that, not just for the code.
