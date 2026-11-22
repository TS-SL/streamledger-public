# Limitations

What this project does **not** show. Each item is a deliberate scope boundary or a known gap.

## Guarantees
- **At-least-once delivery, effectively-once effects per log position.** Not exactly-once delivery, and no cross-system atomicity between Kafka and PostgreSQL. Kafka transactions are not used.
- **Single broker, replication factor 1, single PostgreSQL.** There is no durability against disk loss and no replication. A SIGKILL of either container was tested; losing a data volume was not.
- **Cross-order `event_id` collisions** (the same id on two orders, so in two partitions) are detected as CONFLICT for the loser. Which copy wins depends on cross-partition interleaving, so all three implementations can disagree. This is excluded from the fault model and never generated.
- **CreateTime timestamps.** Lateness (`late_*` columns) trusts the producer's clock. A producer with a wrong clock mislabels lateness. It cannot change admission or totals.
- **Pending dependencies never expire.** By design, an unresolved refund waits forever and is reported. There is no operational workflow to close such items.

## Verification
- **Validation is shared.** All three implementations use `domain/events.parse_event`, so a parser bug would be consistent everywhere and reconciliation would not catch it. The parser has its own unit tests.
- **Same origin.** The reference, processor and Spark rebuild were all written within this project, from the same written contract. Their independence is structural (different algorithms and no imports), not organisational. No independent human review has taken place.
- **Mutation checks are hand-picked** (≈40 mutants in total), not a systematic mutation-testing tool.
- **Container-kill tests are opt-in and local only.** CI cannot control its service containers.
- **CI:** the workflow (`.github/workflows/ci.yml`) has not been observed passing on GitHub's runners for this release. A green run counts only if its report shows the stack tests executed and none skipped.
- **Reconciliation proves consistency up to the database's committed progress**, not that the consumer has caught up, and a database must project a single topic. See consistency-contract.md section 8.

## Performance
- **Every number comes from one laptop** (Snapdragon X Elite, WSL2, about 0.5 GB of free host RAM) with one run per configuration, at revision `1cdab24`; none was reproduced for this release. The same 1-consumer configuration measured 94.2, 92.5 and 83.3 records/s in three runs, so differences under ~12 % are noise. They are not a scalability claim.
- **Transaction-time percentiles** are exact in benchmark runs (every sample kept). A long-running consumer keeps a uniform sample of at most 10,000 durations, so its percentiles are estimates; `count`, `total` and `max` stay exact.
- The processor commits one PostgreSQL transaction per record with synchronous commit. Throughput is bounded by round trips and commit latency (see [verification-history.md](verification-history.md)). Batching several records per transaction is an obvious next step and was not done.
- Recovery time after a hard kill is dominated by the consumer group waiting out the dead member's session timeout (10 s), (see [verification-history.md](verification-history.md)).

## Not built
- No schema registry, no Avro or Protobuf, no multi-version consumers (v1 only; other versions are INVALID).
- No monitoring stack. Observability is the consumer's stats JSON, logs, and the SQL in `sql/queries/`.
- No authentication or TLS. Ports are bound to 127.0.0.1, and the database password is a throwaway local value.
- No real payment data, ever. Everything is synthetic.
