"""Kafka consumer loop around the transactional Processor.

Lifecycle per batch (docs/consistency-contract.md section 6):
  consume -> for each record in partition order: one DB transaction (effects +
  progress) -> after the batch, synchronous Kafka commit of offset+1 for the
  contiguous prefix that committed in the DB. Kafka offsets are advisory; on
  assignment the consumer seeks to the DB progress row.

    python -m streamledger.consumer --topic commerce-events --until-settled
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from collections import Counter
from dataclasses import dataclass, field

import psycopg
from confluent_kafka import Consumer, KafkaException, TopicPartition
from psycopg import errors as pgerr

from streamledger.domain.events import LogRecord
from streamledger.kafka_io import DEFAULT_BOOTSTRAP
from streamledger.faults import Faults
from streamledger.instrumentation import DEFAULT_TXN_SAMPLE_LIMIT, TxnTimings
from streamledger.storage.processor import Processor
from streamledger.storage.snapshot import progress

log = logging.getLogger("streamledger.consumer")
DEFAULT_DSN = "postgresql://streamledger:streamledger_local_only@localhost:55432/streamledger"

TRANSIENT = (pgerr.SerializationFailure, pgerr.DeadlockDetected, pgerr.UniqueViolation,
             pgerr.LockNotAvailable, psycopg.OperationalError)


@dataclass
class RunStats:
    outcomes: Counter = field(default_factory=Counter)
    retries: int = 0
    kafka_commit_failures: int = 0
    assignments: list = field(default_factory=list)  # one entry per rebalance, not per record
    txn: TxnTimings = field(default_factory=TxnTimings)
    stopped_because: str = ""

    @property
    def txn_seconds(self) -> list[float]:
        """Retained samples (all of them while txn.exact)."""
        return self.txn.samples()


class StreamConsumer:
    def __init__(self, topic: str, group: str = "streamledger-projector", bootstrap: str = DEFAULT_BOOTSTRAP,
                 dsn: str = DEFAULT_DSN, faults: Faults | None = None, max_attempts: int = 5,
                 batch_size: int = 200, instance: str = "c1",
                 txn_sample_limit: int = DEFAULT_TXN_SAMPLE_LIMIT):
        self.topic, self.group, self.dsn = topic, group, dsn
        self.max_attempts, self.batch_size = max_attempts, batch_size
        self.stats = RunStats(txn=TxnTimings(txn_sample_limit))
        self.conn = psycopg.connect(dsn, autocommit=True, application_name=f"streamledger-{instance}")
        self.faults = faults or Faults()
        self.processor = Processor(self.conn, owner=group, faults=self.faults)
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "client.id": instance,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "range",
            "session.timeout.ms": 10_000,
            "max.poll.interval.ms": 60_000,
        })
        self._done: dict[int, int] = {}  # partition -> next offset durably processed in this session

    # -------------------------------------------------------------- rebalance callbacks
    def _on_assign(self, consumer, partitions):
        db = progress(self.conn, self.topic)
        for tp in partitions:
            tp.offset = db.get(tp.partition, 0)  # DB is authoritative; nothing durable means start at 0
            if tp.partition in db:
                # bring the advisory Kafka offset up to the DB position at the next commit, even if
                # no new record ever arrives on this partition (otherwise lag would be over-reported)
                self._done[tp.partition] = db[tp.partition]
        self.stats.assignments.append(sorted((tp.partition, tp.offset) for tp in partitions))
        log.info("assigned %s", self.stats.assignments[-1])
        consumer.assign(partitions)

    def _on_revoke(self, consumer, partitions):
        self._commit_kafka([tp.partition for tp in partitions])
        for tp in partitions:
            self._done.pop(tp.partition, None)
        log.info("revoked %s", [tp.partition for tp in partitions])

    # -------------------------------------------------------------- helpers
    def _commit_kafka(self, parts=None) -> None:
        offsets = [TopicPartition(self.topic, p, o) for p, o in self._done.items() if parts is None or p in parts]
        if not offsets:
            return
        try:
            self.consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as exc:  # advisory: DB progress remains authoritative
            self.stats.kafka_commit_failures += 1
            log.warning("kafka commit failed: %s", exc)
            return
        self.faults.hit("after_kafka_commit")

    def _process_with_retry(self, rec: LogRecord) -> str:
        for attempt in range(1, self.max_attempts + 1):
            try:
                t0 = time.perf_counter()
                outcome = self.processor.process(rec)
                self.stats.txn.record(time.perf_counter() - t0)
                return outcome
            except TRANSIENT as exc:
                self.stats.retries += 1
                log.warning("transient error at %s[%d]@%d attempt %d: %s",
                            rec.topic, rec.partition, rec.offset, attempt, exc)
                time.sleep(min(2.0, 0.05 * 2 ** attempt))
                if self.conn.closed or self.conn.broken:
                    try:
                        self.conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=3)
                        self.processor.conn = self.conn
                    except psycopg.OperationalError as exc2:
                        log.warning("reconnect failed: %s", exc2)
        raise RuntimeError(f"giving up on {rec.topic}[{rec.partition}]@{rec.offset} after {self.max_attempts} attempts")

    def settled(self, until: dict[int, int]) -> bool:
        db = progress(self.conn, self.topic)
        return all(db.get(p, 0) >= end for p, end in until.items())

    # -------------------------------------------------------------- main loop
    def run(self, until: dict[int, int] | None = None, max_records: int | None = None,
            timeout_s: float = 120.0) -> RunStats:
        """Process until DB progress reaches `until`, `max_records` were handled, or timeout."""
        self.consumer.subscribe([self.topic], on_assign=self._on_assign, on_revoke=self._on_revoke)
        deadline = time.monotonic() + timeout_s
        handled = 0
        try:
            while True:
                if until is not None and self.settled(until):
                    self.stats.stopped_because = "settled"
                    self._commit_kafka()  # one more try: a commit racing a rebalance can fail
                    return self.stats
                if time.monotonic() > deadline:
                    self.stats.stopped_because = "timeout"
                    return self.stats
                msgs = self.consumer.consume(self.batch_size, timeout=1.0)
                for msg in msgs:
                    if msg.error():
                        log.warning("kafka error: %s", msg.error())
                        continue
                    rec = LogRecord(msg.topic(), msg.partition(), msg.offset(), msg.timestamp()[1],
                                    msg.key(), msg.value())
                    outcome = self._process_with_retry(rec)  # raises -> halt without committing this record
                    self.stats.outcomes[outcome] += 1
                    self._done[rec.partition] = rec.offset + 1
                    handled += 1
                    if max_records is not None and handled >= max_records:
                        self._commit_kafka()
                        self.stats.stopped_because = "max_records"
                        return self.stats
                self._commit_kafka()
        finally:
            self.close()

    def close(self) -> None:
        try:
            self.consumer.close()
        except RuntimeError:
            pass  # already closed
        if not self.conn.closed:
            self.conn.close()


def main() -> int:
    """Exit codes: 0 settled or max-records reached, 2 timeout. Crashes exit non-zero by themselves."""
    from streamledger.kafka_io import end_offsets

    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="commerce-events")
    ap.add_argument("--group", default="streamledger-projector")
    ap.add_argument("--instance", default="c1")
    ap.add_argument("--until-settled", action="store_true", help="stop once the end offsets seen at start are processed")
    ap.add_argument("--max-records", type=int)
    ap.add_argument("--faults", default=os.environ.get("STREAMLEDGER_FAULTS", ""), help="see streamledger/faults.py")
    ap.add_argument("--timeout", type=float, default=600)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    until = end_offsets(a.topic) if a.until_settled else None
    c = StreamConsumer(a.topic, a.group, dsn=os.environ.get("STREAMLEDGER_PG_DSN", DEFAULT_DSN),
                       faults=Faults.parse(a.faults), instance=a.instance)
    s = c.run(until=until, max_records=a.max_records, timeout_s=a.timeout)
    txn = sorted(s.txn_seconds)
    print(json.dumps({"stopped": s.stopped_because, "outcomes": dict(s.outcomes), "retries": s.retries,
                      "kafka_commit_failures": s.kafka_commit_failures, "assignments": s.assignments,
                      "txn_count": s.txn.count, "txn_p50_exact": s.txn.exact,
                      "txn_p50_ms": round(1000 * txn[len(txn) // 2], 3) if txn else None}), flush=True)
    return 2 if s.stopped_because == "timeout" else 0


if __name__ == "__main__":
    raise SystemExit(main())
