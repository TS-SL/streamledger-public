"""Thin Kafka helpers: topic creation, publishing the synthetic workload, raw log reads."""
from __future__ import annotations

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from streamledger.domain.events import LogRecord
from streamledger.domain.generator import Emission

DEFAULT_BOOTSTRAP = "localhost:19092"


def create_topic(topic: str, partitions: int = 3, bootstrap: str = DEFAULT_BOOTSTRAP) -> None:
    """Record timestamps are producer CreateTime (the simulated send time); see consistency-contract 4."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    fut = admin.create_topics([NewTopic(topic, partitions, 1, config={
        "message.timestamp.type": "CreateTime", "retention.ms": "-1", "retention.bytes": "-1"})])[topic]
    try:
        fut.result(30)
    except KafkaException as exc:
        if exc.args[0].name() != "TOPIC_ALREADY_EXISTS":
            raise


def delete_topic(topic: str, bootstrap: str = DEFAULT_BOOTSTRAP) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})  # keep a reference until the future resolves
    admin.delete_topics([topic])[topic].result(30)


def publish(emissions: list[Emission], topic: str, bootstrap: str = DEFAULT_BOOTSTRAP) -> int:
    """Send in simulated emission order. Idempotent producer: broker-level retries do
    not add records; application-level duplicates in the workload are sent as-is."""
    errors: list[str] = []
    p = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all",
                  "linger.ms": 5, "partitioner": "murmur2_random"})
    cb = lambda err, _msg: errors.append(str(err)) if err else None
    for em in sorted(emissions, key=lambda e: (e.emit_ts_ms, e.seq)):
        while True:
            try:
                p.produce(topic, key=em.key, value=em.value, timestamp=em.emit_ts_ms, on_delivery=cb)
                break
            except BufferError:
                p.poll(0.1)
        p.poll(0)
    left = p.flush(60)
    if left or errors:
        raise RuntimeError(f"publish failed: {left} unflushed, errors={errors[:3]}")
    return len(emissions)


def end_offsets(topic: str, bootstrap: str = DEFAULT_BOOTSTRAP) -> dict[int, int]:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "streamledger-admin", "enable.auto.commit": False})
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    finally:
        c.close()


def read_log(topic: str, bootstrap: str = DEFAULT_BOOTSTRAP, until: dict[int, int] | None = None) -> list[LogRecord]:
    """Every record in [0, until[p]) for each partition, exactly as stored."""
    until = until or end_offsets(topic, bootstrap)
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "streamledger-reader", "enable.auto.commit": False,
                  "enable.partition.eof": True})
    c.assign([TopicPartition(topic, p, 0) for p, end in until.items() if end > 0])
    out: list[LogRecord] = []
    remaining = {p for p, end in until.items() if end > 0}
    try:
        while remaining:
            msg = c.poll(5)
            if msg is None:
                raise TimeoutError(f"read_log stalled; remaining partitions {remaining}")
            if msg.error():
                continue  # partition EOF events
            if msg.offset() < until[msg.partition()]:
                out.append(LogRecord(msg.topic(), msg.partition(), msg.offset(), msg.timestamp()[1],
                                     msg.key(), msg.value()))
            if msg.offset() + 1 >= until[msg.partition()]:
                remaining.discard(msg.partition())
    finally:
        c.close()
    return out
