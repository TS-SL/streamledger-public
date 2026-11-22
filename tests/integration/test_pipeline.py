"""Kafka -> transactional consumer -> PostgreSQL, checked against the reference."""
import pytest

from streamledger.consumer import StreamConsumer
from streamledger.domain.generator import GeneratorConfig, generate
from streamledger.kafka_io import end_offsets, publish, read_log
from streamledger.storage.processor import Processor
from streamledger.storage.snapshot import db_view
from stack_helpers import check_against_reference

pytestmark = pytest.mark.integration
CFG = GeneratorConfig(seed=21, orders=250, duplicate_rate=0.08, conflict_rate=0.02, invalid_rate=0.02)


def settle(topic, **kw):
    until = end_offsets(topic)
    stats = StreamConsumer(topic, **kw).run(until=until, timeout_s=180)
    assert stats.stopped_because == "settled", stats
    return until, stats


def financial(view):
    return {k: view[k] for k in ("orders", "payments", "refunds", "periods", "unresolved")}


def test_stream_matches_reference_exactly(db, topic):
    publish(generate(CFG), topic)
    until, stats = settle(topic)
    expected = check_against_reference(db, topic, until)
    # the workload must actually exercise every path, or this test proves little
    assert {"APPLIED", "DUPLICATE", "CONFLICT", "PENDING", "REJECTED", "INVALID"} <= {
        s for s, _ in expected["positions"].values()}
    assert stats.outcomes["SKIPPED"] == 0 and stats.retries == 0
    assert sum(stats.outcomes.values()) == sum(until.values())


def test_republished_history_adds_no_money(db, topic):
    emissions = generate(CFG)
    publish(emissions, topic)
    until1, _ = settle(topic)
    before = financial(db_view(db, topic))
    publish(emissions, topic)  # the whole history again, at new offsets
    until2, stats = settle(topic)
    assert sum(until2.values()) == 2 * sum(until1.values())
    after = check_against_reference(db, topic, until2)
    assert financial(after) == before
    assert set(stats.outcomes) <= {"DUPLICATE", "CONFLICT", "INVALID"}


def test_replay_from_zero_with_fence_removed_is_a_no_op(db, topic):
    """Second line of defence: even if progress rows are lost, event_id dedup holds."""
    publish(generate(CFG), topic)
    until, _ = settle(topic)
    before = db_view(db, topic)
    db.execute("DELETE FROM consumer_progress WHERE topic = %s", (topic,))
    _, stats = settle(topic, group="streamledger-replay")
    assert db_view(db, topic) == before
    assert stats.outcomes["APPLIED"] == 0 and stats.outcomes["REPLAYED"] > 0


def test_restart_resumes_from_db_progress_not_kafka_offsets(db, topic):
    publish(generate(CFG), topic)
    until = end_offsets(topic)
    first = StreamConsumer(topic).run(max_records=300, timeout_s=120)
    assert first.stopped_because == "max_records"
    # simulate "DB committed, Kafka commit lost": rewind the group's Kafka offsets to 0
    from confluent_kafka import Consumer, TopicPartition
    from streamledger.kafka_io import DEFAULT_BOOTSTRAP
    c = Consumer({"bootstrap.servers": DEFAULT_BOOTSTRAP, "group.id": "streamledger-projector"})
    c.commit(offsets=[TopicPartition(topic, p, 0) for p in until], asynchronous=False)
    c.close()
    second = StreamConsumer(topic).run(until=until, timeout_s=180)
    assert second.stopped_because == "settled"
    # seeking from the DB row means nothing already processed was even re-read
    assert second.outcomes["SKIPPED"] == 0 and second.outcomes["REPLAYED"] == 0
    assert sum(first.outcomes.values()) + sum(second.outcomes.values()) == sum(until.values())
    check_against_reference(db, topic, until)


def test_progress_fence_skips_already_processed_position(db, topic):
    publish(generate(GeneratorConfig(seed=3, orders=5, duplicate_rate=0, conflict_rate=0, invalid_rate=0)), topic)
    records = sorted(read_log(topic), key=lambda r: (r.partition, r.offset))
    proc = Processor(db)
    first = [proc.process(r) for r in records]
    assert "SKIPPED" not in first
    assert [proc.process(r) for r in records] == ["SKIPPED"] * len(records)
