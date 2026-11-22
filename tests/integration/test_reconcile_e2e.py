"""Kafka -> PostgreSQL projection, Kafka -> Parquet archive, then PySpark vs DB vs reference at one cutoff."""
import pytest

pytest.importorskip("pyspark")

from stack_helpers import DSN  # noqa: E402
from streamledger.analytics.reconcile import compare, take_snapshot  # noqa: E402
from streamledger.archive import archive_topic, verify  # noqa: E402
from streamledger.consumer import StreamConsumer  # noqa: E402
from streamledger.domain.generator import GeneratorConfig, generate  # noqa: E402
from streamledger.kafka_io import end_offsets, publish  # noqa: E402

pytestmark = [pytest.mark.integration, pytest.mark.spark]


def test_three_way_reconciliation_and_tamper_detection(db, topic, tmp_path):
    publish(generate(GeneratorConfig(seed=61, orders=300, duplicate_rate=0.08, conflict_rate=0.02,
                                     invalid_rate=0.02)), topic)
    until = end_offsets(topic)
    assert StreamConsumer(topic).run(until=until, timeout_s=180).stopped_because == "settled"
    archive = tmp_path / "archive"
    archive_topic(topic, archive, until=until)
    assert verify(archive, topic).complete_up_to(until)

    snap = tmp_path / "snap.json"
    take_snapshot(topic, snap, DSN)
    report = compare(topic, snap, archive)
    assert report["match"], report["examples"]
    assert report["records"] == sum(until.values())
    print(f"\nRECONCILE {report['records']} records: {report['seconds']}")

    # deliberate damage the schema allows: one redelivery's evidence lost, one period total off by a cent
    db.execute('DELETE FROM duplicate_deliveries WHERE ("partition", "offset") = '
               '(SELECT "partition", "offset" FROM duplicate_deliveries WHERE topic = %s LIMIT 1)', (topic,))
    db.execute("UPDATE financial_projection SET captured_minor = captured_minor + 1 "
               "WHERE (period_date, currency) = (SELECT period_date, currency FROM financial_projection LIMIT 1)")
    take_snapshot(topic, snap, DSN)
    bad = compare(topic, snap, archive)
    assert not bad["match"]
    assert bad["counts"]["spark_vs_reference"] == 0  # the two independent rebuilds still agree
    assert bad["classified"]["spark_vs_db"] == {"periods:mismatch": 1, "positions:missing": 1}
    assert bad["classified"]["reference_vs_db"] == bad["classified"]["spark_vs_db"]
