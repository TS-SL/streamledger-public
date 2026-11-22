"""compare() must refuse a snapshot taken for a different topic, before any reconstruction.

Two topics can have identical projections (both empty, or the same seeded workload
published twice). A comparison that only looks at projections would then report a
match for the wrong topic. These tests run without pyspark: the Spark module is
replaced by a stand-in that rebuilds with the Python reference and records calls.
"""
import json
import sys
import types

import pytest

from streamledger.analytics import reconcile
from streamledger.archive import load_manifest, read_records, write_chunk
from streamledger.domain.generator import GeneratorConfig, generate, simulate_log
from streamledger.reference.model import evaluate
from streamledger.verify import reference_view, to_jsonable


@pytest.fixture
def spark_calls(monkeypatch):
    """Stand-in for streamledger.analytics.spark_job that counts how often it is used."""
    calls = []
    fake = types.ModuleType("streamledger.analytics.spark_job")
    fake.DEFAULT_GRACE_MS = 3_600_000

    class _Session:
        def stop(self):
            pass

    def session():
        calls.append("session")
        return _Session()

    def load_archive(_spark, d, topic, cutoff):
        calls.append("load_archive")
        return read_records(d, topic, cutoff)

    def reconstruct(records, grace_ms=fake.DEFAULT_GRACE_MS):
        calls.append("reconstruct")
        return reference_view(evaluate(records, grace_ms))

    fake.session, fake.load_archive, fake.reconstruct = session, load_archive, reconstruct
    monkeypatch.setitem(sys.modules, "streamledger.analytics.spark_job", fake)
    return calls


def _archive(d, records, topic):
    d.mkdir(parents=True, exist_ok=True)
    m = load_manifest(d, topic)
    for p in sorted({r.partition for r in records}):
        write_chunk(d, m, sorted((r for r in records if r.partition == p), key=lambda r: r.offset))
    cut = {}
    for r in records:
        cut[r.partition] = max(cut.get(r.partition, 0), r.offset + 1)
    return cut


def _snapshot(path, topic, records, cutoff):
    """The JSON shape take_snapshot() writes, built from the reference instead of PostgreSQL."""
    view = reference_view(evaluate(records))
    path.write_text(json.dumps({"topic": topic, "cutoff": {str(p): o for p, o in cutoff.items()},
                                "taken_at": 0, "view": to_jsonable(view)}, sort_keys=True))
    return path


def _workload(topic):
    return simulate_log(generate(GeneratorConfig(seed=5, orders=40, duplicate_rate=0.1)), partitions=3, topic=topic)


def test_different_topics_with_identical_empty_state_are_refused(tmp_path, spark_calls):
    archive = tmp_path / "archive-b"
    archive.mkdir()
    snap = _snapshot(tmp_path / "a.snapshot.json", "topic-a", [], {})
    with pytest.raises(reconcile.SnapshotMismatch, match="'topic-a'.*'topic-b'"):
        reconcile.compare("topic-b", snap, archive)
    assert spark_calls == []


def test_different_topics_with_identical_nonempty_state_are_refused(tmp_path, spark_calls, monkeypatch):
    rec_a, rec_b = _workload("topic-a"), _workload("topic-b")
    cut_a = _archive(tmp_path / "archive-a", rec_a, "topic-a")
    cut_b = _archive(tmp_path / "archive-b", rec_b, "topic-b")
    assert cut_a == cut_b and reference_view(evaluate(rec_a)) == reference_view(evaluate(rec_b))
    snap = _snapshot(tmp_path / "a.snapshot.json", "topic-a", rec_a, cut_a)

    # the identity check must come before any rebuild, not only before Spark
    monkeypatch.setattr("streamledger.reference.model.evaluate",
                        lambda *a, **k: pytest.fail("reconstruction ran before the identity check"))
    with pytest.raises(reconcile.SnapshotMismatch, match="'topic-a'.*'topic-b'"):
        reconcile.compare("topic-b", snap, tmp_path / "archive-b")
    assert spark_calls == []


def test_same_topic_comparison_runs_and_matches(tmp_path, spark_calls):
    rec = _workload("topic-a")
    cut = _archive(tmp_path / "archive-a", rec, "topic-a")
    snap = _snapshot(tmp_path / "a.snapshot.json", "topic-a", rec, cut)
    report = reconcile.compare("topic-a", snap, tmp_path / "archive-a")
    assert report["match"], report["examples"]
    assert report["topic"] == "topic-a" and report["records"] == len(rec) > 0
    assert spark_calls == ["session", "load_archive", "reconstruct"]


def test_same_topic_with_damaged_snapshot_reports_classified_difference(tmp_path, spark_calls):
    """Controlled synthetic mismatch: a comparison that produced an empty report would pass wrongly."""
    rec = _workload("topic-a")
    cut = _archive(tmp_path / "archive-a", rec, "topic-a")
    snap = _snapshot(tmp_path / "a.snapshot.json", "topic-a", rec, cut)
    data = json.loads(snap.read_text())
    period = data["view"]["periods"][0]
    period[1][0] += 1  # one period's captured total off by one minor unit
    snap.write_text(json.dumps(data))
    report = reconcile.compare("topic-a", snap, tmp_path / "archive-a")
    assert not report["match"]
    assert report["classified"]["spark_vs_db"] == {"periods:mismatch": 1}
    assert report["classified"]["reference_vs_db"] == {"periods:mismatch": 1}
    assert report["counts"]["spark_vs_reference"] == 0


@pytest.mark.parametrize("bad", [{}, {"topic": None}, {"topic": ""}])
def test_snapshot_without_topic_is_refused(tmp_path, spark_calls, bad):
    snap = tmp_path / "s.json"
    snap.write_text(json.dumps({"cutoff": {}, "view": to_jsonable(reference_view(evaluate([]))), **bad}))
    with pytest.raises(reconcile.SnapshotMismatch, match="no topic"):
        reconcile.compare("topic-a", snap, tmp_path)
    assert spark_calls == []


def test_cli_reports_mismatch_clearly(tmp_path, spark_calls, capsys, monkeypatch):
    archive = tmp_path / "archive-b"
    archive.mkdir()
    snap = _snapshot(tmp_path / "a.snapshot.json", "topic-a", [], {})
    monkeypatch.setattr(sys, "argv", ["reconcile", "compare", "--topic", "topic-b", "--snapshot", str(snap),
                                      "--archive", str(archive)])
    assert reconcile.main() == 2
    err = capsys.readouterr().err
    assert "snapshot identity mismatch" in err and "topic-a" in err and "topic-b" in err
