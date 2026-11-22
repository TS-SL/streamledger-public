"""PySpark reconstruction from a Parquet archive must equal the Python reference exactly."""
import pytest

pytest.importorskip("pyspark")
pytest.importorskip("pandas")

from streamledger.analytics.spark_job import load_archive, reconstruct, session  # noqa: E402
from streamledger.archive import load_manifest, write_chunk  # noqa: E402
from streamledger.domain.generator import GeneratorConfig, generate, simulate_log  # noqa: E402
from streamledger.reference.model import evaluate  # noqa: E402
from streamledger.verify import diff, reference_view  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    s = session()
    yield s
    s.stop()


def archive(d, records, topic):
    m = load_manifest(d, topic)
    for p in sorted({r.partition for r in records}):
        rs = sorted((r for r in records if r.partition == p), key=lambda r: r.offset)
        for i in range(0, len(rs), 400):
            write_chunk(d, m, rs[i:i + 400])
    cut = {}
    for r in records:
        cut[r.partition] = max(cut.get(r.partition, 0), r.offset + 1)
    return cut


@pytest.mark.parametrize("seed", [7, 21, 99])
def test_spark_matches_reference_on_generated_archive(spark, tmp_path, seed):
    cfg = GeneratorConfig(seed=seed, orders=400, duplicate_rate=0.1, conflict_rate=0.03, invalid_rate=0.03)
    records = simulate_log(generate(cfg), partitions=3, topic="arch")
    cut = archive(tmp_path, records, "arch")
    problems = diff(reference_view(evaluate(records)), reconstruct(load_archive(spark, tmp_path, "arch", cut)))
    assert problems == [], "\n".join(problems)


def test_spark_matches_reference_on_rule_scenarios(spark, tmp_path):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "integration"))
    from test_rule_parity import as_log, scenarios

    for i, (name, items) in enumerate(scenarios()):
        records = [r.__class__("parity", r.partition, r.offset, r.timestamp_ms, r.key, r.value) for r in as_log(items)]
        d = tmp_path / f"s{i}"
        d.mkdir()
        cut = archive(d, records, "parity")
        problems = diff(reference_view(evaluate(records)), reconstruct(load_archive(spark, d, "parity", cut)))
        assert problems == [], f"{name}:\n" + "\n".join(problems)


def test_spark_refuses_incomplete_archive(spark, tmp_path):
    records = simulate_log(generate(GeneratorConfig(seed=1, orders=20)), partitions=2, topic="arch")
    cut = archive(tmp_path, records, "arch")
    with pytest.raises(ValueError, match="not complete"):
        load_archive(spark, tmp_path, "arch", {p: n + 5 for p, n in cut.items()})
