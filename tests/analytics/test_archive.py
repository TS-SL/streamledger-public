"""Archive completeness protocol: contiguity, hashes, orphans, cutoffs. No services needed."""
import json

import pytest

pytest.importorskip("pyarrow")

from streamledger.archive import covered, load_manifest, read_records, remove_orphans, verify, write_chunk  # noqa: E402
from streamledger.domain.generator import GeneratorConfig, generate, simulate_log  # noqa: E402

TOPIC = "commerce-events"


def archived(tmp_path, chunk=50):
    records = simulate_log(generate(GeneratorConfig(seed=5, orders=60)), partitions=3, topic=TOPIC)
    m = load_manifest(tmp_path, TOPIC)
    for p in sorted({r.partition for r in records}):
        rs = sorted((r for r in records if r.partition == p), key=lambda r: r.offset)
        for i in range(0, len(rs), chunk):
            write_chunk(tmp_path, m, rs[i:i + chunk])
    return records


def ends(records):
    out = {}
    for r in records:
        out[r.partition] = max(out.get(r.partition, 0), r.offset + 1)
    return out


def test_round_trip_is_byte_exact_and_complete(tmp_path):
    records = archived(tmp_path)
    v = verify(tmp_path, TOPIC)
    assert v.problems == [] and v.complete_to == ends(records)
    back = read_records(tmp_path, TOPIC, ends(records))
    key = lambda r: (r.partition, r.offset)
    assert sorted(back, key=key) == sorted(records, key=key)


def test_cutoff_filters_and_refuses_beyond_coverage(tmp_path):
    records = archived(tmp_path)
    e = ends(records)
    half = {p: n // 2 for p, n in e.items()}
    assert len(read_records(tmp_path, TOPIC, half)) == sum(half.values())
    with pytest.raises(ValueError, match="not complete"):
        read_records(tmp_path, TOPIC, {p: n + 1 for p, n in e.items()})


def test_tampered_file_and_gap_are_detected(tmp_path):
    archived(tmp_path, chunk=20)
    m = load_manifest(tmp_path, TOPIC)
    starts = {(f["partition"], f["start"]) for f in m["files"]}
    victim = next(f for f in m["files"] if f["start"] > 0 and (f["partition"], f["end"]) in starts)  # a middle chunk
    (tmp_path / victim["file"]).write_bytes(b"corrupt")
    assert any("sha256 mismatch" in p for p in verify(tmp_path, TOPIC).problems)
    m["files"].remove(victim)
    (tmp_path / "manifest.json").write_text(json.dumps(m))
    probs = verify(tmp_path, TOPIC).problems
    assert any("gap" in p for p in probs), probs


def test_non_contiguous_chunk_is_refused(tmp_path):
    records = sorted((r for r in simulate_log(generate(GeneratorConfig(seed=5, orders=10)), 1, TOPIC)),
                     key=lambda r: r.offset)
    m = load_manifest(tmp_path, TOPIC)
    with pytest.raises(AssertionError):
        write_chunk(tmp_path, m, records[5:10])  # does not start at the covered end (0)


def test_orphan_from_crash_between_rename_and_manifest_is_removed(tmp_path):
    archived(tmp_path)
    (tmp_path / "p000-000000009999-000000010000.parquet").write_bytes(b"x")
    (tmp_path / "p001-000000000000-000000000001.parquet.tmp").write_bytes(b"x")
    m = load_manifest(tmp_path, TOPIC)
    assert sorted(remove_orphans(tmp_path, m)) == ["p000-000000009999-000000010000.parquet",
                                                  "p001-000000000000-000000000001.parquet.tmp"]
    assert verify(tmp_path, TOPIC).problems == [] and covered(m)
