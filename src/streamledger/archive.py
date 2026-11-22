"""Raw event archive: contiguous offset ranges per partition in Parquet, plus a manifest.

Completeness protocol (docs/consistency-contract.md section 8):
  * The archiver reads the topic on its own (assigned partitions, no shared group
    with the projector), and stores records byte-for-byte (key, value, partition, offset, timestamp).
  * Each file covers [start, end) of one partition. It is written to a temporary
    name, fsynced, then renamed. Only after that is the manifest rewritten (again
    temp + fsync + rename) to list it. A crash between the two leaves an orphan
    file that is not in the manifest; it is ignored and removed on the next run.
  * The archive is *complete up to E* when, for every partition p, the manifest's
    ranges cover [0, E_p) with no gap or overlap and every listed file's sha256
    and row count match. `verify()` checks exactly that; nothing assumes it.
  * The archive is not atomic with PostgreSQL. Comparisons use a common cutoff E.

    python -m streamledger.archive write  --topic T --dir results/archive/T
    python -m streamledger.archive verify --topic T --dir results/archive/T
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from streamledger.domain.events import LogRecord

SCHEMA = pa.schema([
    ("topic", pa.string()),
    ("partition", pa.int32()),
    ("offset", pa.int64()),
    ("timestamp_ms", pa.int64()),
    ("key", pa.binary()),
    ("value", pa.binary()),
])
MANIFEST = "manifest.json"
MAX_ROWS_PER_FILE = 5000


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fsync_dir(d: Path) -> None:
    """Persist a rename. Some filesystems (Windows drives mounted in WSL, Windows itself)
    do not support fsync on a directory; there the rename's durability is the OS's."""
    try:
        fd = os.open(d, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def load_manifest(d: Path, topic: str) -> dict:
    p = d / MANIFEST
    if not p.exists():
        return {"topic": topic, "format": 1, "files": []}
    m = json.loads(p.read_text())
    if m["topic"] != topic:
        raise ValueError(f"manifest is for topic {m['topic']!r}, not {topic!r}")
    return m


def covered(manifest: dict) -> dict[int, int]:
    """Contiguous end offset from 0 per partition (assumes verified ranges)."""
    ends: dict[int, int] = {}
    for f in sorted(manifest["files"], key=lambda f: (f["partition"], f["start"])):
        if f["start"] == ends.get(f["partition"], 0):
            ends[f["partition"]] = f["end"]
    return ends


def write_chunk(d: Path, manifest: dict, records: list[LogRecord]) -> None:
    """Persist one contiguous run of records of a single partition and list it in the manifest."""
    p = records[0].partition
    start, end = records[0].offset, records[-1].offset + 1
    assert [r.offset for r in records] == list(range(start, end)), "chunk must be contiguous"
    assert start == covered(manifest).get(p, 0), "chunk must extend the covered range"
    name = f"p{p:03d}-{start:012d}-{end:012d}.parquet"
    table = pa.Table.from_pylist([
        {"topic": r.topic, "partition": r.partition, "offset": r.offset, "timestamp_ms": r.timestamp_ms,
         "key": r.key, "value": r.value} for r in records], schema=SCHEMA)
    tmp = d / (name + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    with open(tmp, "rb+") as f:
        os.fsync(f.fileno())
    os.replace(tmp, d / name)
    _fsync_dir(d)
    manifest["files"].append({"file": name, "partition": p, "start": start, "end": end,
                              "rows": len(records), "sha256": _sha256(d / name)})
    _atomic_write_bytes(d / MANIFEST, json.dumps(manifest, indent=1, sort_keys=True).encode())


def remove_orphans(d: Path, manifest: dict) -> list[str]:
    listed = {f["file"] for f in manifest["files"]}
    removed = []
    for f in d.glob("p*.parquet*"):
        if f.name not in listed:
            f.unlink()
            removed.append(f.name)
    return removed


def archive_topic(topic: str, d: Path, until: dict[int, int] | None = None, bootstrap: str | None = None,
                  max_rows: int = MAX_ROWS_PER_FILE) -> dict:
    """Extend the archive from what the manifest covers up to `until` (default: current end offsets)."""
    from streamledger.kafka_io import DEFAULT_BOOTSTRAP, end_offsets, read_log

    bootstrap = bootstrap or DEFAULT_BOOTSTRAP
    d.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(d, topic)
    remove_orphans(d, manifest)
    until = until or end_offsets(topic, bootstrap)
    have = covered(manifest)
    # read_log reads from 0; fine for bounded experiments, and keeps the reader trivially correct
    records = [r for r in read_log(topic, bootstrap, until) if r.offset >= have.get(r.partition, 0)]
    by_part: dict[int, list[LogRecord]] = {}
    for r in sorted(records, key=lambda r: (r.partition, r.offset)):
        by_part.setdefault(r.partition, []).append(r)
    for p, rs in by_part.items():
        for i in range(0, len(rs), max_rows):
            write_chunk(d, manifest, rs[i:i + max_rows])
    return manifest


@dataclass
class VerifyResult:
    complete_to: dict[int, int]
    problems: list[str]

    def complete_up_to(self, cutoff: dict[int, int]) -> bool:
        return not self.problems and all(self.complete_to.get(p, 0) >= e for p, e in cutoff.items())


def verify(d: Path, topic: str) -> VerifyResult:
    manifest = load_manifest(d, topic)
    problems: list[str] = []
    ends: dict[int, int] = {}
    for f in sorted(manifest["files"], key=lambda f: (f["partition"], f["start"])):
        p, path = f["partition"], d / f["file"]
        expect = ends.get(p, 0)
        if f["start"] != expect:
            problems.append(f"{f['file']}: {'gap' if f['start'] > expect else 'overlap'} at partition {p} "
                            f"(expected start {expect}, got {f['start']})")
        if not path.exists():
            problems.append(f"{f['file']}: missing")
            continue
        if _sha256(path) != f["sha256"]:
            problems.append(f"{f['file']}: sha256 mismatch")
            continue
        t = pq.read_table(path)
        offsets = t.column("offset").to_pylist()
        if t.num_rows != f["rows"] or offsets != list(range(f["start"], f["end"])):
            problems.append(f"{f['file']}: rows/offsets do not match manifest")
        if set(t.column("partition").to_pylist()) != {p} or set(t.column("topic").to_pylist()) != {topic}:
            problems.append(f"{f['file']}: foreign rows")
        ends[p] = max(ends.get(p, 0), f["end"])
    return VerifyResult(ends, problems)


def read_records(d: Path, topic: str, cutoff: dict[int, int]) -> list[LogRecord]:
    """All archived records below the cutoff, after verifying completeness up to it."""
    v = verify(d, topic)
    if not v.complete_up_to(cutoff):
        raise ValueError(f"archive not complete up to {cutoff}: covered={v.complete_to} problems={v.problems}")
    out = []
    for f in load_manifest(d, topic)["files"]:
        for row in pq.read_table(d / f["file"]).to_pylist():
            if row["offset"] < cutoff.get(row["partition"], 0):
                out.append(LogRecord(row["topic"], row["partition"], row["offset"], row["timestamp_ms"],
                                     row["key"], row["value"]))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["write", "verify"])
    ap.add_argument("--topic", default="commerce-events")
    ap.add_argument("--dir", required=True)
    a = ap.parse_args()
    d = Path(a.dir)
    if a.cmd == "write":
        m = archive_topic(a.topic, d)
        print(json.dumps({"files": len(m["files"]), "covered": covered(m)}))
        return 0
    v = verify(d, a.topic)
    print(json.dumps({"complete_to": v.complete_to, "problems": v.problems}))
    return 1 if v.problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
