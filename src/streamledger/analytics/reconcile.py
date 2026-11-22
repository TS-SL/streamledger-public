"""Three-way reconciliation at a settled cutoff: PostgreSQL snapshot vs PySpark vs Python reference.

Two steps, so Spark never has to run next to the broker and database:

    # with the stack up and the projector settled
    python -m streamledger.analytics.reconcile snapshot --topic T --out results/raw/T.snapshot.json
    python -m streamledger.archive write --topic T --dir results/raw/archive/T
    # stack may now be stopped
    python -m streamledger.analytics.reconcile compare --topic T --snapshot ... --archive ... --out report.json

The cutoff is the projector's committed progress at snapshot time. The snapshot
must have been taken for the requested topic, and the archive must verify as
complete up to that cutoff, or the comparison refuses to run (exit 2 for a
snapshot of another topic). Any difference is a failure (exit 1), reported by
section and kind.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

from streamledger.verify import diff, from_jsonable, reference_view, to_jsonable


def take_snapshot(topic: str, out: Path, dsn: str) -> dict:
    import psycopg

    from streamledger.storage.snapshot import db_view, progress

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.transaction():  # one consistent read of progress + projection
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cutoff = progress(conn, topic)
            view = db_view(conn, topic)
    snap = {"topic": topic, "cutoff": {str(p): o for p, o in cutoff.items()}, "taken_at": time.time(),
            "view": to_jsonable(view)}
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(snap, sort_keys=True))
    return snap


def classify(problems: list[str]) -> dict[str, int]:
    return dict(Counter(":".join(p.split(":")[:2]) for p in problems))


class SnapshotMismatch(ValueError):
    """The snapshot does not describe the topic being compared."""


def load_snapshot(snapshot: Path, topic: str) -> tuple[dict[int, int], dict]:
    """Read a snapshot and check it belongs to `topic`; returns (cutoff, db view).

    Two topics can have identical projections (both empty, or the same workload
    published twice), so equal state alone does not prove the right snapshot was used.
    """
    snap = json.loads(snapshot.read_text())
    taken_for = snap.get("topic")
    if not isinstance(taken_for, str) or not taken_for:
        raise SnapshotMismatch(f"snapshot {snapshot} records no topic; refusing to compare it with {topic!r}")
    if taken_for != topic:
        raise SnapshotMismatch(f"snapshot {snapshot} was taken for topic {taken_for!r}, not {topic!r}")
    return {int(p): o for p, o in snap["cutoff"].items()}, from_jsonable(snap["view"])


def compare(topic: str, snapshot: Path, archive: Path, grace_ms: int | None = None) -> dict:
    # identity first: nothing below may run for a snapshot of another topic
    cutoff, db = load_snapshot(snapshot, topic)

    from streamledger.archive import read_records
    from streamledger.analytics.spark_job import DEFAULT_GRACE_MS, load_archive, reconstruct, session
    from streamledger.reference import model

    grace_ms = grace_ms or DEFAULT_GRACE_MS
    evaluate = model.evaluate

    t0 = time.monotonic()
    ref = reference_view(evaluate(read_records(archive, topic, cutoff), grace_ms))
    t_ref = time.monotonic() - t0

    t0 = time.monotonic()
    spark = session()
    t_start = time.monotonic() - t0
    t0 = time.monotonic()
    sv = reconstruct(load_archive(spark, archive, topic, cutoff), grace_ms)
    t_spark = time.monotonic() - t0
    spark.stop()

    pairs = {"spark_vs_db": diff(sv, db, limit=10**6), "spark_vs_reference": diff(sv, ref, limit=10**6),
             "reference_vs_db": diff(ref, db, limit=10**6)}
    return {
        "topic": topic, "cutoff": cutoff, "records": sum(cutoff.values()),
        "counts": {k: len(v) for k, v in pairs.items()},
        "classified": {k: classify(v) for k, v in pairs.items()},
        "examples": {k: v[:10] for k, v in pairs.items()},
        "seconds": {"reference": round(t_ref, 3), "spark_session_start": round(t_start, 3),
                    "spark_reconstruct": round(t_spark, 3)},
        "summary": {"orders": len(db["orders"]), "positions": len(db["positions"]),
                    "unresolved": len(db["unresolved"]), "conflicts": len(db["conflicts"])},
        "match": not any(pairs.values()),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("snapshot"); s.add_argument("--topic", required=True); s.add_argument("--out", required=True)
    c = sub.add_parser("compare")
    for a in ("--topic", "--snapshot", "--archive"):
        c.add_argument(a, required=True)
    c.add_argument("--out")
    a = ap.parse_args()
    if a.cmd == "snapshot":
        from streamledger.consumer import DEFAULT_DSN

        snap = take_snapshot(a.topic, Path(a.out), os.environ.get("STREAMLEDGER_PG_DSN", DEFAULT_DSN))
        print(json.dumps({"cutoff": snap["cutoff"]}))
        return 0
    try:
        report = compare(a.topic, Path(a.snapshot), Path(a.archive))
    except SnapshotMismatch as exc:
        print(f"snapshot identity mismatch: {exc}", file=sys.stderr)
        return 2
    text = json.dumps(report, indent=1, sort_keys=True)
    if a.out:
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.out).write_text(text)
    print(text)
    return 0 if report["match"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
