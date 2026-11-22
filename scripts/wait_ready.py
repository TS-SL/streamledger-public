"""Block until PostgreSQL, Kafka, Java and PySpark are usable; exit 1 with the reason if not.

Used by CI before the stack tests, so that a test run cannot pass by skipping
everything because a service was not up yet.

    python scripts/wait_ready.py [--timeout 120] [--no-spark]
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time

DSN = os.environ.get("STREAMLEDGER_PG_DSN",
                     "postgresql://streamledger:streamledger_local_only@localhost:55432/streamledger")
BOOTSTRAP = os.environ.get("STREAMLEDGER_KAFKA", "localhost:19092")


def retry(what, fn, deadline):
    last = None
    while time.monotonic() < deadline:
        try:
            result = fn()
            print(f"ready  {what}: {result}", flush=True)
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(1)
    raise SystemExit(f"NOT READY {what}: {last}")


def postgres():
    import psycopg

    with psycopg.connect(DSN, connect_timeout=3) as conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
        tables = conn.execute("SELECT count(*) FROM information_schema.tables "
                              "WHERE table_name IN ('consumer_progress', 'received_events')").fetchone()[0]
    if tables != 2:
        raise RuntimeError("schema not migrated")
    return f"PostgreSQL {version}, schema present"


def kafka():
    from confluent_kafka.admin import AdminClient

    md = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=5)
    return f"{len(md.brokers)} broker(s) at {BOOTSTRAP}"


def spark():
    java = shutil.which("java")
    if not java:
        raise RuntimeError("java not on PATH")
    jv = subprocess.run([java, "-version"], capture_output=True, text=True).stderr.splitlines()[0]
    import pyspark

    return f"pyspark {pyspark.__version__}; {jv}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--timeout", type=float, default=120)
    ap.add_argument("--no-spark", action="store_true")
    a = ap.parse_args()
    deadline = time.monotonic() + a.timeout
    retry("postgres", postgres, deadline)
    retry("kafka", kafka, deadline)
    if not a.no_spark:
        retry("spark", spark, deadline)
    return 0


if __name__ == "__main__":
    sys.exit(main())
