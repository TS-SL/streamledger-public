"""Shared fixtures and helpers for tests that need the compose stack."""
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
pytest.importorskip("confluent_kafka")

from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from streamledger.kafka_io import DEFAULT_BOOTSTRAP, create_topic, delete_topic, read_log  # noqa: E402
from streamledger.reference.model import evaluate  # noqa: E402
from streamledger.storage.snapshot import db_view, progress, reset  # noqa: E402
from streamledger.verify import diff, reference_view  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DSN = os.environ.get(
    "STREAMLEDGER_PG_DSN", "postgresql://streamledger:streamledger_local_only@localhost:55432/streamledger"
)
GROUP = "streamledger-projector"


@pytest.fixture
def db():
    try:
        conn = psycopg.connect(DSN, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    reset(conn)
    yield conn
    try:
        reset(conn)  # leave nothing behind for tests that do not use this fixture
    except psycopg.OperationalError:  # the test killed the server under this connection
        conn = psycopg.connect(DSN, autocommit=True, connect_timeout=10)
        reset(conn)
    conn.close()


@pytest.fixture
def topic():
    from confluent_kafka.admin import AdminClient

    try:
        AdminClient({"bootstrap.servers": DEFAULT_BOOTSTRAP}).list_topics(timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Kafka not reachable: {exc}")
    name = f"it-{uuid.uuid4().hex[:10]}"
    create_topic(name, partitions=3)
    yield name
    delete_topic(name)


def check_against_reference(conn, topic, until):
    expected = reference_view(evaluate(read_log(topic, until=until)))
    problems = diff(expected, db_view(conn, topic))
    assert problems == [], "\n".join(problems)
    return expected


def ledger_rows(conn, topic) -> dict[int, int]:
    return dict(conn.execute("SELECT partition, count(*) FROM position_ledger WHERE topic = %s GROUP BY 1",
                             (topic,)).fetchall())


def assert_ledger_matches_progress(conn, topic):
    """No partial transaction: every partition has exactly next_offset ledger rows, at offsets 0..next-1."""
    prog = progress(conn, topic)
    assert ledger_rows(conn, topic) == {p: n for p, n in prog.items() if n}
    gaps = conn.execute(
        'SELECT partition, count(DISTINCT "offset"), max("offset") FROM position_ledger WHERE topic = %s GROUP BY 1',
        (topic,)).fetchall()
    for p, distinct, mx in gaps:
        assert distinct == prog[p] and mx == prog[p] - 1, (p, distinct, mx, prog[p])
    return prog


def kafka_committed(topic, partitions, group=GROUP) -> dict[int, int]:
    c = Consumer({"bootstrap.servers": DEFAULT_BOOTSTRAP, "group.id": group})
    try:
        tps = c.committed([TopicPartition(topic, p) for p in partitions], timeout=10)
        return {tp.partition: max(tp.offset, 0) for tp in tps}
    finally:
        c.close()


def consumer_cmd(topic, *args):
    return [sys.executable, "-m", "streamledger.consumer", "--topic", topic, *args]


def proc_env(faults=""):
    return dict(os.environ, PYTHONPATH=str(ROOT / "src"), STREAMLEDGER_FAULTS=faults)


def run_consumer_process(topic, *args, faults="", timeout=240):
    """Run the consumer CLI in a child process; returns (exit code, stats or None, seconds, stderr tail)."""
    t0 = time.monotonic()
    p = subprocess.run(consumer_cmd(topic, *args), env=proc_env(faults), capture_output=True, text=True,
                       timeout=timeout, cwd=ROOT)
    elapsed = time.monotonic() - t0
    stats = None
    for line in reversed(p.stdout.strip().splitlines()):
        if line.startswith("{"):
            stats = json.loads(line)
            break
    return p.returncode, stats, elapsed, p.stderr[-2000:]


def wait_for(cond, timeout=60, interval=0.2, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for {what}")
