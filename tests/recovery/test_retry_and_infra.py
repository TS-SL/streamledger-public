"""Retry classification and infrastructure failures.

Transient errors are retried a bounded number of times; anything else halts the
partition without advancing it. Killing PostgreSQL or Kafka mid-stream must not
lose or duplicate effects. The stack_kill tests kill real containers and are
opt-in: STREAMLEDGER_ALLOW_STACK_KILL=1 and access to `docker compose`.
"""
import os
import subprocess
import time

import pytest

from stack_helpers import (ROOT, assert_ledger_matches_progress, check_against_reference, consumer_cmd,
                           proc_env, wait_for)
from streamledger.consumer import StreamConsumer
from streamledger.domain.generator import GeneratorConfig, generate
from streamledger.faults import Faults, Rule
from streamledger.kafka_io import end_offsets, publish
from streamledger.storage.snapshot import progress

pytestmark = [pytest.mark.integration, pytest.mark.recovery]
CFG = GeneratorConfig(seed=51, orders=200)


def test_single_transient_error_is_retried_and_converges(db, topic):
    publish(generate(CFG), topic)
    until = end_offsets(topic)
    s = StreamConsumer(topic, faults=Faults([Rule("after_lock", 10, "transient")])).run(until=until, timeout_s=120)
    assert s.stopped_because == "settled" and s.retries == 1
    check_against_reference(db, topic, until)


def test_persistent_transient_error_halts_after_bounded_attempts(db, topic):
    publish(generate(CFG), topic)
    until = end_offsets(topic)
    faults = Faults([Rule("after_lock", 10, "transient", repeat=True)])
    c = StreamConsumer(topic, faults=faults, max_attempts=5)
    t0 = time.monotonic()
    box = {}

    def target():
        try:
            c.run(until=until, timeout_s=120)
        except Exception as exc:  # noqa: BLE001
            box["exc"] = exc

    import threading
    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(60)  # an unbounded retry loop must fail this test, not hang the suite
    assert not t.is_alive(), f"consumer still retrying after 60s: retries={c.stats.retries}"
    elapsed = time.monotonic() - t0
    assert isinstance(box.get("exc"), RuntimeError) and "giving up" in str(box["exc"]), box
    assert c.stats.retries == 5 and faults.hits["after_lock"] == 9 + 5  # no unbounded loop
    assert sum(assert_ledger_matches_progress(db, topic).values()) == 9  # nothing skipped past the bad record
    assert elapsed < 30
    # once the cause is gone, a normal restart converges
    assert StreamConsumer(topic).run(until=until, timeout_s=120).stopped_because == "settled"
    check_against_reference(db, topic, until)


def test_invariant_violation_is_not_retried(db, topic):
    """A schema CHECK firing means a processor bug: halt immediately, do not retry, do not advance."""
    publish(generate(CFG), topic)
    until = end_offsets(topic)

    def corrupt():  # simulate a buggy write the schema must refuse at COMMIT
        c.processor.conn.execute("UPDATE orders SET captured_minor = captured_minor + 1 "
                                 "WHERE order_id = (SELECT order_id FROM orders LIMIT 1)")

    c = StreamConsumer(topic, faults=Faults([Rule("before_commit", 50, corrupt)]))
    import psycopg
    with pytest.raises(psycopg.errors.CheckViolation):
        c.run(until=until, timeout_s=60)
    assert c.stats.retries == 0
    assert sum(assert_ledger_matches_progress(db, topic).values()) == 49


# ------------------------------------------------------------------ real container kills

stack_kill = pytest.mark.skipif(os.environ.get("STREAMLEDGER_ALLOW_STACK_KILL") != "1",
                                reason="set STREAMLEDGER_ALLOW_STACK_KILL=1 to kill compose services")


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, capture_output=True, timeout=180)


@stack_kill
@pytest.mark.stack_kill
def test_postgres_killed_mid_stream(db, topic):
    publish(generate(GeneratorConfig(seed=52, orders=400)), topic)
    until = end_offsets(topic)
    total = sum(until.values())
    proc = subprocess.Popen(consumer_cmd(topic, "--until-settled"), env=proc_env(), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)
    wait_for(lambda: sum(progress(db, topic).values()) >= 0.3 * total, timeout=120, interval=0.02, what="30%")
    t_kill = time.monotonic()
    compose("kill", "-s", "SIGKILL", "postgres")
    code = proc.wait(60)  # consumer must give up within bounded retries, not hang
    halted_after = time.monotonic() - t_kill
    assert code != 0
    compose("start", "postgres")
    t_up = time.monotonic()
    import psycopg
    from stack_helpers import DSN
    wait_for(lambda: _can_connect(psycopg, DSN), timeout=60, what="postgres back")
    db_back = time.monotonic() - t_up
    conn = psycopg.connect(DSN, autocommit=True)
    at_kill = sum(assert_ledger_matches_progress(conn, topic).values())
    t0 = time.monotonic()
    p = subprocess.run(consumer_cmd(topic, "--until-settled"), env=proc_env(), capture_output=True, text=True,
                       timeout=240)
    assert p.returncode == 0, p.stderr[-2000:]
    print(f"\nRECOVERY postgres SIGKILL at {at_kill}/{total}: consumer halted after {halted_after:.2f}s, "
          f"postgres accepting connections after {db_back:.2f}s, restart-to-settled {time.monotonic() - t0:.2f}s")
    check_against_reference(conn, topic, until)
    conn.close()


def _can_connect(psycopg, dsn):
    try:
        psycopg.connect(dsn, connect_timeout=2).close()
        return True
    except psycopg.OperationalError:
        return False


@stack_kill
@pytest.mark.stack_kill
def test_kafka_killed_mid_stream_consumer_survives(db, topic):
    publish(generate(GeneratorConfig(seed=53, orders=400)), topic)
    until = end_offsets(topic)
    total = sum(until.values())
    import threading
    c = StreamConsumer(topic)
    box = {}
    t = threading.Thread(target=lambda: box.update(stats=c.run(until=until, timeout_s=300)), daemon=True)
    t.start()
    wait_for(lambda: sum(progress(db, topic).values()) >= 0.3 * total, timeout=120, interval=0.02, what="30%")
    compose("kill", "-s", "SIGKILL", "kafka")
    stalled_at = sum(progress(db, topic).values())
    time.sleep(5)
    compose("start", "kafka")
    t_up = time.monotonic()
    t.join(300)
    s = box["stats"]
    assert s.stopped_because == "settled", s
    print(f"\nRECOVERY kafka SIGKILL at ~{stalled_at}/{total}: same consumer process settled "
          f"{time.monotonic() - t_up:.2f}s after broker restart, retries={s.retries}")
    check_against_reference(db, topic, until)
