"""Crash at each transaction boundary, then restart: state must converge to the reference.

Rows of the crash-window table in docs/consistency-contract.md section 6:
    before BEGIN, during the transaction, after COMMIT before the Kafka commit,
    after the Kafka commit. Each is tested as an in-process crash (exception;
    connection survives) and as a hard process death (os._exit, no cleanup).
"""
import os
import signal
import subprocess

import pytest

from stack_helpers import (GROUP, assert_ledger_matches_progress, check_against_reference, consumer_cmd,
                           kafka_committed, proc_env, run_consumer_process, wait_for)
from streamledger.consumer import StreamConsumer
from streamledger.domain.generator import GeneratorConfig, generate
from streamledger.faults import FaultInjected, Faults, Rule
from streamledger.kafka_io import end_offsets, publish
from streamledger.storage.snapshot import progress

pytestmark = [pytest.mark.integration, pytest.mark.recovery]
CFG = GeneratorConfig(seed=31, orders=200, duplicate_rate=0.08, conflict_rate=0.02, invalid_rate=0.02)
N = 150  # crash on the 150th record

# point -> number of records durably processed when the crash hits record N
COMMITTED_AT_CRASH = {"before_begin": N - 1, "after_lock": N - 1, "before_commit": N - 1, "after_commit": N}


@pytest.fixture
def loaded(topic):
    publish(generate(CFG), topic)
    return topic, end_offsets(topic)


def restart_and_check(db, topic, until):
    stats = StreamConsumer(topic).run(until=until, timeout_s=180)
    assert stats.stopped_because == "settled"
    # the restart seeks from the DB progress row: nothing durable is re-read
    assert stats.outcomes["SKIPPED"] == 0 and stats.outcomes["REPLAYED"] == 0
    check_against_reference(db, topic, until)
    assert_ledger_matches_progress(db, topic)
    return stats


@pytest.mark.parametrize("point", list(COMMITTED_AT_CRASH))
def test_in_process_crash_at_boundary(db, loaded, point):
    topic, until = loaded
    c = StreamConsumer(topic, faults=Faults([Rule(point, N)]))
    with pytest.raises(FaultInjected):
        c.run(until=until, timeout_s=120)
    prog = assert_ledger_matches_progress(db, topic)  # no half-written transaction
    assert sum(prog.values()) == COMMITTED_AT_CRASH[point]
    committed = kafka_committed(topic, until)
    assert all(committed[p] <= prog.get(p, 0) for p in until), (committed, prog)  # Kafka never ahead of DB
    restart_and_check(db, topic, until)


def test_crash_right_after_kafka_commit(db, loaded):
    topic, until = loaded
    c = StreamConsumer(topic, faults=Faults([Rule("after_kafka_commit", 1)]), batch_size=50)
    with pytest.raises(FaultInjected):
        c.run(until=until, timeout_s=120)
    prog = assert_ledger_matches_progress(db, topic)
    committed = kafka_committed(topic, until)
    assert {p: committed[p] for p in prog} == prog  # the only window with Kafka and DB equal
    restart_and_check(db, topic, until)


@pytest.mark.parametrize("point", ["after_lock", "before_commit", "after_commit"])
def test_hard_process_death_at_boundary(db, loaded, point):
    topic, until = loaded
    code, _, _, err = run_consumer_process(topic, "--until-settled", faults=f"{point}@{N}:exit")
    assert code == 137, err
    prog = assert_ledger_matches_progress(db, topic)
    assert sum(prog.values()) == COMMITTED_AT_CRASH[point]
    # no connection is left holding locks or an open transaction
    wait_for(lambda: db.execute("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'streamledger-%' "
                                "AND state LIKE 'idle in transaction%'").fetchone()[0] == 0,
             timeout=15, what="server to clean up the dead session")
    code, stats, seconds, err = run_consumer_process(topic, "--until-settled")
    assert code == 0, err
    assert stats["stopped"] == "settled" and stats["outcomes"].get("SKIPPED", 0) == 0
    print(f"\nRECOVERY hard-kill@{point}: restart-to-settled {seconds:.2f}s, outcomes={stats['outcomes']}")
    check_against_reference(db, topic, until)


def test_external_sigkill_mid_stream(db, loaded):
    """Not tied to a hook: kill -9 whenever roughly 40% is durable; assertions don't depend on where it lands."""
    topic, until = loaded
    total = sum(until.values())
    proc = subprocess.Popen(consumer_cmd(topic, "--until-settled"), env=proc_env(), stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    try:
        wait_for(lambda: sum(progress(db, topic).values()) >= 0.4 * total, timeout=120, interval=0.02,
                 what="40% progress")
        os.kill(proc.pid, signal.SIGKILL)
        assert proc.wait(10) == -signal.SIGKILL
    finally:
        if proc.poll() is None:
            proc.kill()
    at_kill = sum(assert_ledger_matches_progress(db, topic).values())
    assert 0 < at_kill < total
    code, stats, seconds, err = run_consumer_process(topic, "--until-settled")
    assert code == 0, err
    print(f"\nRECOVERY external SIGKILL at {at_kill}/{total}: restart-to-settled {seconds:.2f}s")
    check_against_reference(db, topic, until)
    # regression: a partition finished by the killed process but never re-read by the restarted one
    # used to keep its stale Kafka offset forever (observed {0: 200} vs DB {0: 217})
    assert kafka_committed(topic, until, GROUP) == until, stats


def test_partition_finished_by_previous_owner_gets_kafka_offset_caught_up(db, loaded):
    """Deterministic version of the lag bug: the DB already holds a whole partition (the previous
    owner died after its DB commits but before its Kafka commit), so the new owner reads nothing there."""
    from streamledger.kafka_io import read_log
    from streamledger.storage.processor import Processor
    topic, until = loaded
    p0 = sorted((r for r in read_log(topic, until=until) if r.partition == 0), key=lambda r: r.offset)
    proc = Processor(db, owner="previous-owner")
    for rec in p0:
        proc.process(rec)
    assert kafka_committed(topic, until)[0] == 0  # Kafka never heard about partition 0
    stats = StreamConsumer(topic).run(until=until, timeout_s=180)
    assert stats.stopped_because == "settled" and stats.outcomes["SKIPPED"] == 0
    assert kafka_committed(topic, until) == until
    check_against_reference(db, topic, until)
