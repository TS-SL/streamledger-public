"""Partition ownership changes: consumer groups rebalancing, and a stale owner still writing.

The consumer-group protocol decides who *should* own a partition; correctness must
not depend on it. The progress row lock plus the `offset < next_offset` fence make
a second writer harmless whether it is a legitimate new owner or a zombie.
"""
import subprocess
import threading
from collections import defaultdict

import psycopg
import pytest

from stack_helpers import DSN, check_against_reference, consumer_cmd, proc_env, wait_for
from streamledger.consumer import StreamConsumer
from streamledger.domain.generator import GeneratorConfig, generate, simulate_log
from streamledger.faults import FaultInjected, Faults, Rule
from streamledger.kafka_io import end_offsets, publish
from streamledger.reference.model import evaluate
from streamledger.storage.processor import Processor
from streamledger.storage.snapshot import db_view, progress
from streamledger.verify import diff, reference_view

pytestmark = [pytest.mark.integration, pytest.mark.recovery]
CFG = GeneratorConfig(seed=41, orders=250, duplicate_rate=0.08, conflict_rate=0.02, invalid_rate=0.02)


def run_in_thread(consumer, **kw):
    box = {}
    t = threading.Thread(target=lambda: box.update(stats=consumer.run(**kw)), daemon=True)
    t.start()
    return t, box


def test_two_members_share_partitions_and_converge(db, topic):
    publish(generate(CFG), topic)
    until = end_offsets(topic)
    a, b = StreamConsumer(topic, instance="a"), StreamConsumer(topic, instance="b")
    ta, ra = run_in_thread(a, until=until, timeout_s=180)
    tb, rb = run_in_thread(b, until=until, timeout_s=180)
    ta.join(200); tb.join(200)
    sa, sb = ra["stats"], rb["stats"]
    assert sa.stopped_because == sb.stopped_because == "settled"
    owned = [set(p for p, _ in asg) for s in (sa, sb) for asg in s.assignments[-1:]]
    assert owned[0] and owned[1], f"both members should have owned partitions: {sa.assignments} {sb.assignments}"
    done = sum(sa.outcomes.values()) + sum(sb.outcomes.values())
    skipped = sa.outcomes["SKIPPED"] + sb.outcomes["SKIPPED"]
    assert done - skipped == sum(until.values())  # every position applied exactly once
    check_against_reference(db, topic, until)


def test_member_leaves_mid_stream_and_partitions_move(db, topic):
    publish(generate(CFG), topic)
    until = end_offsets(topic)
    import time
    throttle = Rule("before_begin", 1, lambda: time.sleep(0.02), repeat=True)  # keep work in flight
    stayer = StreamConsumer(topic, instance="stayer", faults=Faults([throttle]))
    ts, rs = run_in_thread(stayer, until=until, timeout_s=240)
    wait_for(lambda: stayer.stats.assignments, timeout=30, what="stayer assignment")
    leaver = StreamConsumer(topic, instance="leaver")
    tl, rl = run_in_thread(leaver, max_records=60, timeout_s=120)  # joins, takes partitions, then leaves
    tl.join(150); ts.join(260)
    s, l = rs["stats"], rl["stats"]
    assert l.stopped_because == "max_records" and sum(l.outcomes.values()) == 60
    assert s.stopped_because == "settled"
    owned = [{p for p, _ in a} for a in s.assignments]
    assert owned[0] == set(until) and owned[-1] == set(until), s.assignments
    assert any(o < set(until) for o in owned), f"stayer never shared partitions: {s.assignments}"
    assert sum(s.outcomes.values()) - s.outcomes["SKIPPED"] + 60 == sum(until.values())
    check_against_reference(db, topic, until)


def test_member_dies_and_survivor_takes_over_after_session_timeout(db, topic):
    publish(generate(CFG), topic)
    until = end_offsets(topic)
    victim = subprocess.Popen(consumer_cmd(topic, "--until-settled", "--instance", "victim"),
                              env=proc_env("before_commit@80:exit"), stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL)
    wait_for(lambda: sum(progress(db, topic).values()) > 0, timeout=60, what="victim to start")
    survivor = StreamConsumer(topic, instance="survivor")
    t, r = run_in_thread(survivor, until=until, timeout_s=240)
    assert victim.wait(120) == 137
    t.join(260)
    s = r["stats"]
    assert s.stopped_because == "settled", s
    assert {p for p, _ in s.assignments[-1]} == set(until), s.assignments
    check_against_reference(db, topic, until)


# ------------------------------------------------------------------ zombies (no group protocol at all)

def zombie_log():
    return sorted(simulate_log(generate(GeneratorConfig(seed=43, orders=60)), partitions=1, topic="zombie"),
                  key=lambda r: r.offset)


def test_two_writers_on_one_partition_apply_each_position_once(db):
    records = zombie_log()
    results = defaultdict(list)

    def writer(name):
        with psycopg.connect(DSN, autocommit=True) as conn:
            proc = Processor(conn, owner=name)
            for rec in records:
                results[rec.offset].append(proc.process(rec))

    threads = [threading.Thread(target=writer, args=(n,)) for n in ("old-owner", "new-owner")]
    for t in threads: t.start()
    for t in threads: t.join(120)
    for off, outs in results.items():
        assert len(outs) == 2 and outs.count("SKIPPED") == 1, (off, outs)
    assert diff(reference_view(evaluate(records)), db_view(db, "zombie")) == []


@pytest.mark.parametrize("zombie_commits", [True, False], ids=["zombie-commits", "zombie-rolls-back"])
def test_new_owner_blocks_behind_zombie_transaction(db, zombie_commits):
    records = zombie_log()
    k = 10
    entered, release = threading.Event(), threading.Event()

    def pause():
        entered.set()
        assert release.wait(30)
        if not zombie_commits:
            raise FaultInjected("zombie dies before COMMIT")

    zconn = psycopg.connect(DSN, autocommit=True, application_name="streamledger-zombie")
    nconn = psycopg.connect(DSN, autocommit=True, application_name="streamledger-newowner")
    zombie = Processor(zconn, owner="zombie", faults=Faults([Rule("before_commit", k + 1, pause)]))
    owner = Processor(nconn, owner="new")
    for rec in records[:k]:
        zombie.process(rec)
    out = {}

    def zombie_k():
        try:
            out["zombie"] = zombie.process(records[k])
        except FaultInjected:
            out["zombie"] = "ROLLED_BACK"

    tz = threading.Thread(target=zombie_k)
    tz.start()
    assert entered.wait(10)
    tn = threading.Thread(target=lambda: out.update(new=owner.process(records[k])))
    tn.start()
    wait_for(lambda: db.execute("SELECT wait_event_type FROM pg_stat_activity "
                                "WHERE application_name = 'streamledger-newowner'").fetchone()[0] == "Lock",
             timeout=10, what="new owner to block on the progress row lock")
    release.set()
    tz.join(30); tn.join(30)
    if zombie_commits:
        assert out == {"zombie": out["zombie"], "new": "SKIPPED"} and out["zombie"] != "SKIPPED"
    else:
        assert out["zombie"] == "ROLLED_BACK" and out["new"] not in ("SKIPPED", "REPLAYED")
    for rec in records[k + 1:]:
        owner.process(rec)
    assert diff(reference_view(evaluate(records)), db_view(db, "zombie")) == []
    zconn.close(); nconn.close()
