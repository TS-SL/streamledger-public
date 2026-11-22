"""Deferred events survive restarts and are released, in admission order, once parents arrive later."""
import pytest

from stack_helpers import assert_ledger_matches_progress, check_against_reference
from streamledger.consumer import StreamConsumer
from streamledger.domain.events import canonical_json, format_event_time
from streamledger.domain.generator import Emission
from streamledger.faults import FaultInjected, Faults, Rule
from streamledger.kafka_io import end_offsets, publish

pytestmark = [pytest.mark.integration, pytest.mark.recovery]
T0 = 1_767_225_600_000
ORDERS = 40


def ev(n, etype, order, amount, pay=None, ref=None, t=T0):
    return {"event_id": f"pd{n}", "event_type": etype, "schema_version": 1, "order_id": order, "payment_id": pay,
            "refund_id": ref, "event_time": format_event_time(t), "producer_id": "test", "producer_seq": n,
            "currency": "GBP", "amount_minor": amount}


def batches():
    """Batch 1: refunds whose payments (and for odd orders, the order itself) are not yet known.
    Batch 2: the missing parents, published only after a consumer restart."""
    first, second, n = [], [], 0
    for i in range(ORDERS):
        o = f"ord{i:03d}"
        def nxt(*a, **k):
            nonlocal n
            n += 1
            return ev(n, *a, **k)
        create = nxt("ORDER_CREATED", o, 1000)
        refunds = [nxt("REFUND_ISSUED", o, 600, pay="p1", ref="r1", t=T0 + 60_000),
                   nxt("REFUND_ISSUED", o, 500, pay="p1", ref="r2", t=T0 + 120_000)]  # r2 must lose (600+500>1000)
        capture = nxt("PAYMENT_CAPTURED", o, 1000, pay="p1", t=T0 + 30_000)
        orphan = nxt("REFUND_ISSUED", o, 1, pay="never", ref="r3")  # stays unresolved forever
        if i % 2:
            first += refunds + [orphan]
            second += [create, capture]
        else:
            first += [create] + refunds + [orphan]
            second += [capture]
    to_em = lambda evs, base: [Emission(base + k, k, e["order_id"].encode(), canonical_json(e), "pending")
                               for k, e in enumerate(evs)]
    return to_em(first, T0 + 1000), to_em(second, T0 + 10_000_000)


def pending_count(db):
    return db.execute("SELECT count(*) FROM pending_dependencies").fetchone()[0]


def test_pending_survives_restart_and_releases_in_admission_order(db, topic):
    first, second = batches()
    publish(first, topic)
    until1 = end_offsets(topic)
    assert StreamConsumer(topic).run(until=until1, timeout_s=120).stopped_because == "settled"
    assert pending_count(db) == ORDERS * 3  # every refund is waiting
    missing = dict(db.execute("SELECT missing, count(*) FROM pending_dependencies GROUP BY 1").fetchall())
    assert missing == {"MISSING_ORDER": ORDERS // 2 * 3, "MISSING_PAYMENT": ORDERS // 2 * 3}
    check_against_reference(db, topic, until1)

    # the consumer is gone; a new process starts later and the parents arrive
    publish(second, topic)
    until2 = end_offsets(topic)
    assert StreamConsumer(topic, instance="c2").run(until=until2, timeout_s=120).stopped_because == "settled"
    assert pending_count(db) == ORDERS  # only the orphans remain
    reasons = dict(db.execute("SELECT refund_id, reason FROM received_events WHERE event_type = 'REFUND_ISSUED' "
                              "AND order_id = 'ord000'").fetchall())
    assert reasons == {"r1": None, "r2": "OVER_REFUND", "r3": "MISSING_PAYMENT"}
    check_against_reference(db, topic, until2)


def test_crash_inside_cascading_release_rolls_back_the_whole_release(db, topic):
    first, second = batches()
    publish(first, topic)
    until1 = end_offsets(topic)
    StreamConsumer(topic).run(until=until1, timeout_s=120)
    publish(second, topic)
    until2 = end_offsets(topic)
    # crash just before COMMIT of the 5th parent: its own effects and all releases it triggered must vanish
    c = StreamConsumer(topic, faults=Faults([Rule("before_commit", 5)]))
    with pytest.raises(FaultInjected):
        c.run(until=until2, timeout_s=120)
    assert_ledger_matches_progress(db, topic)
    resolved_now = db.execute("SELECT count(*) FROM received_events WHERE event_type = 'REFUND_ISSUED' "
                              "AND status <> 'PENDING'").fetchone()[0]
    assert resolved_now % 2 == 0  # releases happen in whole (r1, r2) pairs, never half
    assert StreamConsumer(topic).run(until=until2, timeout_s=120).stopped_because == "settled"
    check_against_reference(db, topic, until2)
    assert pending_count(db) == ORDERS
