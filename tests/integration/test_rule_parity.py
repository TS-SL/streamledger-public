"""Each contract rule scenario, run through the PostgreSQL processor and the reference.

Targets edge cases the random workload rarely produces (cascading release with
competing refunds, pending limit, cancellation with balance). No Kafka needed:
records are fed to the Processor directly, in log order.
"""
import pytest

from streamledger.domain.events import LogRecord, canonical_json, format_event_time
from streamledger.reference.model import MAX_PENDING_PER_ORDER, evaluate
from streamledger.storage.processor import Processor
from streamledger.storage.snapshot import db_view
from streamledger.verify import diff, reference_view

pytestmark = pytest.mark.integration
T0 = 1_767_225_600_000
DAY = 86_400_000
TOPIC = "parity"


class Ids:
    n = 0

    @classmethod
    def next(cls):
        cls.n += 1
        return f"e{cls.n}"


def ev(etype, amount=0, order="o1", pay=None, ref=None, cur="USD", t=T0):
    return {"event_id": Ids.next(), "event_type": etype, "schema_version": 1, "order_id": order,
            "payment_id": pay, "refund_id": ref, "event_time": format_event_time(t), "producer_id": "test",
            "producer_seq": 0, "currency": cur, "amount_minor": amount}


CREATE = lambda total=1000, **kw: ev("ORDER_CREATED", total, **kw)
CAPTURE = lambda amount=1000, pay="p1", **kw: ev("PAYMENT_CAPTURED", amount, pay=pay, **kw)
FAIL = lambda amount=1000, pay="p1", **kw: ev("PAYMENT_FAILED", amount, pay=pay, **kw)
REFUND = lambda amount, ref="r1", pay="p1", **kw: ev("REFUND_ISSUED", amount, pay=pay, ref=ref, **kw)
CANCEL = lambda **kw: ev("ORDER_CANCELLED", **kw)


def scenarios():
    c = CAPTURE()
    r = REFUND(300)
    conflict = CAPTURE(600, pay="p9")
    yield "redelivery", [CREATE(), c, c, c]
    yield "conflict", [CREATE(), conflict, dict(conflict, amount_minor=900)]
    yield "refund_before_capture", [CREATE(), REFUND(400), CAPTURE()]
    yield "cascade_before_order", [REFUND(300), CAPTURE(), CREATE()]
    yield "reason_moves_to_payment", [REFUND(300), CREATE()]
    yield "deferred_refund_of_failed", [CREATE(), REFUND(300), FAIL()]
    yield "over_refund_order", [CREATE(), CAPTURE(), REFUND(600, "r1"), REFUND(500, "r2"), REFUND(400, "r3")]
    yield "release_in_admission_order", [CREATE(), REFUND(500, "r2"), REFUND(600, "r1"), CAPTURE()]
    yield "cascade_restart_capacity", [REFUND(600, "r1"), CAPTURE(), REFUND(500, "r2"), CREATE()]
    yield "payment_state_conflicts", [CREATE(), CAPTURE(400, "p1"), CAPTURE(400, "p1"), FAIL(400, "p1"),
                                      FAIL(400, "p2"), CAPTURE(400, "p2"), CAPTURE(700, "p3"), CAPTURE(600, "p4")]
    yield "currency_and_ids", [CREATE(), CREATE(), CAPTURE(cur="EUR"), CAPTURE(), REFUND(10, "r1"), REFUND(10, "r1")]
    yield "cancel_with_balance", [CREATE(), CAPTURE(), CANCEL(), CANCEL()]
    yield "pending_limit", [CREATE()] + [REFUND(1, ref=f"r{i}", pay=f"px{i}") for i in range(MAX_PENDING_PER_ORDER + 2)]
    # an event that does not need to wait is never limit-rejected, even with the queue full
    yield "pending_limit_then_ready_event", ([CREATE()] + [REFUND(1, ref=f"r{i}", pay=f"px{i}")
                                             for i in range(MAX_PENDING_PER_ORDER + 1)] + [CAPTURE(10, pay="unrelated")])
    # same refund_id on two payments: the one that becomes *ready* first wins, not the lower offset
    yield "refund_id_reuse_across_payments", [CREATE(2000), CAPTURE(1000, pay="p2"), REFUND(100, ref="r1", pay="p1"),
                                              REFUND(100, ref="r1", pay="p2"), CAPTURE(1000, pay="p1")]
    yield "pending_replay", [CREATE(), r, r, CAPTURE(), r]
    yield "invalid", [CREATE(), b"\x00garbage", CAPTURE(amount=-1)]
    yield "two_orders_interleaved", [CREATE(order="a"), CREATE(order="b", cur="EUR"), REFUND(5, order="a"),
                                     CAPTURE(order="b", cur="EUR"), CAPTURE(order="a"), REFUND(1000, order="b", cur="EUR")]


def as_log(items, late_last=False):
    out = []
    for i, e in enumerate(items):
        value = e if isinstance(e, bytes) else canonical_json(e)
        key = e["order_id"].encode() if isinstance(e, dict) else b"o1"
        out.append(LogRecord(TOPIC, 0, i, T0 + 1000 * i, key, value))
    return out


@pytest.mark.parametrize("name,items", list(scenarios()), ids=[n for n, _ in scenarios()])
def test_processor_matches_reference(db, name, items):
    records = as_log(items)
    proc = Processor(db)
    for rec in records:
        proc.process(rec)
    problems = diff(reference_view(evaluate(records)), db_view(db, TOPIC))
    assert problems == [], "\n".join(problems)


def test_late_refund_restatement_matches_reference(db):
    items = [CREATE(), CAPTURE(), REFUND(250, t=T0 + 3_600_000)]
    records = [LogRecord(TOPIC, 0, i, ts, b"o1", canonical_json(e))
               for i, (e, ts) in enumerate(zip(items, [T0, T0 + 1, T0 + 2 * DAY]))]
    proc = Processor(db)
    for rec in records:
        proc.process(rec)
    view = db_view(db, TOPIC)
    assert diff(reference_view(evaluate(records)), view) == []
    assert view["periods"][("2026-01-01", "USD")] == (1000, 250, 0, 250)
