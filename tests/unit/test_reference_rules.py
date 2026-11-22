"""Hand-computed expectations for each contract rule (docs/event-contract.md)."""
import itertools

from streamledger.domain.events import LogRecord, canonical_json, format_event_time
from streamledger.reference.model import MAX_PENDING_PER_ORDER, evaluate

T0 = 1_767_225_600_000  # 2026-01-01T00:00:00Z
_ids = itertools.count(1)


def ev(etype, amount=0, order="o1", pay=None, ref=None, cur="USD", t=T0, eid=None):
    return {
        "event_id": eid or f"e{next(_ids)}",
        "event_type": etype,
        "schema_version": 1,
        "order_id": order,
        "payment_id": pay,
        "refund_id": ref,
        "event_time": format_event_time(t),
        "producer_id": "test",
        "producer_seq": 0,
        "currency": cur,
        "amount_minor": amount,
    }


def log(*events, ts=None, partition=0):
    """Records in the given order on one partition; ts defaults to event time + 1s."""
    out = []
    for i, e in enumerate(events):
        value = e if isinstance(e, bytes) else canonical_json(e)
        key = e["order_id"].encode() if isinstance(e, dict) else b"o1"
        t = ts[i] if ts else (T0 + 1000 * i)
        out.append(LogRecord("t", partition, i, t, key, value))
    return out


def statuses(st):
    return [(o.status, o.reason) for o in sorted(st.positions.values(), key=lambda o: o.offset)]


CREATE = lambda **kw: ev("ORDER_CREATED", 1000, **kw)
CAPTURE = lambda amount=1000, pay="p1", **kw: ev("PAYMENT_CAPTURED", amount, pay=pay, **kw)
REFUND = lambda amount, ref="r1", pay="p1", **kw: ev("REFUND_ISSUED", amount, pay=pay, ref=ref, **kw)


def test_identical_redelivery_is_counted_once():
    c = CAPTURE()
    st = evaluate(log(CREATE(), c, c, c))
    assert st.orders["o1"].captured_minor == 1000
    assert [s for s, _ in statuses(st)] == ["APPLIED", "APPLIED", "DUPLICATE", "DUPLICATE"]


def test_same_log_position_seen_twice_is_ignored():
    records = log(CREATE(), CAPTURE())
    st = evaluate(records + records)
    assert st.orders["o1"].captured_minor == 1000
    assert len(st.positions) == 2


def test_conflicting_payload_under_same_event_id_is_kept_as_evidence_not_applied():
    c = CAPTURE(amount=600)
    st = evaluate(log(CREATE(), c, dict(c, amount_minor=900)))
    assert st.orders["o1"].captured_minor == 600
    assert statuses(st)[2] == ("CONFLICT", "EVENT_ID_CONFLICT")
    assert len(st.conflicts) == 1 and st.conflicts[0][0] == c["event_id"]


def test_refund_before_capture_is_deferred_then_applied():
    st = evaluate(log(CREATE(), REFUND(400), CAPTURE()))
    assert statuses(st) == [("APPLIED", None)] * 3
    assert st.orders["o1"].net_paid_minor == 600
    assert st.unresolved() == []


def test_cascade_payment_and_refund_before_order():
    st = evaluate(log(REFUND(300), CAPTURE(), CREATE()))
    assert all(s == "APPLIED" for s, _ in statuses(st))
    assert st.orders["o1"].refunded_minor == 300


def test_pending_reason_moves_from_order_to_payment():
    st = evaluate(log(REFUND(300), CREATE()))
    assert statuses(st)[0] == ("PENDING", "MISSING_PAYMENT")
    assert st.unresolved() == [("o1", st.positions[(0, 0)].event_id, "MISSING_PAYMENT")]


def test_deferred_refund_of_failed_payment_is_rejected_on_release():
    st = evaluate(log(CREATE(), REFUND(300), ev("PAYMENT_FAILED", 1000, pay="p1")))
    assert statuses(st)[1] == ("REJECTED", "REFUND_OF_FAILED_PAYMENT")
    assert st.orders["o1"].refunded_minor == 0 and st.unresolved() == []


def test_partial_and_over_refund_first_in_admission_order_wins():
    st = evaluate(log(CREATE(), CAPTURE(), REFUND(600, "r1"), REFUND(500, "r2"), REFUND(400, "r3")))
    assert [r for _, r in statuses(st)[2:]] == [None, "OVER_REFUND", None]
    assert st.orders["o1"].refunded_minor == 1000


def test_deferred_events_release_in_admission_order_not_arrival_of_parent():
    st = evaluate(log(CREATE(), REFUND(500, "r2"), REFUND(600, "r1"), CAPTURE()))
    assert statuses(st)[1:3] == [("APPLIED", None), ("REJECTED", "OVER_REFUND")]


def test_cascading_release_restarts_so_earlier_deferred_event_wins_capacity():
    # all three wait for the order; once the capture is released, the refund admitted
    # *before* it (600) must be evaluated before the refund admitted after it (500)
    st = evaluate(log(REFUND(600, "r1"), CAPTURE(), REFUND(500, "r2"), CREATE()))
    assert statuses(st)[:3] == [("APPLIED", None), ("APPLIED", None), ("REJECTED", "OVER_REFUND")]


def test_duplicate_capture_failed_payment_and_overcapture():
    st = evaluate(log(
        CREATE(),
        CAPTURE(400, "p1"),
        CAPTURE(400, "p1"),  # new event_id, same payment
        ev("PAYMENT_FAILED", 400, pay="p1"),
        ev("PAYMENT_FAILED", 400, pay="p2"),
        CAPTURE(400, "p2"),
        CAPTURE(700, "p3"),  # 400 + 700 > 1000
        CAPTURE(600, "p4"),
    ))
    assert [r for _, r in statuses(st)[2:]] == [
        "DUPLICATE_CAPTURE", "PAYMENT_ALREADY_CAPTURED", None, "PAYMENT_ALREADY_FAILED", "OVERCAPTURE", None,
    ]
    assert st.orders["o1"].captured_minor == 1000


def test_currency_mismatch_and_duplicate_order_and_refund_id():
    st = evaluate(log(
        CREATE(), CREATE(), CAPTURE(cur="EUR"), CAPTURE(), REFUND(10, "r1"), REFUND(10, "r1"),
    ))
    assert [r for _, r in statuses(st)] == [
        None, "ORDER_ALREADY_EXISTS", "CURRENCY_MISMATCH", None, None, "DUPLICATE_REFUND_ID",
    ]


def test_cancellation_keeps_money_and_flags_balance():
    st = evaluate(log(CREATE(), CAPTURE(), ev("ORDER_CANCELLED"), ev("ORDER_CANCELLED")))
    o = st.orders["o1"]
    assert o.status == "CANCELLED" and o.captured_minor == 1000
    assert o.flags == ("CANCELLED_WITH_BALANCE",)
    assert statuses(st)[3] == ("REJECTED", "ALREADY_CANCELLED")
    st2 = evaluate(log(CREATE(), CAPTURE(), REFUND(1000), ev("ORDER_CANCELLED")))
    assert st2.orders["o1"].flags == ()


def test_late_refund_restates_its_event_time_period():
    day = 86_400_000
    refund = REFUND(250, t=T0 + 3_600_000)
    st = evaluate(log(CREATE(), CAPTURE(), refund, ts=[T0, T0 + 1, T0 + 2 * day]))
    p = st.periods[("2026-01-01", "USD")]
    assert (p.captured_minor, p.refunded_minor, p.late_captured_minor, p.late_refunded_minor) == (1000, 250, 0, 250)
    # within the 1h grace after period end: not late
    st2 = evaluate(log(CREATE(), CAPTURE(), REFUND(250), ts=[T0, T0 + 1, T0 + day + 3_599_999]))
    assert st2.periods[("2026-01-01", "USD")].late_refunded_minor == 0


def test_pending_limit_is_bounded_and_deterministic():
    refunds = [REFUND(1, ref=f"r{i}", pay=f"px{i}") for i in range(MAX_PENDING_PER_ORDER + 2)]
    st = evaluate(log(CREATE(), *refunds))
    s = statuses(st)[1:]
    assert s.count(("PENDING", "MISSING_PAYMENT")) == MAX_PENDING_PER_ORDER
    assert s[-2:] == [("REJECTED", "PENDING_LIMIT")] * 2


def test_invalid_records_are_recorded_with_position():
    st = evaluate(log(CREATE(), b"\x00garbage", CAPTURE(amount=-1)))
    assert st.invalid == [(0, 1, "UNDECODABLE"), (0, 2, "BAD_AMOUNT")]
    assert st.status_counts() == {"APPLIED": 1, "INVALID": 2}


def test_replay_of_pending_event_is_duplicate_not_reapplied():
    r = REFUND(300)
    st = evaluate(log(CREATE(), r, r, CAPTURE(), r))
    assert [s for s, _ in statuses(st)] == ["APPLIED", "APPLIED", "DUPLICATE", "APPLIED", "DUPLICATE"]
    assert st.orders["o1"].refunded_minor == 300
