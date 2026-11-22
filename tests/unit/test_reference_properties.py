"""Properties of the reference model over the synthetic workload."""
import random
from collections import Counter, defaultdict
from dataclasses import replace

import pytest

from streamledger.domain.events import InvalidEvent, parse_event
from streamledger.domain.generator import GeneratorConfig, generate, simulate_log
from streamledger.reference.model import evaluate

CFG = GeneratorConfig(seed=11, orders=400, duplicate_rate=0.08, conflict_rate=0.02, invalid_rate=0.02)


def financial_snapshot(st):
    return {
        "orders": {k: (o.currency, o.total_minor, o.status, o.captured_minor, o.refunded_minor)
                   for k, o in st.orders.items()},
        "payments": {k: (p.status, p.amount_minor, p.refunded_minor) for k, p in st.payments.items()},
        "refunds": dict(st.refunds),
        "periods": {k: vars(v).copy() for k, v in st.periods.items()},
        "unresolved": st.unresolved(),
        "conflicts": sorted(e for e, _, _ in st.conflicts),
    }


@pytest.fixture(scope="module")
def records():
    return simulate_log(generate(CFG), partitions=3)


@pytest.fixture(scope="module")
def state(records):
    return evaluate(records)


def test_generator_is_deterministic():
    a, b = generate(CFG), generate(CFG)
    assert a == b
    assert generate(replace(CFG, seed=12)) != a


def test_workload_exercises_every_outcome(state):
    counts = state.status_counts()
    for status in ("APPLIED", "DUPLICATE", "CONFLICT", "PENDING", "REJECTED", "INVALID"):
        assert counts.get(status, 0) > 0, status
    reasons = Counter(o.reason for o in state.positions.values())
    for r in ("OVER_REFUND", "OVERCAPTURE", "CURRENCY_MISMATCH", "MISSING_PAYMENT", "UNSUPPORTED_SCHEMA_VERSION"):
        assert reasons[r] > 0, r
    assert any(v.late_refunded_minor for v in state.periods.values())


def test_every_log_position_has_exactly_one_outcome(records, state):
    assert set(state.positions) == {(r.partition, r.offset) for r in records}


def test_financial_invariants(state):
    for o in state.orders.values():
        assert 0 <= o.refunded_minor <= o.captured_minor <= o.total_minor
    for p in state.payments.values():
        assert p.refunded_minor <= (p.amount_minor if p.status == "CAPTURED" else 0)
    by_order = defaultdict(lambda: [0, 0])
    for (oid, _), p in state.payments.items():
        if p.status == "CAPTURED":
            by_order[oid][0] += p.amount_minor
    for (oid, _), (_, amount) in state.refunds.items():
        by_order[oid][1] += amount
    for oid, o in state.orders.items():
        assert by_order[oid] == [o.captured_minor, o.refunded_minor]
    # period totals are a re-partition of the same money
    for cur in ("USD", "EUR", "GBP"):
        cap = sum(v.captured_minor for (_, c), v in state.periods.items() if c == cur)
        ref = sum(v.refunded_minor for (_, c), v in state.periods.items() if c == cur)
        assert cap == sum(o.captured_minor for o in state.orders.values() if o.currency == cur)
        assert ref == sum(o.refunded_minor for o in state.orders.values() if o.currency == cur)


def test_full_replay_at_new_offsets_changes_nothing(records, state):
    """Simulates a producer re-publishing the whole history (redelivery at new positions)."""
    n = {p: max(r.offset for r in records if r.partition == p) + 1 for p in {r.partition for r in records}}
    replayed = [replace(r, offset=r.offset + n[r.partition], timestamp_ms=r.timestamp_ms + 10**10) for r in records]
    st2 = evaluate(records + replayed)
    a, b = financial_snapshot(state), financial_snapshot(st2)
    # a replayed conflicting payload is reported again; the set of conflicting ids is unchanged
    assert set(a.pop("conflicts")) == set(b.pop("conflicts"))
    assert a == b
    replay_status = Counter(st2.positions[(r.partition, r.offset)].status for r in replayed)
    assert set(replay_status) <= {"DUPLICATE", "CONFLICT", "INVALID"}


def test_result_is_independent_of_cross_partition_interleaving(records, state):
    """Per-partition order is fixed by the log; how partitions interleave must not matter."""
    rng = random.Random(3)
    shift = {p: rng.randint(-10**9, 10**9) for p in {r.partition for r in records}}
    shuffled = [replace(r, timestamp_ms=r.timestamp_ms + shift[r.partition]) for r in records]
    rng.shuffle(shuffled)  # input order must not matter either
    # lateness is defined by broker timestamps, so compare everything except late_* columns
    a, b = financial_snapshot(state), financial_snapshot(evaluate(shuffled))
    for snap in (a, b):
        snap["periods"] = {k: (v["captured_minor"], v["refunded_minor"]) for k, v in snap["periods"].items()}
    assert a == b


def naive_fold(records):
    """Deliberately wrong: applies every parseable capture/refund it sees, no dedup."""
    net = defaultdict(int)
    for r in records:
        try:
            e = parse_event(r.value, r.key)
        except InvalidEvent:
            continue
        if e.event_type.value == "PAYMENT_CAPTURED":
            net[e.order_id] += e.amount_minor
        elif e.event_type.value == "REFUND_ISSUED":
            net[e.order_id] -= e.amount_minor
    return dict(net)


def test_naive_fold_is_caught_by_the_reference(records, state):
    expected = {k: o.net_paid_minor for k, o in state.orders.items() if o.captured_minor or o.refunded_minor}
    naive = {k: v for k, v in naive_fold(records).items() if v}
    assert naive != expected
