"""Reference evaluator: the expected settled state as a pure function of the log.

Written from docs/event-contract.md and docs/consistency-contract.md section 4.
It is the oracle for the streaming consumer and the PySpark job, so it must
stay independent of them: no imports from storage/consumer code, no SQL.

Input: broker records (partition, offset, broker timestamp, key, value).
Output: ReferenceState. Records are consumed in a canonical merge order that
preserves per-partition offset order; since every decision is local to one
order (= one partition), the result does not depend on how partitions are
interleaved, except for cross-order event_id collisions (excluded by contract).
"""
from __future__ import annotations

import heapq
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable

from streamledger.domain.events import (
    Event,
    EventType,
    InvalidEvent,
    LogRecord,
    format_event_time,
    parse_event,
)

DAY_MS = 86_400_000
DEFAULT_GRACE_MS = 3_600_000
MAX_PENDING_PER_ORDER = 32

APPLIED, DUPLICATE, CONFLICT, PENDING, REJECTED, INVALID = (
    "APPLIED", "DUPLICATE", "CONFLICT", "PENDING", "REJECTED", "INVALID",
)


@dataclass
class Order:
    order_id: str
    currency: str
    total_minor: int
    status: str = "OPEN"
    captured_minor: int = 0
    refunded_minor: int = 0

    @property
    def net_paid_minor(self) -> int:
        return self.captured_minor - self.refunded_minor

    @property
    def flags(self) -> tuple[str, ...]:
        return ("CANCELLED_WITH_BALANCE",) if self.status == "CANCELLED" and self.net_paid_minor else ()


@dataclass
class Payment:
    status: str  # CAPTURED | FAILED
    amount_minor: int
    refunded_minor: int = 0


@dataclass
class PeriodTotals:
    captured_minor: int = 0
    refunded_minor: int = 0
    late_captured_minor: int = 0
    late_refunded_minor: int = 0


@dataclass(frozen=True)
class Outcome:
    partition: int
    offset: int
    event_id: str | None
    status: str  # final status of this log position
    reason: str | None


@dataclass
class ReferenceState:
    orders: dict[str, Order] = field(default_factory=dict)
    payments: dict[tuple[str, str], Payment] = field(default_factory=dict)
    refunds: dict[tuple[str, str], tuple[str, int]] = field(default_factory=dict)  # -> (payment_id, amount)
    periods: dict[tuple[str, str], PeriodTotals] = field(default_factory=lambda: defaultdict(PeriodTotals))
    # event_id -> (digest, status, reason); status is final or PENDING
    events: dict[str, list] = field(default_factory=dict)
    # order_id -> [(admission position, record ts, Event)] in admission order
    pending: dict[str, list] = field(default_factory=lambda: defaultdict(list))
    pending_reason: dict[str, str] = field(default_factory=dict)  # event_id -> MISSING_*
    conflicts: list[tuple[str, str, str]] = field(default_factory=list)  # (event_id, kept digest, rejected digest)
    invalid: list[tuple[int, int, str]] = field(default_factory=list)  # (partition, offset, reason)
    positions: dict[tuple[int, int], Outcome] = field(default_factory=dict)

    def status_counts(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for o in self.positions.values():
            counts[o.status] += 1
        return dict(counts)

    def unresolved(self) -> list[tuple[str, str, str]]:
        """(order_id, event_id, missing dependency) still pending."""
        return sorted(
            (oid, ev.event_id, self.pending_reason[ev.event_id])
            for oid, items in self.pending.items()
            for _, _, ev in items
        )


def period_of(event_time_ms: int) -> tuple[str, int]:
    """UTC date string and the period's end (exclusive) in epoch ms."""
    start = event_time_ms - event_time_ms % DAY_MS
    return format_event_time(start)[:10], start + DAY_MS


def _decide(st: ReferenceState, ev: Event) -> tuple[str, str | None]:
    """Return (APPLIED|PENDING|REJECTED, reason) without mutating state."""
    t = ev.event_type
    order = st.orders.get(ev.order_id)
    if t is EventType.ORDER_CREATED:
        return (REJECTED, "ORDER_ALREADY_EXISTS") if order else (APPLIED, None)
    if order is None:
        return PENDING, "MISSING_ORDER"
    if ev.currency != order.currency:
        return REJECTED, "CURRENCY_MISMATCH"
    if t is EventType.ORDER_CANCELLED:
        return (REJECTED, "ALREADY_CANCELLED") if order.status == "CANCELLED" else (APPLIED, None)

    pay = st.payments.get((ev.order_id, ev.payment_id))
    if t is EventType.PAYMENT_CAPTURED:
        if pay is not None:
            return REJECTED, "DUPLICATE_CAPTURE" if pay.status == "CAPTURED" else "PAYMENT_ALREADY_FAILED"
        if order.captured_minor + ev.amount_minor > order.total_minor:
            return REJECTED, "OVERCAPTURE"
        return APPLIED, None
    if t is EventType.PAYMENT_FAILED:
        if pay is not None:
            return REJECTED, "PAYMENT_ALREADY_CAPTURED" if pay.status == "CAPTURED" else "DUPLICATE_FAILURE"
        return APPLIED, None
    # REFUND_ISSUED
    if pay is None:
        return PENDING, "MISSING_PAYMENT"
    if pay.status == "FAILED":
        return REJECTED, "REFUND_OF_FAILED_PAYMENT"
    if (ev.order_id, ev.refund_id) in st.refunds:
        return REJECTED, "DUPLICATE_REFUND_ID"
    if pay.refunded_minor + ev.amount_minor > pay.amount_minor:
        return REJECTED, "OVER_REFUND"
    return APPLIED, None


def _apply(st: ReferenceState, ev: Event, record_ts_ms: int, grace_ms: int) -> None:
    t = ev.event_type
    if t is EventType.ORDER_CREATED:
        st.orders[ev.order_id] = Order(ev.order_id, ev.currency, ev.amount_minor)
        return
    order = st.orders[ev.order_id]
    if t is EventType.ORDER_CANCELLED:
        order.status = "CANCELLED"
        return
    if t is EventType.PAYMENT_FAILED:
        st.payments[(ev.order_id, ev.payment_id)] = Payment("FAILED", ev.amount_minor)
        return

    period, period_end = period_of(ev.event_time_ms)
    totals = st.periods[(period, ev.currency)]
    late = record_ts_ms > period_end + grace_ms
    if t is EventType.PAYMENT_CAPTURED:
        st.payments[(ev.order_id, ev.payment_id)] = Payment("CAPTURED", ev.amount_minor)
        order.captured_minor += ev.amount_minor
        totals.captured_minor += ev.amount_minor
        if late:
            totals.late_captured_minor += ev.amount_minor
    else:  # REFUND_ISSUED
        st.payments[(ev.order_id, ev.payment_id)].refunded_minor += ev.amount_minor
        st.refunds[(ev.order_id, ev.refund_id)] = (ev.payment_id, ev.amount_minor)
        order.refunded_minor += ev.amount_minor
        totals.refunded_minor += ev.amount_minor
        if late:
            totals.late_refunded_minor += ev.amount_minor


def _finish(st: ReferenceState, pos: tuple[int, int], ev: Event, status: str, reason: str | None) -> None:
    st.events[ev.event_id][1:] = [status, reason]
    st.positions[pos] = Outcome(pos[0], pos[1], ev.event_id, status, reason)


def _release_pending(st: ReferenceState, order_id: str, grace_ms: int) -> None:
    """Re-evaluate deferred events of one order, in admission order, to a fixpoint."""
    changed = True
    while changed and st.pending.get(order_id):
        changed = False
        for item in list(st.pending[order_id]):
            pos, ts, ev = item
            status, reason = _decide(st, ev)
            if status == PENDING:
                if st.pending_reason[ev.event_id] != reason:  # e.g. order arrived, payment still missing
                    st.pending_reason[ev.event_id] = reason
                    _finish(st, pos, ev, PENDING, reason)
                continue
            st.pending[order_id].remove(item)
            del st.pending_reason[ev.event_id]
            if status == APPLIED:
                _apply(st, ev, ts, grace_ms)
            _finish(st, pos, ev, status, reason)
            changed = True
            break  # restart from the earliest pending event after any state change
    if order_id in st.pending and not st.pending[order_id]:
        del st.pending[order_id]


def merge_order(records: Iterable[LogRecord]) -> list[LogRecord]:
    """Canonical consumption order: k-way merge on (broker ts, partition, offset),
    never reordering records within a partition."""
    by_part: dict[int, list[LogRecord]] = defaultdict(list)
    for r in records:
        by_part[r.partition].append(r)
    heads = []
    for p, rs in by_part.items():
        rs.sort(key=lambda r: r.offset)
        heads.append(((rs[0].timestamp_ms, p, rs[0].offset), p, 0))
    heapq.heapify(heads)
    out = []
    while heads:
        _, p, i = heapq.heappop(heads)
        out.append(by_part[p][i])
        if i + 1 < len(by_part[p]):
            r = by_part[p][i + 1]
            heapq.heappush(heads, ((r.timestamp_ms, p, r.offset), p, i + 1))
    return out


def evaluate(records: Iterable[LogRecord], grace_ms: int = DEFAULT_GRACE_MS) -> ReferenceState:
    st = ReferenceState()
    seen_positions: set[tuple[int, int]] = set()
    for rec in merge_order(records):
        pos = (rec.partition, rec.offset)
        if pos in seen_positions:  # the same log position observed twice
            continue
        seen_positions.add(pos)
        try:
            ev = parse_event(rec.value, rec.key)
        except InvalidEvent as exc:
            st.invalid.append((rec.partition, rec.offset, exc.reason))
            st.positions[pos] = Outcome(rec.partition, rec.offset, exc.event_id, INVALID, exc.reason)
            continue

        digest = ev.digest()
        prior = st.events.get(ev.event_id)
        if prior is not None:
            if prior[0] == digest:
                st.positions[pos] = Outcome(rec.partition, rec.offset, ev.event_id, DUPLICATE, None)
            else:
                st.conflicts.append((ev.event_id, prior[0], digest))
                st.positions[pos] = Outcome(rec.partition, rec.offset, ev.event_id, CONFLICT, "EVENT_ID_CONFLICT")
            continue

        st.events[ev.event_id] = [digest, None, None]
        status, reason = _decide(st, ev)
        if status == PENDING:
            if len(st.pending[ev.order_id]) >= MAX_PENDING_PER_ORDER:
                status, reason = REJECTED, "PENDING_LIMIT"
            else:
                st.pending[ev.order_id].append((pos, rec.timestamp_ms, ev))
                st.pending_reason[ev.event_id] = reason
                _finish(st, pos, ev, PENDING, reason)
                continue
        if status == APPLIED:
            _apply(st, ev, rec.timestamp_ms, grace_ms)
        _finish(st, pos, ev, status, reason)
        if status == APPLIED:
            _release_pending(st, ev.order_id, grace_ms)
    return st
