"""Per-order admission decisions for the batch job, by *ready offset* rather than by simulation.

This is the third, deliberately different formulation of docs/event-contract.md.
The streaming processor and the Python reference both replay a pending queue.
This module never builds a queue. It relies on three properties of the contract:

  1. Rule groups interact only through fixed facts. Captures and failures compete
     only with each other (per order total, per payment_id). Refunds compete only
     with refunds (per payment), and they see payments only through *when and how*
     the payment was resolved. Cancellations compete only with cancellations.
  2. An event is evaluated at its ready offset: the log position at which its last
     dependency was satisfied. For an event that does not wait, that is its own offset.
  3. Events released at the same ready offset are evaluated in admission order.
     So inside each rule group, the evaluation order is a sort by (ready, offset).

The pending limit (32 deferred events per order) depends on how many events are
waiting at each arrival, which in turn depends on decisions. That is solved by
fixed-point iteration over the set of limit-rejected events.

Input: one order's first-occurrence events (dicts with keys offset, event_id,
event_type, payment_id, refund_id, currency, amount). Output: event_id -> (status, reason).
"""
from __future__ import annotations

MAX_PENDING = 32
INF = float("inf")


def _evaluate(events: list[dict], excluded: set[str]) -> tuple[dict, dict, float, dict]:
    """Return (decision per event_id, ready offset per event_id with INF = never ready,
    order creation offset (INF if none), payment_id -> resolution offset)."""
    decision: dict[str, tuple[str, str | None]] = {}
    ready: dict[str, float] = {}
    creates = [e for e in events if e["event_type"] == "ORDER_CREATED"]
    order = creates[0] if creates else None
    for e in creates[1:]:
        decision[e["event_id"]] = ("REJECTED", "ORDER_ALREADY_EXISTS")
        ready[e["event_id"]] = e["offset"]
    if order is not None:
        decision[order["event_id"]] = ("APPLIED", None)
        ready[order["event_id"]] = order["offset"]

    rest = [e for e in events if e["event_type"] != "ORDER_CREATED" and e["event_id"] not in excluded]
    if order is None:
        for e in rest:
            decision[e["event_id"]] = ("PENDING", "MISSING_ORDER")
            ready[e["event_id"]] = INF
        return decision, ready, INF, {}

    groups: dict[str, list[tuple[float, int, dict]]] = {"cancel": [], "payment": [], "refund": []}
    for e in rest:
        r0 = max(e["offset"], order["offset"])
        if e["currency"] != order["currency"]:
            decision[e["event_id"]] = ("REJECTED", "CURRENCY_MISMATCH")
            ready[e["event_id"]] = r0
            continue
        kind = {"ORDER_CANCELLED": "cancel", "REFUND_ISSUED": "refund"}.get(e["event_type"], "payment")
        groups[kind].append((r0, e["offset"], e))

    cancelled = False
    for r0, _, e in sorted(groups["cancel"], key=lambda t: t[:2]):
        decision[e["event_id"]] = ("REJECTED", "ALREADY_CANCELLED") if cancelled else ("APPLIED", None)
        ready[e["event_id"]] = r0
        cancelled = True

    captured = 0
    resolved: dict[str, tuple[str, int, float]] = {}  # payment_id -> (status, amount, resolved at)
    for r0, _, e in sorted(groups["payment"], key=lambda t: t[:2]):
        ready[e["event_id"]] = r0
        prior = resolved.get(e["payment_id"])
        if e["event_type"] == "PAYMENT_CAPTURED":
            if prior:
                decision[e["event_id"]] = ("REJECTED", "DUPLICATE_CAPTURE" if prior[0] == "CAPTURED"
                                           else "PAYMENT_ALREADY_FAILED")
            elif captured + e["amount"] > order["amount"]:
                decision[e["event_id"]] = ("REJECTED", "OVERCAPTURE")  # payment stays unknown
            else:
                captured += e["amount"]
                resolved[e["payment_id"]] = ("CAPTURED", e["amount"], r0)
                decision[e["event_id"]] = ("APPLIED", None)
        else:  # PAYMENT_FAILED
            if prior:
                decision[e["event_id"]] = ("REJECTED", "PAYMENT_ALREADY_CAPTURED" if prior[0] == "CAPTURED"
                                           else "DUPLICATE_FAILURE")
            else:
                resolved[e["payment_id"]] = ("FAILED", e["amount"], r0)
                decision[e["event_id"]] = ("APPLIED", None)

    refunded: dict[str, int] = {}
    refund_ids: set[str] = set()
    timed = []
    for r0, off, e in groups["refund"]:
        pay = resolved.get(e["payment_id"])
        if pay is None:
            decision[e["event_id"]] = ("PENDING", "MISSING_PAYMENT")
            ready[e["event_id"]] = INF
        else:
            timed.append((max(r0, pay[2]), off, e))
    for r, _, e in sorted(timed, key=lambda t: t[:2]):
        ready[e["event_id"]] = r
        status, amount, _ = resolved[e["payment_id"]]
        if status == "FAILED":
            decision[e["event_id"]] = ("REJECTED", "REFUND_OF_FAILED_PAYMENT")
        elif e["refund_id"] in refund_ids:
            decision[e["event_id"]] = ("REJECTED", "DUPLICATE_REFUND_ID")
        elif refunded.get(e["payment_id"], 0) + e["amount"] > amount:
            decision[e["event_id"]] = ("REJECTED", "OVER_REFUND")
        else:
            refunded[e["payment_id"]] = refunded.get(e["payment_id"], 0) + e["amount"]
            refund_ids.add(e["refund_id"])
            decision[e["event_id"]] = ("APPLIED", None)
    return decision, ready, order["offset"], {pid: v[2] for pid, v in resolved.items()}


def _readiness(e: dict, ready: dict, created_at: float, resolved_at: dict) -> float:
    """Ready offset of any event. For an event excluded from evaluation it is derived from its
    parents alone; no event's own decision affects its own readiness."""
    if e["event_id"] in ready:
        return ready[e["event_id"]]
    if e["event_type"] == "ORDER_CREATED":
        return e["offset"]
    r = max(e["offset"], created_at)
    if e["event_type"] == "REFUND_ISSUED":
        r = max(r, resolved_at.get(e["payment_id"], INF))
    return r


def _limit_rejections(events: list[dict], readiness: dict[str, float]) -> set[str]:
    """In admission order: an event that must wait while MAX_PENDING others already wait is rejected
    (and so never waits itself)."""
    rejected: set[str] = set()
    waiting: list[float] = []  # ready offsets of events that were admitted as waiting
    for e in sorted(events, key=lambda e: e["offset"]):
        r, off = readiness[e["event_id"]], e["offset"]
        if r <= off:
            continue  # did not have to wait
        if sum(1 for w in waiting if w > off) >= MAX_PENDING:
            rejected.add(e["event_id"])
        else:
            waiting.append(r)
    return rejected


def decide(events: list[dict]) -> dict[str, tuple[str, str | None]]:
    excluded: set[str] = set()
    for _ in range(200):
        decision, ready, created_at, resolved_at = _evaluate(events, excluded)
        readiness = {e["event_id"]: _readiness(e, ready, created_at, resolved_at) for e in events}
        new = _limit_rejections(events, readiness)
        if new == excluded:
            for eid in excluded:
                decision[eid] = ("REJECTED", "PENDING_LIMIT")
            return decision
        excluded = new
    raise RuntimeError("pending-limit fixed point did not converge")
