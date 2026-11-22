"""Transactional record processor: one broker record -> one PostgreSQL transaction.

Implements docs/consistency-contract.md section 6 and the decision table in
docs/event-contract.md against PostgreSQL state. Business decisions read state
only after taking the per-order advisory lock, so READ COMMITTED is enough.

Outcomes returned by process():
    APPLIED / PENDING / REJECTED  first occurrence of an event_id
    DUPLICATE                     identical event_id + digest at a new position
    CONFLICT                      same event_id, different digest
    INVALID                       record failed schema validation
    REPLAYED                      this exact position was already recorded (fence bypassed)
    SKIPPED                       position below the progress fence; nothing done
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from streamledger.domain.events import Event, EventType, InvalidEvent, LogRecord, parse_event
from streamledger.faults import FaultInjected, Faults  # noqa: F401  (re-exported)

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
DAY_MS = 86_400_000
DEFAULT_GRACE_MS = 3_600_000
MAX_PENDING_PER_ORDER = 32


def ms_to_dt(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=ms)


def dt_to_ms(dt: datetime) -> int:
    return (dt - EPOCH) // timedelta(milliseconds=1)


class GapError(RuntimeError):
    """A record arrived above the progress fence: something was skipped upstream."""


class Processor:
    def __init__(self, conn, owner: str = "streamledger", grace_ms: int = DEFAULT_GRACE_MS,
                 faults: Faults | None = None, max_pending: int = MAX_PENDING_PER_ORDER):
        if not conn.autocommit:
            raise ValueError("Processor needs an autocommit connection; it manages transactions itself")
        self.conn = conn
        self.owner = owner
        self.grace_ms = grace_ms
        self.faults = faults or Faults()
        self.max_pending = max_pending

    # ------------------------------------------------------------------ entry point
    def process(self, rec: LogRecord) -> str:
        self.faults.hit("before_begin")
        with self.conn.transaction():
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO consumer_progress (last_owner_group, topic, partition, next_offset) "
                "VALUES (%s, %s, %s, 0) ON CONFLICT DO NOTHING",
                (self.owner, rec.topic, rec.partition),
            )
            (next_offset,) = cur.execute(
                "SELECT next_offset FROM consumer_progress WHERE topic = %s AND partition = %s FOR UPDATE",
                (rec.topic, rec.partition),
            ).fetchone()
            if rec.offset < next_offset:
                return "SKIPPED"
            if rec.offset > next_offset:
                raise GapError(f"{rec.topic}[{rec.partition}] expected offset {next_offset}, got {rec.offset}")
            self.faults.hit("after_lock")
            outcome = self._handle(cur, rec)
            cur.execute(
                "UPDATE consumer_progress SET next_offset = %s, last_owner_group = %s, updated_at = now() "
                "WHERE topic = %s AND partition = %s",
                (rec.offset + 1, self.owner, rec.topic, rec.partition),
            )
            self.faults.hit("before_commit")
        self.faults.hit("after_commit")
        return outcome

    # ------------------------------------------------------------------ classification
    def _handle(self, cur, rec: LogRecord) -> str:
        ts = ms_to_dt(rec.timestamp_ms)
        try:
            ev = parse_event(rec.value, rec.key)
        except InvalidEvent as exc:
            cur.execute(
                'INSERT INTO processing_errors (topic, partition, "offset", broker_ts, reason, detail, event_id, '
                "raw_key, raw_value) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (rec.topic, rec.partition, rec.offset, ts, exc.reason, exc.detail or None, exc.event_id,
                 rec.key, rec.value),
            )
            return "INVALID"

        cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (ev.order_id,))
        digest = ev.digest()
        prior = cur.execute(
            'SELECT digest, topic, partition, "offset" FROM received_events WHERE event_id = %s', (ev.event_id,)
        ).fetchone()
        if prior is not None:
            if tuple(prior[1:]) == (rec.topic, rec.partition, rec.offset):
                return "REPLAYED"
            if prior[0] == digest:
                cur.execute(
                    'INSERT INTO duplicate_deliveries (topic, partition, "offset", event_id, broker_ts) '
                    "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (rec.topic, rec.partition, rec.offset, ev.event_id, ts),
                )
                return "DUPLICATE"
            cur.execute(
                'INSERT INTO event_conflicts (topic, partition, "offset", event_id, payload, digest, broker_ts) '
                "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (rec.topic, rec.partition, rec.offset, ev.event_id, ev.canonical_bytes(), digest, ts),
            )
            return "CONFLICT"

        status, reason = self._decide(cur, ev)
        if status == "PENDING":
            (n,) = cur.execute("SELECT count(*) FROM pending_dependencies WHERE order_id = %s",
                               (ev.order_id,)).fetchone()
            if n >= self.max_pending:
                status, reason = "REJECTED", "PENDING_LIMIT"
        cur.execute(
            "INSERT INTO received_events (event_id, payload, digest, event_type, order_id, payment_id, refund_id, "
            'currency, amount_minor, event_time, topic, partition, "offset", broker_ts, status, reason, resolved_at) '
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, CASE WHEN %s = 'PENDING' THEN NULL ELSE now() END)",
            (ev.event_id, ev.canonical_bytes(), digest, ev.event_type.value, ev.order_id, ev.payment_id,
             ev.refund_id, ev.currency, ev.amount_minor, ms_to_dt(ev.event_time_ms), rec.topic, rec.partition,
             rec.offset, ts, status, reason, status),
        )
        if status == "PENDING":
            cur.execute(
                "INSERT INTO pending_dependencies (event_id, order_id, missing, payment_id, admission_offset) "
                "VALUES (%s,%s,%s,%s,%s)",
                (ev.event_id, ev.order_id, reason, ev.payment_id if reason == "MISSING_PAYMENT" else None, rec.offset),
            )
        elif status == "APPLIED":
            self._apply(cur, ev, rec.timestamp_ms)
            self._release_pending(cur, ev.order_id)
        return status

    # ------------------------------------------------------------------ business rules
    def _decide(self, cur, ev: Event) -> tuple[str, str | None]:
        order = cur.execute(
            "SELECT currency, total_minor, status, captured_minor FROM orders WHERE order_id = %s", (ev.order_id,)
        ).fetchone()
        if ev.event_type is EventType.ORDER_CREATED:
            return ("REJECTED", "ORDER_ALREADY_EXISTS") if order else ("APPLIED", None)
        if order is None:
            return "PENDING", "MISSING_ORDER"
        currency, total, order_status, captured = order
        if ev.currency != currency.strip():
            return "REJECTED", "CURRENCY_MISMATCH"
        if ev.event_type is EventType.ORDER_CANCELLED:
            return ("REJECTED", "ALREADY_CANCELLED") if order_status == "CANCELLED" else ("APPLIED", None)

        payment = cur.execute(
            "SELECT status, amount_minor, refunded_minor FROM payments WHERE order_id = %s AND payment_id = %s",
            (ev.order_id, ev.payment_id),
        ).fetchone()
        if ev.event_type is EventType.PAYMENT_CAPTURED:
            if payment:
                return "REJECTED", ("DUPLICATE_CAPTURE" if payment[0] == "CAPTURED" else "PAYMENT_ALREADY_FAILED")
            if captured + ev.amount_minor > total:
                return "REJECTED", "OVERCAPTURE"
            return "APPLIED", None
        if ev.event_type is EventType.PAYMENT_FAILED:
            if payment:
                return "REJECTED", ("PAYMENT_ALREADY_CAPTURED" if payment[0] == "CAPTURED" else "DUPLICATE_FAILURE")
            return "APPLIED", None

        # REFUND_ISSUED
        if payment is None:
            return "PENDING", "MISSING_PAYMENT"
        pay_status, pay_amount, pay_refunded = payment
        if pay_status == "FAILED":
            return "REJECTED", "REFUND_OF_FAILED_PAYMENT"
        if cur.execute("SELECT 1 FROM refunds WHERE order_id = %s AND refund_id = %s",
                       (ev.order_id, ev.refund_id)).fetchone():
            return "REJECTED", "DUPLICATE_REFUND_ID"
        if pay_refunded + ev.amount_minor > pay_amount:
            return "REJECTED", "OVER_REFUND"
        return "APPLIED", None

    def _apply(self, cur, ev: Event, record_ts_ms: int) -> None:
        t = ev.event_type
        if t is EventType.ORDER_CREATED:
            cur.execute("INSERT INTO orders (order_id, currency, total_minor, created_event_id) VALUES (%s,%s,%s,%s)",
                        (ev.order_id, ev.currency, ev.amount_minor, ev.event_id))
        elif t is EventType.ORDER_CANCELLED:
            cur.execute("UPDATE orders SET status = 'CANCELLED', cancelled_event_id = %s WHERE order_id = %s",
                        (ev.event_id, ev.order_id))
        elif t is EventType.PAYMENT_FAILED:
            cur.execute("INSERT INTO payments (order_id, payment_id, status, amount_minor, event_id) "
                        "VALUES (%s,%s,'FAILED',%s,%s)", (ev.order_id, ev.payment_id, ev.amount_minor, ev.event_id))
        elif t is EventType.PAYMENT_CAPTURED:
            cur.execute("INSERT INTO payments (order_id, payment_id, status, amount_minor, event_id) "
                        "VALUES (%s,%s,'CAPTURED',%s,%s)", (ev.order_id, ev.payment_id, ev.amount_minor, ev.event_id))
            cur.execute("UPDATE orders SET captured_minor = captured_minor + %s WHERE order_id = %s",
                        (ev.amount_minor, ev.order_id))
            self._project(cur, ev, record_ts_ms, captured=ev.amount_minor)
        else:  # REFUND_ISSUED
            cur.execute("INSERT INTO refunds (order_id, refund_id, payment_id, amount_minor, event_id) "
                        "VALUES (%s,%s,%s,%s,%s)", (ev.order_id, ev.refund_id, ev.payment_id, ev.amount_minor, ev.event_id))
            cur.execute("UPDATE payments SET refunded_minor = refunded_minor + %s WHERE order_id = %s AND payment_id = %s",
                        (ev.amount_minor, ev.order_id, ev.payment_id))
            cur.execute("UPDATE orders SET refunded_minor = refunded_minor + %s WHERE order_id = %s",
                        (ev.amount_minor, ev.order_id))
            self._project(cur, ev, record_ts_ms, refunded=ev.amount_minor)

    def _project(self, cur, ev: Event, record_ts_ms: int, captured: int = 0, refunded: int = 0) -> None:
        day_start = ev.event_time_ms - ev.event_time_ms % DAY_MS
        late = record_ts_ms > day_start + DAY_MS + self.grace_ms
        cur.execute(
            "INSERT INTO financial_projection AS fp (period_date, currency, captured_minor, refunded_minor, "
            "late_captured_minor, late_refunded_minor) VALUES (%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (period_date, currency) DO UPDATE SET "
            "captured_minor = fp.captured_minor + EXCLUDED.captured_minor, "
            "refunded_minor = fp.refunded_minor + EXCLUDED.refunded_minor, "
            "late_captured_minor = fp.late_captured_minor + EXCLUDED.late_captured_minor, "
            "late_refunded_minor = fp.late_refunded_minor + EXCLUDED.late_refunded_minor",
            (ms_to_dt(day_start).date(), ev.currency, captured, refunded,
             captured if late else 0, refunded if late else 0),
        )

    def _release_pending(self, cur, order_id: str) -> None:
        """Re-evaluate the order's deferred events in admission order; restart after every change."""
        while True:
            rows = cur.execute(
                "SELECT p.event_id, p.missing, e.payload, e.broker_ts FROM pending_dependencies p "
                "JOIN received_events e USING (event_id) WHERE p.order_id = %s ORDER BY p.admission_offset",
                (order_id,),
            ).fetchall()
            progressed = False
            for event_id, missing, payload, broker_ts in rows:
                ev = parse_event(bytes(payload))
                status, reason = self._decide(cur, ev)
                if status == "PENDING":
                    if reason != missing:
                        cur.execute(
                            "UPDATE pending_dependencies SET missing = %s, payment_id = %s WHERE event_id = %s",
                            (reason, ev.payment_id if reason == "MISSING_PAYMENT" else None, event_id))
                        cur.execute("UPDATE received_events SET reason = %s WHERE event_id = %s", (reason, event_id))
                    continue
                cur.execute("UPDATE received_events SET status = %s, reason = %s, resolved_at = now() "
                            "WHERE event_id = %s", (status, reason, event_id))
                cur.execute("DELETE FROM pending_dependencies WHERE event_id = %s", (event_id,))
                if status == "APPLIED":
                    self._apply(cur, ev, dt_to_ms(broker_ts))
                progressed = True
                break
            if not progressed:
                return
