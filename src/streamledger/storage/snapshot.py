"""Read the persisted projection into the comparison shape used by verify.py."""
from __future__ import annotations

TABLES = ("pending_dependencies", "duplicate_deliveries", "event_conflicts", "processing_errors",
          "refunds", "payments", "orders", "financial_projection", "received_events", "consumer_progress")


def reset(conn) -> None:
    """Delete all projection state (tests / full rebuilds only)."""
    conn.execute("TRUNCATE " + ", ".join(TABLES) + " CASCADE")


def db_view(conn, topic: str) -> dict:
    q = lambda sql, *a: conn.execute(sql, a).fetchall()
    return {
        "orders": {r[0]: (r[1].strip(), r[2], r[3], r[4], r[5]) for r in q(
            "SELECT order_id, currency, total_minor, status, captured_minor, refunded_minor FROM orders")},
        "payments": {(r[0], r[1]): (r[2], r[3], r[4]) for r in q(
            "SELECT order_id, payment_id, status, amount_minor, refunded_minor FROM payments")},
        "refunds": {(r[0], r[1]): (r[2], r[3]) for r in q(
            "SELECT order_id, refund_id, payment_id, amount_minor FROM refunds")},
        "periods": {(r[0].isoformat(), r[1].strip()): (r[2], r[3], r[4], r[5]) for r in q(
            "SELECT period_date, currency, captured_minor, refunded_minor, late_captured_minor, "
            "late_refunded_minor FROM financial_projection WHERE captured_minor + refunded_minor > 0")},
        "unresolved": sorted(tuple(r) for r in q(
            "SELECT order_id, event_id, missing FROM pending_dependencies")),
        "positions": {(r[0], r[1]): (r[2], r[3]) for r in q(
            'SELECT partition, "offset", status, reason FROM position_ledger WHERE topic = %s', topic)},
        "conflicts": sorted(r[0] for r in q("SELECT event_id FROM event_conflicts WHERE topic = %s", topic)),
    }


def progress(conn, topic: str) -> dict[int, int]:
    return dict(conn.execute("SELECT partition, next_offset FROM consumer_progress WHERE topic = %s",
                             (topic,)).fetchall())
