"""The schema itself must refuse invalid financial states, independent of Python logic.

Requires the compose PostgreSQL with migrations applied. Each test runs in a
transaction that is rolled back, so it leaves no data behind.
"""
import hashlib
import os

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg import errors  # noqa: E402

DSN = os.environ.get(
    "STREAMLEDGER_PG_DSN", "postgresql://streamledger:streamledger_local_only@localhost:55432/streamledger"
)
pytestmark = pytest.mark.integration


@pytest.fixture
def cur():
    try:
        conn = psycopg.connect(DSN, connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    try:
        with conn.cursor() as c:
            from streamledger.storage.snapshot import TABLES
            c.execute("TRUNCATE " + ", ".join(TABLES) + " CASCADE")  # inside the txn; undone by rollback
            yield c
    finally:
        conn.rollback()
        conn.close()


_n = iter(range(10**6))


def event(cur, etype="ORDER_CREATED", order="o1", amount=1000, status="APPLIED", reason=None,
          payment=None, refund=None, payload=None):
    i = next(_n)
    eid = f"e{i}"
    payload = payload or f'{{"event_id":"{eid}"}}'.encode()
    cur.execute(
        """INSERT INTO received_events (event_id, payload, digest, event_type, order_id, payment_id, refund_id,
               currency, amount_minor, event_time, topic, partition, "offset", broker_ts, status, reason)
           VALUES (%s,%s,%s,%s,%s,%s,%s,'USD',%s,now(),'t',0,%s,now(),%s,%s)""",
        (eid, payload, hashlib.sha256(payload).hexdigest(), etype, order, payment, refund, amount, i, status, reason),
    )
    return eid


def order(cur, oid="o1", total=1000):
    cur.execute("INSERT INTO orders (order_id, currency, total_minor, created_event_id) VALUES (%s,'USD',%s,%s)",
                (oid, total, event(cur, order=oid, amount=total)))


def capture(cur, oid="o1", pid="p1", amount=1000):
    eid = event(cur, "PAYMENT_CAPTURED", oid, amount, payment=pid)
    cur.execute("INSERT INTO payments (order_id, payment_id, status, amount_minor, event_id) VALUES (%s,%s,'CAPTURED',%s,%s)",
                (oid, pid, amount, eid))
    cur.execute("UPDATE orders SET captured_minor = captured_minor + %s WHERE order_id = %s", (amount, oid))


def commit_check(cur):
    """Fire deferred constraints now without committing."""
    cur.execute("SET CONSTRAINTS ALL IMMEDIATE")


def test_consistent_order_payment_refund_is_accepted(cur):
    order(cur); capture(cur)
    eid = event(cur, "REFUND_ISSUED", amount=400, payment="p1", refund="r1")
    cur.execute("INSERT INTO refunds VALUES ('o1','r1','p1',400,%s)", (eid,))
    cur.execute("UPDATE payments SET refunded_minor = 400 WHERE payment_id = 'p1'")
    cur.execute("UPDATE orders SET refunded_minor = 400 WHERE order_id = 'o1'")
    commit_check(cur)


def test_digest_must_match_payload(cur):
    with pytest.raises(errors.CheckViolation, match="digest_matches_payload"):
        cur.execute(
            """INSERT INTO received_events (event_id, payload, digest, event_type, order_id, currency, amount_minor,
                   event_time, topic, partition, "offset", broker_ts, status)
               VALUES ('x', 'abc', %s, 'ORDER_CREATED', 'o1', 'USD', 1, now(), 't', 0, 999999, now(), 'APPLIED')""",
            ("0" * 64,),
        )


def test_overcapture_is_refused(cur):
    order(cur, total=1000)
    with pytest.raises(errors.CheckViolation, match="no_overcapture"):
        capture(cur, amount=1001)


def test_over_refund_on_payment_is_refused(cur):
    order(cur); capture(cur, amount=500)
    with pytest.raises(errors.CheckViolation, match="no_over_refund"):
        cur.execute("UPDATE payments SET refunded_minor = 501 WHERE payment_id = 'p1'")


def test_refund_without_payment_is_refused(cur):
    order(cur)
    eid = event(cur, "REFUND_ISSUED", amount=1, payment="nope", refund="r1")
    with pytest.raises(errors.ForeignKeyViolation):
        cur.execute("INSERT INTO refunds VALUES ('o1','r1','nope',1,%s)", (eid,))


def test_denormalized_totals_must_match_detail_rows_at_commit(cur):
    order(cur)
    eid = event(cur, "PAYMENT_CAPTURED", payment="p1")
    cur.execute("INSERT INTO payments (order_id, payment_id, status, amount_minor, event_id) VALUES ('o1','p1','CAPTURED',1000,%s)", (eid,))
    # forgot to bump orders.captured_minor: allowed mid-transaction, refused at commit
    with pytest.raises(errors.CheckViolation, match="disagree with detail rows"):
        commit_check(cur)


def test_negative_amount_is_refused(cur):
    with pytest.raises(errors.CheckViolation):
        event(cur, amount=-1)


def test_applied_event_cannot_carry_reason_and_rejected_must(cur):
    with pytest.raises(errors.CheckViolation, match="reason_iff_not_applied"):
        event(cur, status="REJECTED", reason=None)


def test_pending_row_requires_event_to_stay_pending(cur):
    eid = event(cur, "REFUND_ISSUED", status="PENDING", reason="MISSING_PAYMENT", payment="p1", refund="r1")
    cur.execute("INSERT INTO pending_dependencies (event_id, order_id, missing, payment_id, admission_offset) "
                "VALUES (%s,'o1','MISSING_PAYMENT','p1',0)", (eid,))
    commit_check(cur)
    cur.execute("SET CONSTRAINTS ALL DEFERRED")
    cur.execute("UPDATE received_events SET status='APPLIED', reason=NULL, resolved_at=now() WHERE event_id=%s", (eid,))
    with pytest.raises(errors.ForeignKeyViolation):
        commit_check(cur)  # resolved but the pending row was not removed


def test_pending_release_with_row_removed_is_accepted(cur):
    eid = event(cur, "REFUND_ISSUED", status="PENDING", reason="MISSING_PAYMENT", payment="p1", refund="r1")
    cur.execute("INSERT INTO pending_dependencies (event_id, order_id, missing, payment_id, admission_offset) "
                "VALUES (%s,'o1','MISSING_PAYMENT','p1',0)", (eid,))
    cur.execute("UPDATE received_events SET status='REJECTED', reason='REFUND_OF_FAILED_PAYMENT', resolved_at=now() WHERE event_id=%s", (eid,))
    cur.execute("DELETE FROM pending_dependencies WHERE event_id=%s", (eid,))
    commit_check(cur)


def test_projection_late_columns_are_bounded(cur):
    with pytest.raises(errors.CheckViolation):
        cur.execute("INSERT INTO financial_projection (period_date, currency, captured_minor, late_captured_minor) "
                    "VALUES ('2026-01-01','USD',10,11)")
