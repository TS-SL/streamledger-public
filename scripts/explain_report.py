"""EXPLAIN (ANALYZE, BUFFERS) for the hot-path and reporting queries, at the current data volume.

    python scripts/explain_report.py --topic T --out results/raw/explain.txt
"""
import argparse
import os
from pathlib import Path

import psycopg

DSN = os.environ.get("STREAMLEDGER_PG_DSN",
                     "postgresql://streamledger:streamledger_local_only@localhost:55432/streamledger")
ROOT = Path(__file__).resolve().parent.parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("ANALYZE")
        sample = conn.execute("SELECT event_id, order_id FROM received_events ORDER BY event_id LIMIT 1 OFFSET 500").fetchone()
        pay = conn.execute("SELECT order_id, payment_id FROM payments LIMIT 1 OFFSET 200").fetchone()
        hot = {
            "dedup lookup by event_id (every record)":
                ("SELECT digest, topic, partition, \"offset\" FROM received_events WHERE event_id = %s", (sample[0],)),
            "progress row lock (every record)":
                ("SELECT next_offset FROM consumer_progress WHERE topic = %s AND partition = 0 FOR UPDATE", (a.topic,)),
            "order state (every non-create event)":
                ("SELECT currency, total_minor, status, captured_minor FROM orders WHERE order_id = %s", (sample[1],)),
            "payment state (payment/refund events)":
                ("SELECT status, amount_minor, refunded_minor FROM payments WHERE order_id = %s AND payment_id = %s", pay),
            "pending for order (after every APPLIED event)":
                ("SELECT p.event_id, p.missing, e.payload, e.broker_ts FROM pending_dependencies p JOIN received_events e "
                 "USING (event_id) WHERE p.order_id = %s ORDER BY p.admission_offset", (sample[1],)),
            "order totals trigger: refund sum (deferred, per affected order)":
                ("SELECT COALESCE(sum(amount_minor), 0) FROM refunds WHERE order_id = %s", (sample[1],)),
        }
        counts = {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                  for t in ("received_events", "orders", "payments", "refunds", "pending_dependencies",
                            "duplicate_deliveries", "event_conflicts", "processing_errors")}
        out = [f"# EXPLAIN (ANALYZE, BUFFERS), PostgreSQL 17.6, topic {a.topic}", f"# row counts: {counts}", ""]
        for name, (sql, params) in hot.items():
            out.append(f"## {name}\n{sql}")
            with conn.transaction():
                out += [r[0] for r in conn.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql, params).fetchall()]
            out.append("")
        for f in sorted((ROOT / "sql" / "queries").glob("*.sql")):
            body = f.read_text()
            sql = "\n".join(l for l in body.splitlines() if not l.strip().startswith("--")).strip().rstrip(";")
            out.append(f"## report: {f.name}")
            out += [r[0] for r in conn.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql).fetchall()]
            out.append("")
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text("\n".join(out) + "\n")
    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
