"""Apply sql/migrations/*.sql in order, once each, recording a checksum.

Refuses to run if an already-applied migration file has changed: migrations
are append-only. Each migration runs in its own transaction.

    python scripts/migrate.py            # uses STREAMLEDGER_PG_DSN or the local default
"""
from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

import psycopg

DEFAULT_DSN = "postgresql://streamledger:streamledger_local_only@localhost:55432/streamledger"
MIGRATIONS = Path(__file__).resolve().parent.parent / "sql" / "migrations"


def main() -> int:
    dsn = os.environ.get("STREAMLEDGER_PG_DSN", DEFAULT_DSN)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " version text PRIMARY KEY, sha256 char(64) NOT NULL,"
            " applied_at timestamptz NOT NULL DEFAULT now())"
        )
        applied = dict(conn.execute("SELECT version, sha256 FROM schema_migrations").fetchall())
        for path in sorted(MIGRATIONS.glob("*.sql")):
            sql = path.read_bytes()
            digest = hashlib.sha256(sql).hexdigest()
            if path.name in applied:
                if applied[path.name] != digest:
                    print(f"ERROR {path.name} changed after being applied", file=sys.stderr)
                    return 1
                print(f"skip  {path.name}")
                continue
            with conn.transaction():
                conn.execute(sql.decode("utf-8"))
                conn.execute("INSERT INTO schema_migrations (version, sha256) VALUES (%s, %s)", (path.name, digest))
            print(f"apply {path.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
