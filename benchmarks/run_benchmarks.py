"""Bounded, reproducible measurements of the streaming stack. Raw results go to results/raw/ (git-ignored).

Each experiment uses a fixed seed and event count, a fresh topic, and a fresh
database state. Nothing here is a scalability claim: one broker, one database,
one machine. Run on Linux or WSL2 with the stack up:

    python benchmarks/run_benchmarks.py [--orders 3000] [--only normal,contention,...]
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import signal
import statistics
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from streamledger.consumer import DEFAULT_DSN, StreamConsumer  # noqa: E402
from streamledger.domain.generator import GeneratorConfig, generate  # noqa: E402
from streamledger.kafka_io import create_topic, delete_topic, end_offsets, publish  # noqa: E402
from streamledger.storage.snapshot import progress, reset  # noqa: E402

OUT = ROOT / "results" / "raw"  # may contain wall-clock times; publish only redacted copies


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return round(1000 * xs[min(len(xs) - 1, int(q * len(xs)))], 3)


def docker_mem():
    try:
        out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}"],
                             capture_output=True, text=True, timeout=30).stdout
        return dict(line.split(" ", 1) for line in out.strip().splitlines())
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


class Sampler(threading.Thread):
    """Every `interval` s: lag (end offsets - DB progress), pending rows, sessions waiting on locks."""

    def __init__(self, conn, topic, until, interval=0.5):
        super().__init__(daemon=True)
        self.conn, self.topic, self.until, self.interval = conn, topic, until, interval
        self.samples, self.stop_flag = [], threading.Event()

    def run(self):
        t0 = time.monotonic()
        while not self.stop_flag.is_set():
            prog = progress(self.conn, self.topic)
            pending, lock_waits = self.conn.execute(
                "SELECT (SELECT count(*) FROM pending_dependencies), "
                "(SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'streamledger-%%' "
                "AND wait_event_type = 'Lock')").fetchone()
            self.samples.append({"t": round(time.monotonic() - t0, 2),
                                 "lag": sum(self.until.values()) - sum(prog.values()),
                                 "pending": pending, "lock_waits": lock_waits})
            self.stop_flag.wait(self.interval)


def deadlocks(conn):
    return conn.execute("SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()").fetchone()[0]


def fresh(conn, orders, seed=81):
    reset(conn)
    topic = f"bench-{uuid.uuid4().hex[:8]}"
    create_topic(topic)
    emissions = generate(GeneratorConfig(seed=seed, orders=orders, duplicate_rate=0.05, conflict_rate=0.01,
                                         invalid_rate=0.01))
    t0 = time.monotonic()
    publish(emissions, topic)
    pub_s = time.monotonic() - t0
    return topic, emissions, {"records": len(emissions), "publish_s": round(pub_s, 3),
                              "publish_rate_rps": round(len(emissions) / pub_s, 1)}


def run_consumers(conn, topic, n=1, instance_prefix="c"):
    until = end_offsets(topic)
    sampler = Sampler(conn, topic, until)
    dl0 = deadlocks(conn)
    # bounded benchmark mode: keep every transaction duration (the run is finite) so percentiles are exact
    sample_limit = max(1, sum(until.values()))
    consumers = [StreamConsumer(topic, instance=f"{instance_prefix}{i}", txn_sample_limit=sample_limit)
                 for i in range(n)]
    results = [None] * n
    threads = [threading.Thread(target=lambda i=i: results.__setitem__(i, consumers[i].run(until=until, timeout_s=1800)))
               for i in range(n)]
    sampler.start()
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.monotonic() - t0
    sampler.stop_flag.set(); sampler.join()
    txn = [x for r in results for x in r.txn_seconds]
    txn_exact = all(r.txn.exact for r in results)
    outcomes = {}
    for r in results:
        for k, v in r.outcomes.items():
            outcomes[k] = outcomes.get(k, 0) + v
    total = sum(until.values())
    # rates use the records this run processed, not the topic size (a replay pass starts mid-topic)
    processed = sum(v for k, v in outcomes.items() if k != "SKIPPED")
    return {
        "consumers": n, "topic_records": total, "processed": processed, "wall_s": round(wall, 3),
        "apply_rate_rps": round(processed / wall, 1),
        "outcomes": outcomes,
        "duplicate_rate": round((outcomes.get("DUPLICATE", 0) + outcomes.get("CONFLICT", 0)) / processed, 4),
        "txn_ms": {"p50": pct(txn, 0.5), "p95": pct(txn, 0.95), "p99": pct(txn, 0.99), "max": pct(txn, 1.0),
                   "mean": round(1000 * statistics.fmean(txn), 3) if txn else None,
                   "samples": len(txn), "transactions": sum(r.txn.count for r in results), "exact": txn_exact},
        "retries": sum(r.retries for r in results), "kafka_commit_failures": sum(r.kafka_commit_failures for r in results),
        "stopped": [r.stopped_because for r in results],
        "max_lag": max((s["lag"] for s in sampler.samples), default=None),  # backlog: everything is published first
        "max_pending": max((s["pending"] for s in sampler.samples), default=None),
        "lock_wait_samples": sum(1 for s in sampler.samples if s["lock_waits"]),
        "samples": len(sampler.samples), "deadlocks": deadlocks(conn) - dl0,
        "lag_timeline": sampler.samples,
    }


def exp_normal(conn, orders):
    topic, _, pub = fresh(conn, orders)
    r = run_consumers(conn, topic, 1)
    r.update(publish=pub, container_mem=docker_mem())
    delete_topic(topic)
    return r


def exp_contention(conn, orders):
    topic, _, pub = fresh(conn, orders)
    r = run_consumers(conn, topic, 2)
    r.update(publish=pub, container_mem=docker_mem())
    delete_topic(topic)
    return r


def exp_replay(conn, orders):
    """The whole history published a second time: every record is a redelivery."""
    topic, emissions, pub = fresh(conn, orders)
    first = run_consumers(conn, topic, 1)
    before = end_offsets(topic)
    publish(emissions, topic)
    second = run_consumers(conn, topic, 1)
    second["records_replayed"] = sum(end_offsets(topic).values()) - sum(before.values())
    delete_topic(topic)
    return {"first_pass": {k: v for k, v in first.items() if k != "lag_timeline"}, "replay_pass": second, "publish": pub}


def exp_recovery(conn, orders, session_timeouts=(10_000,)):
    """kill -9 the consumer at ~40% and restart; split restart time into group join and catch-up."""
    out = []
    for _ in session_timeouts:
        topic, _, pub = fresh(conn, orders)
        until = end_offsets(topic)
        total = sum(until.values())
        env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
        cmd = [sys.executable, "-m", "streamledger.consumer", "--topic", topic, "--until-settled"]
        p = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        while sum(progress(conn, topic).values()) < 0.4 * total:
            time.sleep(0.01)
        os.kill(p.pid, signal.SIGKILL); p.wait()
        at_kill = sum(progress(conn, topic).values())
        t0 = time.monotonic()
        p2 = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        first_progress = None
        while p2.poll() is None:
            if first_progress is None and sum(progress(conn, topic).values()) > at_kill:
                first_progress = time.monotonic() - t0
            time.sleep(0.01)
        settled = time.monotonic() - t0
        out.append({"records": total, "killed_at": at_kill, "exit_code": p2.returncode,
                    "restart_to_first_progress_s": round(first_progress, 3) if first_progress else None,
                    "restart_to_settled_s": round(settled, 3),
                    "remaining_records": total - at_kill, "session_timeout_ms": 10_000, "publish": pub})
        delete_topic(topic)
    return out


def exp_roundtrip(conn, n=300):
    """Latency floor of one statement and of one committed write transaction on this setup."""
    def timed(fn):
        xs = []
        for _ in range(n):
            t0 = time.perf_counter(); fn(); xs.append(time.perf_counter() - t0)
        return {"p50": pct(xs, 0.5), "p95": pct(xs, 0.95)}
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS rt (k int PRIMARY KEY, v int)")
    conn.execute("INSERT INTO rt VALUES (1, 0) ON CONFLICT DO NOTHING")

    def write_txn():
        with conn.transaction():
            conn.execute("UPDATE rt SET v = v + 1 WHERE k = 1")

    real = psycopg.connect(DEFAULT_DSN, autocommit=True)
    real.execute("CREATE TABLE IF NOT EXISTS bench_rt (k int PRIMARY KEY, v int)")
    real.execute("INSERT INTO bench_rt VALUES (1, 0) ON CONFLICT DO NOTHING")

    def durable_txn():
        with real.transaction():
            real.execute("UPDATE bench_rt SET v = v + 1 WHERE k = 1")

    r = {"select_1_ms": timed(lambda: conn.execute("SELECT 1").fetchone()),
         "temp_table_write_txn_ms": timed(write_txn),
         "durable_write_txn_ms": timed(durable_txn),
         "synchronous_commit": real.execute("SHOW synchronous_commit").fetchone()[0],
         "fsync": real.execute("SHOW fsync").fetchone()[0]}
    real.execute("DROP TABLE bench_rt"); real.close()
    return r


def host_info():
    def sh(*cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=20).stdout.strip()
        except Exception:  # noqa: BLE001
            return None
    return {"machine": platform.machine(), "kernel": platform.release(), "cpus_visible": os.cpu_count(),
            "mem_total_kb": int(open("/proc/meminfo").read().split()[1]) if Path("/proc/meminfo").exists() else None,
            "python": platform.python_version(), "docker": sh("docker", "version", "--format", "{{.Server.Version}}"),
            "kafka_image": "apache/kafka:3.9.1", "postgres_image": "postgres:17.6-alpine",
            "limits": {"kafka": "1 GiB / 2 CPU, heap 256-512 MiB", "postgres": "512 MiB / 2 CPU, shared_buffers 128MB"},
            "note": os.environ.get("STREAMLEDGER_HOST_NOTE", "")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=3000)
    ap.add_argument("--only", default="roundtrip,normal,contention,replay,recovery")
    a = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    conn = psycopg.connect(os.environ.get("STREAMLEDGER_PG_DSN", DEFAULT_DSN), autocommit=True)
    run = {"started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "orders": a.orders, "host": host_info()}
    experiments = {"roundtrip": lambda: exp_roundtrip(conn), "normal": lambda: exp_normal(conn, a.orders),
                   "contention": lambda: exp_contention(conn, a.orders), "replay": lambda: exp_replay(conn, a.orders),
                   "recovery": lambda: exp_recovery(conn, a.orders)}
    for name in a.only.split(","):
        t0 = time.monotonic()
        run[name] = experiments[name]()
        print(f"{name}: done in {time.monotonic() - t0:.1f}s", flush=True)
    run["bench_process_max_rss_kb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    reset(conn)
    path = OUT / f"bench-{run['started'].replace(':', '')}.json"
    path.write_text(json.dumps(run, indent=1))
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
