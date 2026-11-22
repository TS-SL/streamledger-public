"""Deterministic synthetic commerce workload.

Same (seed, config) -> byte-identical emissions. Nothing here resembles real
payment data: ids are opaque hex strings, amounts are random integers.

The generator emits *producer sends* (emit time, key, value). `simulate_log`
turns those into broker records the way a single-broker topic would: append in
send order, partition by key, assign offsets and a monotone LogAppendTime.
A real Kafka run replaces `simulate_log`; the archived records then feed the
same reference model.
"""
from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass

from streamledger.domain.events import LogRecord, canonical_json, format_event_time

BASE_TIME_MS = 1_767_225_600_000  # 2026-01-01T00:00:00.000Z
MINUTE_MS = 60_000
TOPIC = "commerce-events"

SCENARIOS = (
    # name, weight
    ("paid", 30),
    ("split_payment", 8),
    ("partial_refund", 10),
    ("refund_and_cancel", 6),
    ("failed_then_paid", 8),
    ("cancelled_unpaid", 5),
    ("refund_before_capture", 8),
    ("payment_before_order", 6),
    ("over_refund", 4),
    ("orphan_refund", 3),
    ("currency_mismatch", 3),
    ("late_refund", 5),
    ("overcapture", 4),
)


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 7
    orders: int = 200
    duplicate_rate: float = 0.05  # identical resend of an event
    conflict_rate: float = 0.01  # same event_id, altered amount
    invalid_rate: float = 0.01  # malformed / unsupported records
    days: int = 3  # spread of order start times


@dataclass(frozen=True)
class Emission:
    emit_ts_ms: int
    seq: int  # tie-breaker; producer send order
    key: bytes | None
    value: bytes
    label: str  # scenario/fault tag, for diagnostics only


def stable_partition(key: bytes | None, partitions: int) -> int:
    if key is None:
        return 0
    return int.from_bytes(hashlib.sha256(key).digest()[:4], "big") % partitions


class _Ids:
    def __init__(self, seed: int):
        self._seed = seed
        self._n = 0

    def __call__(self, prefix: str) -> str:
        self._n += 1
        h = hashlib.sha256(f"{self._seed}:{prefix}:{self._n}".encode()).hexdigest()[:20]
        return f"{prefix}_{h}"


def generate(cfg: GeneratorConfig = GeneratorConfig()) -> list[Emission]:
    rng = random.Random(cfg.seed)
    new_id = _Ids(cfg.seed)
    names = [n for n, _ in SCENARIOS]
    weights = [w for _, w in SCENARIOS]
    producer_seq = {"orders": 0, "payments": 0, "refunds": 0}
    raw: list[tuple[int, dict, str]] = []  # (emit_ts, wire dict, label)

    def ev(etype, producer, order_id, t_ms, amount, currency, payment_id=None, refund_id=None):
        producer_seq[producer] += 1
        return {
            "event_id": new_id("evt"),
            "event_type": etype,
            "schema_version": 1,
            "order_id": order_id,
            "payment_id": payment_id,
            "refund_id": refund_id,
            "event_time": format_event_time(t_ms),
            "producer_id": f"{producer}-svc",
            "producer_seq": producer_seq[producer],
            "currency": currency,
            "amount_minor": amount,
        }

    for _ in range(cfg.orders):
        scenario = rng.choices(names, weights)[0]
        oid = new_id("ord")
        cur = rng.choice(("USD", "EUR", "GBP"))
        total = rng.randint(500, 50_000)
        t0 = BASE_TIME_MS + rng.randint(0, cfg.days * 1440 - 1) * MINUTE_MS
        delay = lambda: rng.randint(50, 2_000)  # normal producer->broker delay (ms)
        out: list[tuple[int, dict]] = []

        def add(e, t, extra_delay=0):
            out.append((t + delay() + extra_delay, e))

        create = ev("ORDER_CREATED", "orders", oid, t0, total, cur)
        p1 = new_id("pay")
        t1, t2, t3 = t0 + 2 * MINUTE_MS, t0 + 30 * MINUTE_MS, t0 + 90 * MINUTE_MS

        if scenario == "paid":
            add(create, t0); add(ev("PAYMENT_CAPTURED", "payments", oid, t1, total, cur, p1), t1)
        elif scenario == "split_payment":
            a = rng.randint(1, total - 1)
            add(create, t0)
            add(ev("PAYMENT_CAPTURED", "payments", oid, t1, a, cur, p1), t1)
            add(ev("PAYMENT_CAPTURED", "payments", oid, t2, total - a, cur, new_id("pay")), t2)
        elif scenario == "partial_refund":
            add(create, t0); add(ev("PAYMENT_CAPTURED", "payments", oid, t1, total, cur, p1), t1)
            add(ev("REFUND_ISSUED", "refunds", oid, t2, rng.randint(1, total - 1), cur, p1, new_id("ref")), t2)
        elif scenario == "refund_and_cancel":
            add(create, t0); add(ev("PAYMENT_CAPTURED", "payments", oid, t1, total, cur, p1), t1)
            add(ev("REFUND_ISSUED", "refunds", oid, t2, total, cur, p1, new_id("ref")), t2)
            add(ev("ORDER_CANCELLED", "orders", oid, t3, 0, cur), t3)
        elif scenario == "failed_then_paid":
            add(create, t0); add(ev("PAYMENT_FAILED", "payments", oid, t1, total, cur, p1), t1)
            add(ev("PAYMENT_CAPTURED", "payments", oid, t2, total, cur, new_id("pay")), t2)
        elif scenario == "cancelled_unpaid":
            add(create, t0); add(ev("ORDER_CANCELLED", "orders", oid, t1, 0, cur), t1)
        elif scenario == "refund_before_capture":
            # payment service is slow to publish; refund service publishes first
            add(create, t0)
            add(ev("PAYMENT_CAPTURED", "payments", oid, t1, total, cur, p1), t1, extra_delay=60 * MINUTE_MS)
            add(ev("REFUND_ISSUED", "refunds", oid, t2, rng.randint(1, total), cur, p1, new_id("ref")), t2)
        elif scenario == "payment_before_order":
            add(create, t0, extra_delay=10 * MINUTE_MS)
            add(ev("PAYMENT_CAPTURED", "payments", oid, t1, total, cur, p1), t1)
        elif scenario == "over_refund":
            add(create, t0); add(ev("PAYMENT_CAPTURED", "payments", oid, t1, total, cur, p1), t1)
            add(ev("REFUND_ISSUED", "refunds", oid, t2, total, cur, p1, new_id("ref")), t2)
            add(ev("REFUND_ISSUED", "refunds", oid, t3, rng.randint(1, total), cur, p1, new_id("ref")), t3)
        elif scenario == "orphan_refund":
            add(create, t0)
            add(ev("REFUND_ISSUED", "refunds", oid, t1, rng.randint(1, total), cur, new_id("pay"), new_id("ref")), t1)
        elif scenario == "currency_mismatch":
            other = "EUR" if cur != "EUR" else "USD"
            add(create, t0); add(ev("PAYMENT_CAPTURED", "payments", oid, t1, total, other, p1), t1)
        elif scenario == "late_refund":
            # refund happened on day D but reaches the broker two days later
            add(create, t0); add(ev("PAYMENT_CAPTURED", "payments", oid, t1, total, cur, p1), t1)
            add(ev("REFUND_ISSUED", "refunds", oid, t2, rng.randint(1, total), cur, p1, new_id("ref")), t2,
                extra_delay=2 * 1440 * MINUTE_MS)
        elif scenario == "overcapture":
            add(create, t0)
            add(ev("PAYMENT_CAPTURED", "payments", oid, t1, total, cur, p1), t1)
            add(ev("PAYMENT_CAPTURED", "payments", oid, t2, rng.randint(1, total), cur, new_id("pay")), t2)

        for emit_ts, e in out:
            raw.append((emit_ts, e, scenario))

    emissions: list[tuple[int, bytes | None, bytes, str]] = []
    for emit_ts, e, label in raw:
        key = e["order_id"].encode()
        emissions.append((emit_ts, key, canonical_json(e), label))
        r = rng.random()
        if r < cfg.duplicate_rate:
            # producer retry: identical content, sometimes a different serialization
            body = canonical_json(e) if rng.random() < 0.5 else json.dumps(e, indent=1).encode()
            emissions.append((emit_ts + rng.randint(1, 5_000), key, body, "fault:duplicate"))
        elif r < cfg.duplicate_rate + cfg.conflict_rate:
            bad = dict(e, amount_minor=e["amount_minor"] + 1) if e["amount_minor"] else dict(e, producer_seq=0)
            emissions.append((emit_ts + rng.randint(1, 5_000), key, canonical_json(bad), "fault:conflict"))
        elif r < cfg.duplicate_rate + cfg.conflict_rate + cfg.invalid_rate:
            kind = rng.choice(("garbage", "negative", "version", "float", "key"))
            if kind == "garbage":
                body = b"\x00not-json\xff"
            elif kind == "negative":
                body = canonical_json(dict(e, event_id=new_id("evt"), amount_minor=-abs(e["amount_minor"] or 1)))
            elif kind == "version":
                body = canonical_json(dict(e, event_id=new_id("evt"), schema_version=2))
            elif kind == "float":
                body = canonical_json(dict(e, event_id=new_id("evt"), amount_minor=float(e["amount_minor"]) + 0.5))
            else:
                body = canonical_json(dict(e, event_id=new_id("evt")))
                key = b"wrong-key"
            emissions.append((emit_ts + rng.randint(1, 5_000), key, body, f"fault:invalid:{kind}"))

    return [Emission(ts, i, k, v, lbl) for i, (ts, k, v, lbl) in enumerate(emissions)]


def simulate_log(emissions: list[Emission], partitions: int = 3, topic: str = TOPIC) -> list[LogRecord]:
    """Append sends in (emit time, send seq) order; LogAppendTime is monotone per partition."""
    next_offset = [0] * partitions
    last_ts = [0] * partitions
    out = []
    for em in sorted(emissions, key=lambda e: (e.emit_ts_ms, e.seq)):
        p = stable_partition(em.key, partitions)
        ts = max(em.emit_ts_ms, last_ts[p])
        out.append(LogRecord(topic, p, next_offset[p], ts, em.key, em.value))
        next_offset[p] += 1
        last_ts[p] = ts
    return out
