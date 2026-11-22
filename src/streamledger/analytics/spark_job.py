"""PySpark reconstruction of the settled financial state from the Parquet archive.

Independent of the streaming processor and the Python reference:
  * dedup / conflict detection: a window over event_id in canonical log order
  * admission decisions: per-order fold by ready offset (analytics/admission.py)
  * money: Spark SQL aggregations over the accepted events only

Shared with the other implementations: the wire-contract parser
(domain/events.parse_event), so schema validation is not independently checked
here; it has its own unit tests. Everything downstream of parsing is separate.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from streamledger.archive import load_manifest, verify

DAY_MS = 86_400_000
DEFAULT_GRACE_MS = 3_600_000

DECODED_SCHEMA = ("partition int, offset long, timestamp_ms long, valid boolean, reason string, event_id string, "
                  "event_type string, order_id string, payment_id string, refund_id string, currency string, "
                  "amount long, event_time_ms long, digest string")


SRC_DIR = str(Path(__file__).resolve().parents[2])  # .../src, so Python workers can import streamledger


def session(cores: int = 2, driver_memory: str = "1g") -> SparkSession:
    os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [SRC_DIR, os.environ.get("PYTHONPATH")]))
    # Python workers must use this interpreter (with pandas/pyarrow), not whatever `python3` is on PATH;
    # without this the job only worked when the venv happened to be activated
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return (SparkSession.builder.master(f"local[{cores}]").appName("streamledger-reconcile")
            .config("spark.executorEnv.PYTHONPATH", os.environ["PYTHONPATH"])
            .config("spark.driver.memory", driver_memory)
            .config("spark.sql.shuffle.partitions", str(cores * 2))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())


def load_archive(spark: SparkSession, d: Path, topic: str, cutoff: dict[int, int]) -> DataFrame:
    v = verify(d, topic)
    if not v.complete_up_to(cutoff):
        raise ValueError(f"archive not complete up to {cutoff}: covered={v.complete_to} problems={v.problems}")
    files = [str(d / f["file"]) for f in load_manifest(d, topic)["files"]]
    raw = spark.read.parquet(*files)
    cut = spark.createDataFrame([(p, e) for p, e in cutoff.items()], "cp int, cend long")
    return raw.join(F.broadcast(cut), raw.partition == cut.cp).where(F.col("offset") < F.col("cend")).drop("cp", "cend")


_DECODED_DTYPES = {"partition": "int32", "offset": "int64", "timestamp_ms": "int64", "valid": "bool",
                   "reason": "string", "event_id": "string", "event_type": "string", "order_id": "string",
                   "payment_id": "string", "refund_id": "string", "currency": "string", "amount": "Int64",
                   "event_time_ms": "Int64", "digest": "string"}


def _decode(batches):
    import pandas as pd

    from streamledger.domain.events import InvalidEvent, parse_event

    for pdf in batches:
        rows = []
        for r in pdf.itertuples(index=False):
            base = {"partition": r.partition, "offset": r.offset, "timestamp_ms": r.timestamp_ms}
            try:
                ev = parse_event(r.value, r.key)
            except InvalidEvent as exc:
                rows.append({**base, "valid": False, "reason": exc.reason})
                continue
            rows.append({**base, "valid": True, "reason": None, "event_id": ev.event_id,
                         "event_type": ev.event_type.value, "order_id": ev.order_id, "payment_id": ev.payment_id,
                         "refund_id": ev.refund_id, "currency": ev.currency, "amount": ev.amount_minor,
                         "event_time_ms": ev.event_time_ms, "digest": ev.digest()})
        if not rows:
            continue
        out = pd.DataFrame(rows, columns=[c.split()[0] for c in DECODED_SCHEMA.split(", ")])
        yield out.astype(_DECODED_DTYPES)


def _decide_order(pdf):
    import pandas as pd

    from streamledger.analytics.admission import decide

    evs = [{"offset": int(r.offset), "event_id": r.event_id, "event_type": r.event_type, "payment_id": r.payment_id,
            "refund_id": r.refund_id, "currency": r.currency, "amount": int(r.amount)}
           for r in pdf.sort_values(["partition", "offset"]).itertuples(index=False)]
    d = decide(evs)
    return pd.DataFrame([(eid, s, why) for eid, (s, why) in d.items()], columns=["event_id", "status", "reason"])


def reconstruct(raw: DataFrame, grace_ms: int = DEFAULT_GRACE_MS) -> dict:
    decoded = raw.select("partition", "offset", "timestamp_ms", "key", "value").mapInPandas(_decode, DECODED_SCHEMA)
    decoded = decoded.cache()
    invalid = decoded.where(~F.col("valid"))
    valid = decoded.where("valid")

    w = Window.partitionBy("event_id").orderBy("timestamp_ms", "partition", "offset")
    ranked = (valid.withColumn("rn", F.row_number().over(w))
              .withColumn("first_digest", F.first("digest").over(w)))
    firsts = ranked.where("rn = 1")
    repeats = ranked.where("rn > 1").withColumn(
        "status", F.when(F.col("digest") == F.col("first_digest"), "DUPLICATE").otherwise("CONFLICT"))

    decisions = firsts.groupBy("order_id").applyInPandas(_decide_order, "event_id string, status string, reason string")
    decided = firsts.drop("reason").join(decisions, "event_id").cache()
    applied = decided.where("status = 'APPLIED'")

    creates = applied.where("event_type = 'ORDER_CREATED'").select("order_id", "currency", F.col("amount").alias("total"))
    cancels = applied.where("event_type = 'ORDER_CANCELLED'").select("order_id").distinct().withColumn("cancelled", F.lit(True))
    caps = applied.where("event_type = 'PAYMENT_CAPTURED'")
    refs = applied.where("event_type = 'REFUND_ISSUED'")
    cap_sum = caps.groupBy("order_id").agg(F.sum("amount").alias("captured"))
    ref_sum = refs.groupBy("order_id").agg(F.sum("amount").alias("refunded"))
    orders = (creates.join(cancels, "order_id", "left").join(cap_sum, "order_id", "left").join(ref_sum, "order_id", "left")
              .select("order_id", "currency", "total",
                      F.when(F.col("cancelled"), "CANCELLED").otherwise("OPEN").alias("status"),
                      F.coalesce("captured", F.lit(0)).alias("captured"), F.coalesce("refunded", F.lit(0)).alias("refunded")))

    pay_ref = refs.groupBy("order_id", "payment_id").agg(F.sum("amount").alias("refunded"))
    payments = (applied.where("event_type IN ('PAYMENT_CAPTURED', 'PAYMENT_FAILED')")
                .join(pay_ref, ["order_id", "payment_id"], "left")
                .select("order_id", "payment_id",
                        F.when(F.col("event_type") == "PAYMENT_CAPTURED", "CAPTURED").otherwise("FAILED").alias("status"),
                        "amount", F.coalesce("refunded", F.lit(0)).alias("refunded")))

    money = (applied.where("event_type IN ('PAYMENT_CAPTURED', 'REFUND_ISSUED')")
             .withColumn("day_start", F.col("event_time_ms") - F.col("event_time_ms") % DAY_MS)
             .withColumn("late", F.col("timestamp_ms") > F.col("day_start") + DAY_MS + grace_ms)
             .withColumn("period", F.date_format(F.timestamp_millis("day_start"), "yyyy-MM-dd")))
    is_cap = F.col("event_type") == "PAYMENT_CAPTURED"
    periods = money.groupBy("period", "currency").agg(
        F.sum(F.when(is_cap, F.col("amount")).otherwise(0)).alias("captured"),
        F.sum(F.when(~is_cap, F.col("amount")).otherwise(0)).alias("refunded"),
        F.sum(F.when(is_cap & F.col("late"), F.col("amount")).otherwise(0)).alias("late_captured"),
        F.sum(F.when(~is_cap & F.col("late"), F.col("amount")).otherwise(0)).alias("late_refunded"))

    positions = (invalid.select("partition", "offset", F.lit("INVALID").alias("status"), "reason")
                 .unionByName(repeats.select("partition", "offset", "status",
                                             F.when(F.col("status") == "CONFLICT", "EVENT_ID_CONFLICT")
                                             .cast("string").alias("reason")))
                 .unionByName(decided.select("partition", "offset", "status", "reason")))

    view = {
        "orders": {r.order_id: (r.currency, r.total, r.status, r.captured, r.refunded) for r in orders.collect()},
        "payments": {(r.order_id, r.payment_id): (r.status, r.amount, r.refunded) for r in payments.collect()},
        "refunds": {(r.order_id, r.refund_id): (r.payment_id, r.amount) for r in refs.collect()},
        "periods": {(r.period, r.currency): (r.captured, r.refunded, r.late_captured, r.late_refunded)
                    for r in periods.collect() if r.captured + r.refunded > 0},
        "unresolved": sorted((r.order_id, r.event_id, r.reason) for r in decided.where("status = 'PENDING'").collect()),
        "positions": {(r.partition, r.offset): (r.status, r.reason) for r in positions.collect()},
        "conflicts": sorted(r.event_id for r in repeats.where("status = 'CONFLICT'").collect()),
    }
    decoded.unpersist(); decided.unpersist()
    return view
