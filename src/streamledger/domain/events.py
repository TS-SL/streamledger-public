"""Commerce event wire contract (schema v1): parsing, validation, canonical form.

This module is shared by every component that reads events. It contains no
business rules about financial state; those live in the processors.
See docs/event-contract.md.
"""
from __future__ import annotations

import calendar
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

SUPPORTED_SCHEMA_VERSIONS = frozenset({1})
SUPPORTED_CURRENCIES = frozenset({"USD", "EUR", "GBP"})
MAX_AMOUNT_MINOR = 10**12  # fits comfortably in BIGINT and IEEE-754 doubles
MAX_RECORD_BYTES = 16_384

_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_TIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


class EventType(str, Enum):
    ORDER_CREATED = "ORDER_CREATED"
    PAYMENT_CAPTURED = "PAYMENT_CAPTURED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    REFUND_ISSUED = "REFUND_ISSUED"
    ORDER_CANCELLED = "ORDER_CANCELLED"


FIELDS = (
    "event_id",
    "event_type",
    "schema_version",
    "order_id",
    "payment_id",
    "refund_id",
    "event_time",
    "producer_id",
    "producer_seq",
    "currency",
    "amount_minor",
)

# (payment_id required, refund_id required, amount rule)
_TYPE_RULES = {
    EventType.ORDER_CREATED: (False, False, "positive"),
    EventType.PAYMENT_CAPTURED: (True, False, "positive"),
    EventType.PAYMENT_FAILED: (True, False, "positive"),
    EventType.REFUND_ISSUED: (True, True, "positive"),
    EventType.ORDER_CANCELLED: (False, False, "zero"),
}


class InvalidEvent(Exception):
    """A record that cannot be interpreted under the contract.

    `reason` is a stable machine-readable code; `event_id` is filled in when
    it could be extracted, for diagnostics only.
    """

    def __init__(self, reason: str, detail: str = "", event_id: str | None = None):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail
        self.event_id = event_id


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: EventType
    schema_version: int
    order_id: str
    payment_id: str | None
    refund_id: str | None
    event_time: str
    producer_id: str
    producer_seq: int
    currency: str
    amount_minor: int

    @property
    def event_time_ms(self) -> int:
        return parse_event_time_ms(self.event_time)

    def to_wire(self) -> dict:
        d = {f: getattr(self, f) for f in FIELDS}
        d["event_type"] = self.event_type.value
        return d

    def canonical_bytes(self) -> bytes:
        return canonical_json(self.to_wire())

    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


def canonical_json(obj: dict) -> bytes:
    """Deterministic serialization: sorted keys, no whitespace, UTF-8."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def parse_event_time_ms(value: str) -> int:
    if not isinstance(value, str) or not _TIME_RE.match(value):
        raise ValueError(f"event_time must be YYYY-MM-DDTHH:MM:SS.mmmZ, got {value!r}")
    dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    return calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond // 1000


def format_event_time(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"


def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _check_id(obj: dict, name: str, required: bool, eid: str | None) -> None:
    v = obj.get(name)
    if required:
        if not isinstance(v, str) or not _ID_RE.match(v):
            raise InvalidEvent("BAD_FIELD", f"{name} must match {_ID_RE.pattern}", eid)
    elif v is not None:
        raise InvalidEvent("BAD_FIELD", f"{name} must be null for this event type", eid)


def parse_event(raw: bytes, key: bytes | None = None) -> Event:
    """Decode and validate one broker record value. Raises InvalidEvent."""
    if raw is None:
        raise InvalidEvent("EMPTY_RECORD")
    if len(raw) > MAX_RECORD_BYTES:
        raise InvalidEvent("RECORD_TOO_LARGE", f"{len(raw)} bytes")
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidEvent("UNDECODABLE", type(exc).__name__) from None
    if not isinstance(obj, dict):
        raise InvalidEvent("NOT_AN_OBJECT")

    eid = obj.get("event_id") if isinstance(obj.get("event_id"), str) else None
    version = obj.get("schema_version")
    if not _is_int(version):
        raise InvalidEvent("BAD_FIELD", "schema_version must be an integer", eid)
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        raise InvalidEvent("UNSUPPORTED_SCHEMA_VERSION", str(version), eid)

    keys = set(obj)
    missing = set(FIELDS) - keys
    extra = keys - set(FIELDS)
    if missing:
        raise InvalidEvent("MISSING_FIELD", ",".join(sorted(missing)), eid)
    if extra:
        raise InvalidEvent("UNKNOWN_FIELD", ",".join(sorted(extra)), eid)

    _check_id(obj, "event_id", True, eid)
    try:
        etype = EventType(obj["event_type"])
    except (ValueError, TypeError):
        raise InvalidEvent("UNKNOWN_EVENT_TYPE", repr(obj["event_type"]), eid) from None
    need_payment, need_refund, amount_rule = _TYPE_RULES[etype]
    _check_id(obj, "order_id", True, eid)
    _check_id(obj, "payment_id", need_payment, eid)
    _check_id(obj, "refund_id", need_refund, eid)
    _check_id(obj, "producer_id", True, eid)

    try:
        parse_event_time_ms(obj["event_time"])
    except ValueError as exc:
        raise InvalidEvent("BAD_EVENT_TIME", str(exc), eid) from None
    if not _is_int(obj["producer_seq"]) or obj["producer_seq"] < 0:
        raise InvalidEvent("BAD_FIELD", "producer_seq must be a non-negative integer", eid)
    if obj["currency"] not in SUPPORTED_CURRENCIES:
        raise InvalidEvent("UNSUPPORTED_CURRENCY", repr(obj["currency"]), eid)

    amount = obj["amount_minor"]
    if not _is_int(amount):
        raise InvalidEvent("BAD_AMOUNT", "amount_minor must be an integer (minor units)", eid)
    if amount_rule == "positive" and not (0 < amount <= MAX_AMOUNT_MINOR):
        raise InvalidEvent("BAD_AMOUNT", f"must be in 1..{MAX_AMOUNT_MINOR}, got {amount}", eid)
    if amount_rule == "zero" and amount != 0:
        raise InvalidEvent("BAD_AMOUNT", f"must be 0 for {etype.value}, got {amount}", eid)

    if key is not None and key != obj["order_id"].encode("utf-8"):
        raise InvalidEvent("KEY_MISMATCH", "record key must equal order_id", eid)

    return Event(
        event_id=obj["event_id"],
        event_type=etype,
        schema_version=version,
        order_id=obj["order_id"],
        payment_id=obj["payment_id"],
        refund_id=obj["refund_id"],
        event_time=obj["event_time"],
        producer_id=obj["producer_id"],
        producer_seq=obj["producer_seq"],
        currency=obj["currency"],
        amount_minor=amount,
    )


@dataclass(frozen=True)
class LogRecord:
    """One broker record as observed by a consumer or read from the archive."""

    topic: str
    partition: int
    offset: int
    timestamp_ms: int  # record timestamp as stored in the log (topic uses CreateTime)
    key: bytes | None
    value: bytes | None
