import json

import pytest

from streamledger.domain.events import InvalidEvent, canonical_json, format_event_time, parse_event, parse_event_time_ms

BASE = {
    "event_id": "evt_1",
    "event_type": "PAYMENT_CAPTURED",
    "schema_version": 1,
    "order_id": "ord_1",
    "payment_id": "pay_1",
    "refund_id": None,
    "event_time": "2026-01-01T10:00:00.250Z",
    "producer_id": "payments-svc",
    "producer_seq": 3,
    "currency": "USD",
    "amount_minor": 1999,
}


def enc(d):
    return json.dumps(d).encode()


def reason(d, key=None):
    with pytest.raises(InvalidEvent) as exc:
        parse_event(enc(d) if isinstance(d, dict) else d, key)
    return exc.value.reason


def test_valid_event_roundtrip_and_canonical_digest():
    a = parse_event(enc(BASE), b"ord_1")
    reordered = json.dumps(dict(reversed(list(BASE.items()))), indent=2).encode()
    b = parse_event(reordered, b"ord_1")
    assert a == b and a.digest() == b.digest()
    assert a.canonical_bytes() == canonical_json(BASE)
    assert parse_event(enc(dict(BASE, amount_minor=2000))).digest() != a.digest()


def test_event_time_parsing_is_exact():
    assert parse_event_time_ms("1970-01-01T00:00:01.001Z") == 1001
    assert format_event_time(1001) == "1970-01-01T00:00:01.001Z"
    with pytest.raises(ValueError):
        parse_event_time_ms("2026-02-30T00:00:00.000Z")


@pytest.mark.parametrize(
    "change, expected",
    [
        ({"amount_minor": -5}, "BAD_AMOUNT"),
        ({"amount_minor": 0}, "BAD_AMOUNT"),
        ({"amount_minor": 19.99}, "BAD_AMOUNT"),
        ({"amount_minor": "1999"}, "BAD_AMOUNT"),
        ({"amount_minor": True}, "BAD_AMOUNT"),
        ({"amount_minor": 10**12 + 1}, "BAD_AMOUNT"),
        ({"schema_version": 2}, "UNSUPPORTED_SCHEMA_VERSION"),
        ({"schema_version": "1"}, "BAD_FIELD"),
        ({"event_type": "PAYMENT_REVERSED"}, "UNKNOWN_EVENT_TYPE"),
        ({"currency": "JPY"}, "UNSUPPORTED_CURRENCY"),
        ({"event_time": "2026-01-01 10:00:00"}, "BAD_EVENT_TIME"),
        ({"payment_id": None}, "BAD_FIELD"),
        ({"refund_id": "ref_1"}, "BAD_FIELD"),
        ({"order_id": "bad id with spaces"}, "BAD_FIELD"),
        ({"extra": 1}, "UNKNOWN_FIELD"),
        ({"event_type": "ORDER_CANCELLED", "payment_id": None, "amount_minor": 5}, "BAD_AMOUNT"),
    ],
)
def test_invalid_fields(change, expected):
    assert reason(dict(BASE, **change)) == expected


def test_missing_field_and_undecodable_and_key_mismatch():
    d = dict(BASE)
    del d["currency"]
    assert reason(d) == "MISSING_FIELD"
    assert reason(b"\x00not-json\xff") == "UNDECODABLE"
    assert reason(b"[1,2]") == "NOT_AN_OBJECT"
    assert reason(BASE, key=b"ord_2") == "KEY_MISMATCH"


def test_unsupported_version_still_reports_event_id_for_diagnostics():
    with pytest.raises(InvalidEvent) as exc:
        parse_event(enc(dict(BASE, schema_version=9)))
    assert exc.value.event_id == "evt_1"
