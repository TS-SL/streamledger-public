"""Compare persisted state with the reference model at a settled cutoff.

Differences are returned as classified strings; callers treat any difference
as a failure. There are no tolerances: money is compared exactly.
"""
from __future__ import annotations

from streamledger.reference.model import ReferenceState


def reference_view(st: ReferenceState) -> dict:
    return {
        "orders": {k: (o.currency, o.total_minor, o.status, o.captured_minor, o.refunded_minor)
                   for k, o in st.orders.items()},
        "payments": {k: (p.status, p.amount_minor, p.refunded_minor) for k, p in st.payments.items()},
        "refunds": dict(st.refunds),
        "periods": {k: (v.captured_minor, v.refunded_minor, v.late_captured_minor, v.late_refunded_minor)
                    for k, v in st.periods.items() if v.captured_minor + v.refunded_minor > 0},
        "unresolved": st.unresolved(),
        "positions": {k: (o.status, o.reason) for k, o in st.positions.items()},
        "conflicts": sorted(e for e, _, _ in st.conflicts),
    }


def diff(expected: dict, observed: dict, limit: int = 20) -> list[str]:
    out: list[str] = []
    for section in expected.keys() | observed.keys():
        a, b = expected.get(section), observed.get(section)
        if a == b:
            continue
        if isinstance(a, dict) and isinstance(b, dict):
            for k in sorted(a.keys() | b.keys(), key=repr):
                if a.get(k) != b.get(k):
                    kind = "missing" if k not in b else "unexpected" if k not in a else "mismatch"
                    out.append(f"{section}:{kind}:{k}: expected={a.get(k)} observed={b.get(k)}")
        else:
            out.append(f"{section}: expected={a!r:.200} observed={b!r:.200}")
    out.sort()
    return out[:limit] + ([f"... {len(out) - limit} more"] if len(out) > limit else [])


def to_jsonable(view: dict) -> dict:
    """Comparison view -> JSON-safe form (tuple keys become lists)."""
    out = {}
    for section, v in view.items():
        if isinstance(v, dict):
            out[section] = sorted(([list(k) if isinstance(k, tuple) else k, list(val) if isinstance(val, tuple) else val]
                                   for k, val in v.items()), key=repr)
        else:
            out[section] = [list(x) if isinstance(x, tuple) else x for x in v]
    return out


def from_jsonable(obj: dict) -> dict:
    tup = lambda x: tuple(x) if isinstance(x, list) else x
    out = {}
    for section, v in obj.items():
        if section in ("unresolved", "conflicts"):
            out[section] = [tup(x) for x in v]
        else:
            out[section] = {tup(k): tup(val) for k, val in v}
    return out
