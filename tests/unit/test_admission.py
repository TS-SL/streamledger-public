"""The batch job's ready-offset admission (analytics/admission.py) against the reference queue model."""
from collections import defaultdict

import pytest

from streamledger.analytics.admission import decide
from streamledger.domain.events import InvalidEvent, parse_event
from streamledger.domain.generator import GeneratorConfig, generate, simulate_log
from streamledger.reference.model import evaluate, merge_order


def first_occurrence_decisions(records):
    """Dedup like the batch job (first by canonical order), then decide per order."""
    first, by_order = {}, defaultdict(list)
    for r in merge_order(records):
        try:
            ev = parse_event(r.value, r.key)
        except InvalidEvent:
            continue
        if ev.event_id in first:
            continue
        first[ev.event_id] = (r.partition, r.offset)
        by_order[ev.order_id].append({"offset": r.offset, "event_id": ev.event_id, "event_type": ev.event_type.value,
                                      "payment_id": ev.payment_id, "refund_id": ev.refund_id,
                                      "currency": ev.currency, "amount": ev.amount_minor})
    out = {}
    for evs in by_order.values():
        for eid, d in decide(evs).items():
            out[first[eid]] = d
    return out


def reference_decisions(records):
    st = evaluate(records)
    return {k: (o.status, o.reason) for k, o in st.positions.items()
            if o.status in ("APPLIED", "PENDING", "REJECTED")}


@pytest.mark.parametrize("seed", [1, 2, 3, 11, 21, 99])
def test_matches_reference_on_generated_workloads(seed):
    cfg = GeneratorConfig(seed=seed, orders=300, duplicate_rate=0.1, conflict_rate=0.03, invalid_rate=0.03)
    records = simulate_log(generate(cfg), partitions=3)
    assert first_occurrence_decisions(records) == reference_decisions(records)


def test_matches_reference_on_hand_built_rule_scenarios():
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "integration"))
    from test_rule_parity import as_log, scenarios  # pure data builders; no database needed

    for name, items in scenarios():
        records = as_log(items)
        assert first_occurrence_decisions(records) == reference_decisions(records), name
