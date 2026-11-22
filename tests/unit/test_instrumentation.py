"""Transaction timing must stay bounded in long-running mode and exact in bounded benchmark mode."""
import tracemalloc

import pytest

from streamledger.instrumentation import TxnTimings


def test_exact_while_under_limit():
    t = TxnTimings(limit=100)
    for i in range(100):
        t.record(i / 1000)
    assert t.exact and t.count == 100 and len(t) == 100
    assert sorted(t.samples()) == [i / 1000 for i in range(100)]
    assert t.max == pytest.approx(0.099) and t.total == pytest.approx(sum(i / 1000 for i in range(100)))


def test_retained_samples_never_exceed_limit():
    t = TxnTimings(limit=50)
    for i in range(10_000):
        t.record(float(i))
    assert len(t) == 50 and t.count == 10_000 and not t.exact
    assert t.max == 9_999.0 and t.total == float(sum(range(10_000)))  # aggregates cover every record


def test_reservoir_is_roughly_uniform():
    # a reservoir that kept only the first `limit` samples would have mean ~limit/2, not ~n/2
    t = TxnTimings(limit=2_000, seed=1)
    n = 200_000
    for i in range(n):
        t.record(float(i))
    mean = sum(t.samples()) / len(t)
    assert abs(mean - n / 2) < 0.05 * n


def test_limit_must_be_positive():
    with pytest.raises(ValueError):
        TxnTimings(limit=0)


def test_sustained_run_memory_is_flat():
    """Two million records: memory after the first 200k and after all 2M must be about the same."""
    t = TxnTimings(limit=10_000)
    tracemalloc.start()
    try:
        for i in range(200_000):
            t.record(0.009 + (i % 97) * 1e-5)
        early, _ = tracemalloc.get_traced_memory()
        for i in range(1_800_000):
            t.record(0.009 + (i % 89) * 1e-5)
        late, _ = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert t.count == 2_000_000 and len(t) == 10_000
    assert late - early < 64 * 1024, f"grew {late - early} bytes between 200k and 2M records"
