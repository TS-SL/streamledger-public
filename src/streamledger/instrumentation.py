"""Bounded in-process instrumentation for the consumer loop."""
from __future__ import annotations

import random

DEFAULT_TXN_SAMPLE_LIMIT = 10_000


class TxnTimings:
    """Transaction durations with bounded memory.

    count, total and max cover every transaction. Percentiles come from at most
    `limit` retained samples: all of them while count <= limit (exact), then a
    uniform reservoir sample (Algorithm R, fixed seed). A long-running consumer
    therefore holds O(limit) floats, not one per record. Bounded benchmark runs
    pass a limit above their record count so their percentiles stay exact.
    """

    def __init__(self, limit: int = DEFAULT_TXN_SAMPLE_LIMIT, seed: int = 0):
        if limit < 1:
            raise ValueError("limit must be >= 1")
        self.limit, self.count, self.total, self.max = limit, 0, 0.0, 0.0
        self._samples: list[float] = []
        self._rng = random.Random(seed)

    def record(self, seconds: float) -> None:
        self.count += 1
        self.total += seconds
        self.max = max(self.max, seconds)
        if len(self._samples) < self.limit:
            self._samples.append(seconds)
        else:
            j = self._rng.randrange(self.count)
            if j < self.limit:
                self._samples[j] = seconds

    @property
    def exact(self) -> bool:
        return self.count <= self.limit

    def samples(self) -> list[float]:
        return list(self._samples)

    def __len__(self) -> int:
        return len(self._samples)
