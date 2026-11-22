"""Deterministic fault hooks at named transaction boundaries (tests only).

Production code calls `faults.hit(point)`; with no rules configured that is a
dictionary lookup and nothing else. A rule fires on the Nth hit of a point
(optionally on every hit from then on) and performs one action:

    raise      raise FaultInjected (in-process crash; transaction rolls back)
    transient  raise psycopg SerializationFailure (exercises the retry path)
    exit       os._exit(137): process dies without cleanup, like SIGKILL
    <callable> call it (tests use this to pause a transaction)

Spec string (env STREAMLEDGER_FAULTS or --faults):
    "before_commit@37:exit"  "after_lock@5:transient*"   (comma-separated)

Points, in the order one record passes them:
    before_begin    record consumed, nothing started
    after_lock      progress row locked, inside the DB transaction
    before_commit   all effects written, COMMIT not yet issued
    after_commit    DB committed, Kafka offset not yet committed
    after_kafka_commit  Kafka commit for the batch returned
"""
from __future__ import annotations

import os
from collections import Counter
from dataclasses import dataclass
from typing import Callable, Union

POINTS = ("before_begin", "after_lock", "before_commit", "after_commit", "after_kafka_commit")
Action = Union[str, Callable[[], None]]


class FaultInjected(RuntimeError):
    """Raised by a configured fault hook."""


@dataclass(frozen=True)
class Rule:
    point: str
    at: int = 1
    action: Action = "raise"
    repeat: bool = False

    def __post_init__(self):
        if self.point not in POINTS:
            raise ValueError(f"unknown fault point {self.point!r}")
        if isinstance(self.action, str) and self.action not in ("raise", "transient", "exit"):
            raise ValueError(f"unknown fault action {self.action!r}")


class Faults:
    def __init__(self, rules: list[Rule] | dict[str, int] | None = None):
        if isinstance(rules, dict):  # shorthand {point: nth} -> raise
            rules = [Rule(p, n) for p, n in rules.items()]
        self.rules = list(rules or [])
        self.hits: Counter = Counter()
        self.fired: list[tuple[str, int]] = []

    @classmethod
    def parse(cls, spec: str | None) -> "Faults":
        rules = []
        for part in filter(None, (s.strip() for s in (spec or "").split(","))):
            where, _, action = part.partition(":")
            point, _, at = where.partition("@")
            repeat = action.endswith("*")
            rules.append(Rule(point, int(at or 1), action.rstrip("*") or "raise", repeat))
        return cls(rules)

    def hit(self, point: str) -> None:
        if not self.rules:
            return
        self.hits[point] += 1
        n = self.hits[point]
        for r in self.rules:
            if r.point == point and (n == r.at or (r.repeat and n > r.at)):
                self.fired.append((point, n))
                self._do(r.action, point, n)

    @staticmethod
    def _do(action: Action, point: str, n: int) -> None:
        if callable(action):
            action()
        elif action == "raise":
            raise FaultInjected(f"{point}#{n}")
        elif action == "transient":
            from psycopg import errors

            raise errors.SerializationFailure(f"injected transient fault at {point}#{n}")
        elif action == "exit":
            os._exit(137)
