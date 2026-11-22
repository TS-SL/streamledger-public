"""Fail a test run that skipped, collected nothing, or did not execute the required tests.

pytest exits 0 when every test is skipped, so exit status alone cannot show that
the stack tests ran. This reads the JUnit XML and the --collect-only listing:

    python -m pytest --collect-only -q ... > collected.txt
    python -m pytest ... --junitxml=report.xml
    python scripts/check_test_report.py report.xml --collected collected.txt \
        --require tests/integration/test_reconcile_e2e.py::test_three_way_reconciliation_and_tamper_detection

Prints a summary line and the executed node ids; exit 1 on any skip, failure,
error, missing required test, or difference between collected and executed.
"""
from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def node_id(case: ET.Element) -> str:
    # pytest writes classname "tests.integration.test_x" (dots) and name "test_y[param]"
    cls = case.get("classname", "")
    if not cls:  # a module skipped at import: name is the dotted module path
        return case.get("name", "").replace(".", "/") + ".py"
    parts = cls.split(".")
    # a test class appears as the last dotted component starting with an upper-case letter
    klass = parts.pop() if parts and parts[-1][:1].isupper() else None
    path = "/".join(parts) + ".py"
    return "::".join(filter(None, [path, klass, case.get("name")]))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("junit")
    ap.add_argument("--collected", help="output of pytest --collect-only -q")
    ap.add_argument("--require", action="append", default=[], help="node id prefix that must have passed")
    a = ap.parse_args()

    executed: dict[str, str] = {}
    for case in ET.parse(a.junit).getroot().iter("testcase"):
        status = "passed"
        for tag in ("failure", "error", "skipped"):
            if case.find(tag) is not None:
                status = tag
        executed[node_id(case)] = status

    problems = []
    if not executed:
        problems.append("no tests executed")
    for nid, st in sorted(executed.items()):
        print(f"{st:8s} {nid}")
        if st != "passed":
            problems.append(f"{st}: {nid}")
    for req in a.require:
        hits = [n for n, s in executed.items() if n.startswith(req)]
        if not hits:
            problems.append(f"required test not executed: {req}")
        elif any(executed[n] != "passed" for n in hits):
            problems.append(f"required test did not pass: {req}")
    if a.collected:
        collected = {line.strip() for line in Path(a.collected).read_text().splitlines()
                     if "::" in line and not line.startswith(" ")}
        missing = sorted(collected - executed.keys())
        extra = sorted(executed.keys() - collected)
        problems += [f"collected but not executed: {n}" for n in missing]
        problems += [f"executed but not in the collection listing: {n}" for n in extra]
        print(f"collected={len(collected)}", end=" ")

    counts = {s: sum(1 for v in executed.values() if v == s) for s in ("passed", "failure", "error", "skipped")}
    print(f"executed={len(executed)} " + " ".join(f"{k}={v}" for k, v in counts.items()))
    for p in problems:
        print(f"PROBLEM {p}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
