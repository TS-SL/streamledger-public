# Methodology

## How correctness is established

1. **Write the contract first.** `event-contract.md` fixes every event's financial effect and every reason code. `consistency-contract.md` fixes the fault model and the settled state.
2. **Build an oracle from the contract alone.** `reference/model.py` never imports processor code.
3. **Compare exactly.** At a settled cutoff, the comparison covers every order, payment, refund, period total, unresolved dependency, conflict, and the final status and reason of every log position. Money has no tolerance.
4. **Build a second, structurally different rebuild.** The PySpark job decides admission by ready offset instead of replaying a queue.
5. **Attack the tests (mutation checks).** `scripts/mutation_check.sh` applies one deliberate bug at a time, runs a selection, and restores the file. The unmutated baseline must pass first. A mutant counts as killed only if pytest ran tests and some failed (exit 1); usage errors count as INVALID. Survivors are either fixed by a new test or argued equivalent in [verification-history.md](verification-history.md).
6. **Inject faults deterministically.** Named hooks (`faults.py`) fire on the Nth hit of a transaction boundary and raise, simulate a transient DB error, `os._exit(137)`, or call a test function. Container kills (`docker compose kill -s SIGKILL`) are opt-in.

## Evidence labels

VERIFIED = a test or command observed it. MEASURED = a number from a run; n is stated.
FAILED = observed not to work. BLOCKED = could not be attempted. ESTIMATED = reasoned,
not measured. NOT TESTED = not attempted.

## Measurements

`benchmarks/run_benchmarks.py` uses fixed seeds and order counts, a fresh topic and a
fresh database per experiment, and writes raw JSON (including lag timelines) to
`results/raw/` (git-ignored, because it contains the wall-clock start time). Published copies live in
`results/benchmarks/` with the start time redacted. Host, versions and container limits are recorded in each file. These are
single-laptop numbers with one broker and one database. They describe this setup and
say nothing about scalability.

## Resource discipline

The host had ~0.5 GB of free RAM. Services have memory limits (Kafka 1 GiB, PostgreSQL
512 MiB). Spark runs with `local[2]` and a 1 GiB driver. The documented reconciliation
flow runs Spark after the stack is stopped, using a saved DB snapshot. The stack is
stopped whenever work ends.
