#!/usr/bin/env bash
# Apply each deliberate bug to a source file, run a test selection, restore the file.
# A mutant that leaves the selection green means the tests do not cover that rule.
#
#   scripts/mutation_check.sh <file> "<pytest args>" <sed-expr>...
#
# The unmutated baseline must pass first. A mutant only counts as KILLED when
# pytest actually ran tests and some failed (exit code 1); collection or usage
# errors (exit 2-5) are reported as INVALID, never as kills.
set -uo pipefail
cd "$(dirname "$0")/.."
PY="${STREAMLEDGER_PY:-$HOME/.venvs/streamledger/bin/python}"
file=$1; args=$2; shift 2
log=$(mktemp)
run() { eval "\"$PY\" -m pytest -p no:cacheprovider -x -q -rf $args" >"$log" 2>&1; }
summary() { grep -E '^(FAILED|ERROR) ' "$log" | head -1 | cut -c1-120; grep -aE '(passed|failed)' "$log" | tail -1 | tr -d '='; }

run; base=$?
if [ $base -ne 0 ]; then echo "BASELINE FAILED (exit $base): $(summary | tr '\n' ' ')"; exit 99; fi
echo "BASELINE     $(summary)"

backup=$(mktemp)
cp "$file" "$backup"
trap 'cp "$backup" "$file"; rm -f "$backup" "$log"' EXIT
bad=0
for expr in "$@"; do
  sed -i "$expr" "$file"
  if cmp -s "$file" "$backup"; then echo "NOT APPLIED  $expr"; bad=$((bad+1)); continue; fi
  run; rc=$?
  case $rc in
    0) echo "SURVIVED     $expr"; bad=$((bad+1)) ;;
    1) echo "KILLED       $expr  -> $(summary | tr '\n' ' ')" ;;
    *) echo "INVALID(rc=$rc) $expr  -> $(tail -3 "$log" | tr '\n' ' ')"; bad=$((bad+1)) ;;
  esac
  cp "$backup" "$file"
done
exit $bad
