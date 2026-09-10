#!/usr/bin/env bash
# Launch each flow as its own process and wait for all of them.
#
#   bash run_parallel.sh local fx06_decorators.py fx07_exception.py ...
#   bash run_parallel.sh argo  fx01_basic.py ...
#
# The fast-bakery / micromamba wheel cache is global, and racing it from a
# cold start corrupts it -- so warm it with one sequential run first, then
# fan out.

set -uo pipefail
MODE="${1:-local}"; shift
export AWS_PROFILE="${AWS_PROFILE:-AWSAdministratorAccess-209479263910}"
NO_TAG_FLOWS="fx10_sandbox.py"

pids=(); names=()
for flow in "$@"; do
  tag="--tag ds.domain:forecasting"
  case " $NO_TAG_FLOWS " in *" $flow "*) tag="";; esac
  log="/tmp/rsmatrix-$MODE-${flow%.py}.log"
  if [ "$MODE" = "local" ]; then
    # shellcheck disable=SC2086
    ( uv run python "$flow" --environment=fast-bakery run $tag >"$log" 2>&1 ) &
  else
    # shellcheck disable=SC2086
    ( uv run python "$flow" --environment=fast-bakery argo-workflows create $tag >"$log" 2>&1 \
      && uv run python "$flow" --environment=fast-bakery argo-workflows trigger >>"$log" 2>&1 ) &
  fi
  pids+=($!); names+=("$flow")
  echo "launched $flow (pid $!)  -> $log"
done

pass=(); fail=()
for i in "${!pids[@]}"; do
  if wait "${pids[$i]}"; then pass+=("${names[$i]}"); else fail+=("${names[$i]}"); fi
done

echo ""
echo "=================== $MODE parallel summary ==================="
echo "passed (${#pass[@]}): ${pass[*]:-none}"
echo "failed (${#fail[@]}): ${fail[*]:-none}"
[ "${#fail[@]}" -eq 0 ]
