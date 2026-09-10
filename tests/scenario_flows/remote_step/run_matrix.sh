#!/usr/bin/env bash
# Run the scenario flows through both orchestrators and summarise.
#
#   bash run_matrix.sh local          # metaflow `run`, driver in-process
#   bash run_matrix.sh argo           # deploy + trigger, driver on an Argo pod
#   bash run_matrix.sh local fx01_basic.py fx02_foreach.py
#
# Argo runs are triggered and then polled, because `argo-workflows trigger`
# returns as soon as the workflow is accepted.

set -uo pipefail
MODE="${1:-local}"; shift || true
export AWS_PROFILE="${AWS_PROFILE:-AWSAdministratorAccess-209479263910}"
TAG="--tag ds.domain:forecasting"

# fx10 deliberately carries no team tag: it proves the sandbox fallback.
NO_TAG_FLOWS="fx10_sandbox.py"
# `content` is the only ClusterQueue with GPU quota -- every other queue,
# sandbox and forecasting included, has nvidia.com/gpu nominalQuota 0 AND
# borrowingLimit 0, so a GPU Workload sent there is never admitted. It does
# not fail either: it sits pending forever behind
#   queued -- Kueue has not admitted this Workload yet (team ClusterQueue at quota)
# Any GPU flow therefore has to name a queue that actually has GPUs.
GPU_FLOWS="fx13_gpu.py"
GPU_TAG="--tag ds.domain:content"

if [ "$#" -gt 0 ]; then FLOWS=("$@"); else FLOWS=(fx*.py); fi

pass=(); fail=()
for flow in "${FLOWS[@]}"; do
  tag="$TAG"
  case " $NO_TAG_FLOWS " in *" $flow "*) tag="";; esac
  case " $GPU_FLOWS " in *" $flow "*) tag="$GPU_TAG";; esac
  echo ""
  echo "======================================================================"
  echo "  $MODE  $flow  ${tag:-(no team tag)}"
  echo "======================================================================"
  log="/tmp/rsmatrix-$MODE-${flow%.py}.log"
  if [ "$MODE" = "local" ]; then
    # shellcheck disable=SC2086
    uv run python "$flow" --environment=fast-bakery run $tag >"$log" 2>&1
    rc=$?
  else
    # shellcheck disable=SC2086
    uv run python "$flow" --environment=fast-bakery argo-workflows create $tag >"$log" 2>&1 \
      && uv run python "$flow" --environment=fast-bakery argo-workflows trigger >>"$log" 2>&1
    rc=$?
  fi
  if [ $rc -eq 0 ]; then
    echo "  submitted/finished OK  (log: $log)"
    grep -E "^\[(check|fx)" "$log" | tail -20
    pass+=("$flow")
  else
    echo "  FAILED rc=$rc  (log: $log)"
    grep -E "FAIL|Error|Exception|Traceback|Step failure" "$log" | head -15
    fail+=("$flow")
  fi
done

echo ""
echo "=========================== $MODE summary ==========================="
echo "passed (${#pass[@]}): ${pass[*]:-none}"
echo "failed (${#fail[@]}): ${fail[*]:-none}"
[ "${#fail[@]}" -eq 0 ]
