#!/usr/bin/env bash
# Local runner smoke-test — validate entrypoint's install path against a
# fake spec.json BEFORE pushing to ECR. Skips the AWS SubmitJob loop.
#
# Usage:
#   bash container/local_smoke.sh
#
# Env vars:
#   GITHUB_TOKEN     for cloning private git deps (falls back to `gh auth token`)
#   SPEC_JSON        override the sample spec (default: minimal)

set -euo pipefail


REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_JSON="$REPO_ROOT/src/metaflow_extensions/remote_step/config.json"
IMAGE_URI="$(python3 -c "import json; print(json.load(open('$ENV_JSON'))['runner_image'])")"
IMAGE_URI="${IMAGE_URI%:*}:latest"

GH_TOKEN="${GITHUB_TOKEN:-$(gh auth token 2>/dev/null || true)}"

# Minimal spec — one small package + one private git dep to exercise every
# code path (version parsing, netrc, uv resolve).
DEFAULT_SPEC='{
  "version": 1,
  "flow_module": "smoke_flow",
  "flow_class": "SmokeFlow",
  "step_name": "train",
  "flow_name": "SmokeFlow",
  "run_id": "smoke",
  "task_id": "t1",
  "attempt": 0,
  "code_package_url": "",
  "code_package_sha": "",
  "datastore_root": "",
  "env": {
    "python": "3.12.13",
    "packages": {
      "ds-platform-utils": "@ git+https://github.com/patterninc/ds-platform-utils.git@remote-step",
      "requests": "2.34.2"
    }
  },
  "inputs": {},
  "output_bucket": "unused",
  "output_prefix": "unused",
  "mfconfig": {}
}'

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
echo "${SPEC_JSON:-$DEFAULT_SPEC}" > "$TMP_DIR/spec.json"

echo "smoke: running $IMAGE_URI against $TMP_DIR/spec.json"
docker run --rm \
    --platform linux/amd64 \
    -e GITHUB_TOKEN="$GH_TOKEN" \
    -e REMOTE_STEP_SPEC_URI=/payload/spec.json \
    -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_REGION=us-west-2 \
    -v "$TMP_DIR:/payload" \
    --entrypoint /bin/bash \
    "$IMAGE_URI" -c '
set -e
mkdir -p /workspace
# Skip the S3 fetch stages; jump straight to the uv install path.
PY=/venv-runner/bin/python
if [ -n "${GITHUB_TOKEN:-}" ]; then
    printf "machine github.com login x-oauth-basic password %s\n" "$GITHUB_TOKEN" > ~/.netrc
    chmod 600 ~/.netrc
fi
PY_VERSION=$($PY -c "import json; print(json.load(open(\"/payload/spec.json\"))[\"env\"][\"python\"])")
/root/.local/bin/uv venv --python "$PY_VERSION" /venv
PKG_SPECS=()
while IFS= read -r -d "" spec_line; do
    PKG_SPECS+=("$spec_line")
done < <($PY -c "
import json, sys
spec = json.load(open(\"/payload/spec.json\"))
out = []
for name, ver in spec[\"env\"][\"packages\"].items():
    ver = (ver or \"\").strip()
    if ver.startswith(\"@\"):
        out.append(f\"{name} {ver}\")
    elif ver.startswith((\"git+\", \"http://\", \"https://\", \"file://\")):
        out.append(f\"{name} @ {ver}\")
    elif name.startswith((\"git+\", \"http://\", \"https://\", \"file://\")):
        out.append(name if not ver else f\"{name}{ver}\")
    else:
        out.append(f\"{name}=={ver}\" if ver else name)
sys.stdout.buffer.write((chr(0).join(out) + chr(0)).encode())
")
echo "smoke: package specs to install:"
for s in "${PKG_SPECS[@]}"; do echo "  [$s]"; done
/root/.local/bin/uv pip install --python /venv/bin/python \
    --index-strategy unsafe-best-match \
    boto3 "${PKG_SPECS[@]}"
echo "smoke: install OK"
/venv/bin/python -c "
import metaflow_extensions.remote_step
from remote_step.sizing import resolve
print(\"smoke: import OK, resolve:\", resolve(1, 2048).queue)
"
'
