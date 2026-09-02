#!/usr/bin/env bash
# remote-step runner entrypoint. Runs inside the AWS Batch container.
#
# Life:
#   1. Fetch spec.json from S3.
#   2. Materialize a Python venv from spec.env (python + packages).
#   3. Fetch Metaflow code-package into /workspace.
#   4. exec python -m remote_step.runner_entry <spec.json>
#
# Every stage emits a marker to stderr so the driver-side poller can attribute
# failures. Exit codes match runner_entry conventions.

set -uo pipefail

trap 'echo "[remote_step] STAGE=entrypoint ERR unexpected exit at line $LINENO" >&2' ERR

log() { echo "[remote_step] $*" >&2; }
stage() { echo "[remote_step] STAGE=$1 $2 ${3:-}" >&2; }

: "${REMOTE_STEP_SPEC_URI:?REMOTE_STEP_SPEC_URI must be set}"

mkdir -p /payload /workspace

# 1. Fetch spec.
t=$(date +%s)
if ! /venv-runner/bin/python -c "
import boto3, sys, urllib.parse, os
u = urllib.parse.urlparse(os.environ['REMOTE_STEP_SPEC_URI'])
s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION'))
s3.download_file(u.netloc, u.path.lstrip('/'), '/payload/spec.json')
"; then
    stage fetch_payload ERR
    exit 3
fi
stage fetch_payload OK "$(( $(date +%s) - t ))s"

# Extract fields from spec via python (jq isn't always installed).
PY=/venv-runner/bin/python
PY_VERSION=$($PY -c "import json; print(json.load(open('/payload/spec.json'))['env']['python'])")
CODE_URL=$($PY -c "import json; print(json.load(open('/payload/spec.json'))['code_package_url'])")
CODE_SHA=$($PY -c "import json; print(json.load(open('/payload/spec.json'))['code_package_sha'])")

# Load METAFLOW_* env from mfconfig so downstream boto3/metaflow calls work.
eval "$($PY -c "
import json, shlex
spec = json.load(open('/payload/spec.json'))
for k, v in (spec.get('mfconfig') or {}).items():
    print(f'export {k}={shlex.quote(str(v))}')
")"

# 2. Build user venv.
t=$(date +%s)
if ! /root/.local/bin/uv venv --python "$PY_VERSION" /venv; then
    stage uv_venv ERR
    exit 4
fi
stage uv_venv OK "$(( $(date +%s) - t ))s"

PACKAGES=$($PY -c "
import json, sys
spec = json.load(open('/payload/spec.json'))
packages = spec['env'].get('packages', {})
sys.stderr.write(f'[remote_step] spec env packages ({len(packages)}): {list(packages.items())[:20]}\n')
out = []
for name, ver in packages.items():
    ver = (ver or '').strip()
    if ver.startswith(('git+', 'http://', 'https://', 'file://')):
        out.append(f'{name} @ {ver}')
        continue
    if name.startswith(('git+', 'http://', 'https://', 'file://')):
        out.append(name if not ver else f'{name}{ver}')
        continue
    out.append(f'{name}=={ver}' if ver else name)
print('\n'.join(out))
")

t=$(date +%s)
if ! /root/.local/bin/uv pip install --python /venv/bin/python \
    --index-strategy unsafe-best-match \
    boto3 \
    ${PACKAGES:+$PACKAGES}; then
    stage uv_pip_install ERR
    exit 4
fi
stage uv_pip_install OK "$(( $(date +%s) - t ))s"

# 3. Fetch Metaflow code package.
t=$(date +%s)
if [ -n "$CODE_URL" ]; then
    if ! $PY -c "
import boto3, sys, urllib.parse, os, tarfile
u = urllib.parse.urlparse('$CODE_URL')
s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION'))
s3.download_file(u.netloc, u.path.lstrip('/'), '/payload/code.tgz')
with tarfile.open('/payload/code.tgz') as t:
    t.extractall('/workspace')
"; then
        stage fetch_code_pkg ERR
        exit 5
    fi
fi
stage fetch_code_pkg OK "$(( $(date +%s) - t ))s"

# 4. Exec runner_entry.
exec /venv/bin/python -m remote_step.runner_entry /payload/spec.json
