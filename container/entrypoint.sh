#!/usr/bin/env bash
# remote-step runner entrypoint. Runs inside the Kubernetes runner pod.
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

# Wire GITHUB_TOKEN (forwarded by the driver via containerOverrides.env)
# into a netrc so `uv pip install git+https://github.com/...` can clone
# private repos non-interactively.
if [ -n "${GITHUB_TOKEN:-}" ]; then
    printf 'machine github.com login x-oauth-basic password %s\n' "$GITHUB_TOKEN" > ~/.netrc
    chmod 600 ~/.netrc
fi

# Emit each package spec as one \0-terminated record so we can read them
# into a bash array without word-splitting on spaces inside PEP 508 URLs
# (e.g. "ds-dqv-tool @ git+https://...").
PKG_SPECS=()
while IFS= read -r -d '' spec_line; do
    PKG_SPECS+=("$spec_line")
done < <($PY -c "
import json, sys
spec = json.load(open('/payload/spec.json'))
packages = spec['env'].get('packages', {})
sys.stderr.write(f'[remote_step] spec env packages ({len(packages)}): {list(packages.items())[:20]}\n')
out = []
for name, ver in packages.items():
    ver = (ver or '').strip()
    if ver.startswith('@'):
        out.append(f'{name} {ver}')
    elif ver.startswith(('git+', 'http://', 'https://', 'file://')):
        out.append(f'{name} @ {ver}')
    elif name.startswith(('git+', 'http://', 'https://', 'file://')):
        out.append(name if not ver else f'{name}{ver}')
    else:
        out.append(f'{name}=={ver}' if ver else name)
if out:
    # Only terminate when there is something to terminate. Writing the
    # trailing NUL unconditionally emits one empty record for an empty
    # package set, which lands in PKG_SPECS as \"\" and makes uv fail with
    #   error: Failed to parse: \`\`
    #   Caused by: Empty field is not allowed for PEP508
    # A step that needs only the standard library is legitimate, so an
    # empty set has to mean 'install nothing', not 'install \"\"'.
    sys.stdout.buffer.write(('\0'.join(out) + '\0').encode())
")

t=$(date +%s)
if ! /root/.local/bin/uv pip install --python /venv/bin/python \
    --index-strategy unsafe-best-match \
    boto3 \
    "${PKG_SPECS[@]}"; then
    stage uv_pip_install ERR
    exit 4
fi
stage uv_pip_install OK "$(( $(date +%s) - t ))s"

# The runner itself has to be importable by the job venv's interpreter.
#
# The image installs ds-platform-utils into /venv-runner, but the job venv
# created above is isolated, so `/venv/bin/python -m
# metaflow_extensions.remote_step.runner_entry` cannot see it:
#   ModuleNotFoundError: No module named 'metaflow_extensions'
#
# --no-deps is deliberate and load-bearing. ds-platform-utils depends on
# outerbounds, pandas, polars, pyarrow, snowflake-connector and kubernetes;
# resolving those here would both cost minutes and fight the flow's own
# pinned versions. runner_entry needs only boto3/botocore (installed above)
# plus the standard library — metaflow is imported under try/except and comes
# from the flow's own packages when present.
t=$(date +%s)
if ! /root/.local/bin/uv pip install --python /venv/bin/python \
    --no-deps /ds-platform-utils; then
    stage install_runner ERR
    exit 4
fi
stage install_runner OK "$(( $(date +%s) - t ))s"

# 3. Fetch Metaflow code package.
t=$(date +%s)
if [ -n "$CODE_URL" ]; then
    if ! $PY -c "
import boto3, sys, urllib.parse, os, tarfile
u = urllib.parse.urlparse('$CODE_URL')
s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION'))
s3.download_file(u.netloc, u.path.lstrip('/'), '/payload/code.tgz')
with tarfile.open('/payload/code.tgz') as tf:
    # filter='data' silences the 3.12+ DeprecationWarning and, more to the
    # point, is the behaviour Python 3.14 makes the default. It refuses
    # absolute paths, '..' escapes and links pointing outside /workspace, and
    # drops owner/group and special files — so a malformed or hostile archive
    # cannot write outside the extraction directory. Executable bits on
    # regular files survive, which is all this archive needs.
    tf.extractall('/workspace', filter='data')
"; then
        stage fetch_code_pkg ERR
        exit 5
    fi
fi
stage fetch_code_pkg OK "$(( $(date +%s) - t ))s"

# 4. Exec runner_entry.
exec /venv/bin/python -m metaflow_extensions.remote_step.runner_entry /payload/spec.json
