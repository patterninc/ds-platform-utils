#!/usr/bin/env bash
# Build the remote-step runner image and push to ECR.
#
# Usage:
#   bash container/build_and_push.sh [env]     # default env: dev
#
# Uses the environment JSON shipped with the metaflow_extensions.remote_step
# package to find the target ECR repo.

set -euo pipefail

ENV_NAME="${1:-dev}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_JSON="$REPO_ROOT/src/metaflow_extensions/remote_step/environments/${ENV_NAME}.json"

if [ ! -f "$ENV_JSON" ]; then
    echo "no env config at $ENV_JSON" >&2
    echo "run: cd infra && terraform output -json remote_step_config > $ENV_JSON" >&2
    exit 1
fi

IMAGE_URI="$(python3 -c "import json; print(json.load(open('$ENV_JSON'))['runner_image'])")"
REGION="$(python3 -c "import json; print(json.load(open('$ENV_JSON'))['region'])")"

REPO_URI="${IMAGE_URI%:*}"
REGISTRY="${REPO_URI%%/*}"
TAG="$(date -u +%Y%m%d-%H%M%S)"

echo "building $REPO_URI:$TAG (also tagging :latest)"
aws ecr get-login-password --region "$REGION" \
    | docker login --username AWS --password-stdin "$REGISTRY"

docker build \
    --platform linux/amd64 \
    -t "$REPO_URI:$TAG" \
    -t "$REPO_URI:latest" \
    -f "$REPO_ROOT/container/Dockerfile" \
    "$REPO_ROOT"

docker push "$REPO_URI:$TAG"
docker push "$REPO_URI:latest"

echo ""
echo "pushed  $REPO_URI:$TAG"
echo "        $REPO_URI:latest"
