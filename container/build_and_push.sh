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

echo "building multi-arch (linux/amd64 + linux/arm64) $REPO_URI:$TAG (also tagging :latest)"
aws ecr get-login-password --region "$REGION" \
    | docker login --username AWS --password-stdin "$REGISTRY"

# buildx builder — reused across runs. Idempotent: create if missing.
if ! docker buildx inspect remote-step-builder >/dev/null 2>&1; then
    docker buildx create --name remote-step-builder --use
else
    docker buildx use remote-step-builder
fi

# `--push` uploads the multi-arch manifest list in one shot. AWS
# Batch/Fargate picks the right variant based on the job def's
# runtimePlatform.cpuArchitecture (X86_64 or ARM64).
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    -t "$REPO_URI:$TAG" \
    -t "$REPO_URI:latest" \
    -f "$REPO_ROOT/container/Dockerfile" \
    --push \
    "$REPO_ROOT"

echo ""
echo "pushed multi-arch  $REPO_URI:$TAG"
echo "                    $REPO_URI:latest"
