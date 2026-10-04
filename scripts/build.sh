#!/usr/bin/env bash
set -euo pipefail

# Build the three Go applications as local Docker images for a kind cluster.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REGISTRY="${REGISTRY:-ghcr.io/openmind-systems-lab/rabbitmq-k8s-playground}"
TAG="${TAG:-local}"

for app in publisher worker consumer; do
  echo "🔨 Building $app ..."
  docker build -t "${REGISTRY}/${app}:${TAG}" "./services/${app}"
done

echo "✅ Images built: ${REGISTRY}/{publisher,worker,consumer}:${TAG}"
