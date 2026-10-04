#!/usr/bin/env bash
set -euo pipefail

# Cleanup: delete playground resources (keeps the kind cluster by default).
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

NAMESPACE="${NAMESPACE:-rabbitmq-playground}"
DELETE_CLUSTER="${DELETE_CLUSTER:-false}"
CLUSTER="${CLUSTER:-rabbitmq-k8s}"

kubectl delete -f k8s/50-consumers.yaml --ignore-not-found=true
kubectl delete -f k8s/40-worker.yaml --ignore-not-found=true
kubectl delete -f k8s/30-publisher.yaml --ignore-not-found=true
kubectl delete -f k8s/15-topology.yaml --ignore-not-found=true
kubectl delete -f k8s/10-rabbitmq.yaml --ignore-not-found=true
kubectl delete -f k8s/20-postgres.yaml --ignore-not-found=true

# The namespace holds the RabbitmqCluster; the Cluster Operator finalizes it.
kubectl delete namespace "$NAMESPACE" --ignore-not-found=true --wait=true

# The Cluster Operator, cert-manager and the Messaging Topology Operator are kept
# installed to speed up subsequent runs. Remove them manually if needed:
#   kubectl delete -f https://github.com/rabbitmq/messaging-topology-operator/releases/latest/download/messaging-topology-operator-with-certmanager.yaml
#   kubectl delete -f https://github.com/rabbitmq/cluster-operator/releases/latest/download/cluster-operator.yml

if [ "$DELETE_CLUSTER" = "true" ]; then
  echo "🗑️  Deleting kind cluster '$CLUSTER' ..."
  kind delete cluster --name "$CLUSTER"
fi

echo "✅ Cleanup complete."
