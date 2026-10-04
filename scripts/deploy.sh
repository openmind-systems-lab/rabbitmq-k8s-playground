#!/usr/bin/env bash
set -euo pipefail

# Deploy the full rabbitmq-k8s-playground PoC on a local kind cluster.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CLUSTER="${CLUSTER:-rabbitmq-k8s}"
NAMESPACE="${NAMESPACE:-rabbitmq-playground}"
REGISTRY="${REGISTRY:-ghcr.io/openmind-systems-lab/rabbitmq-k8s-playground}"
TAG="${TAG:-local}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.2}"

# Poll a custom resource until its Ready condition is True.
wait_ready() {
  local target="$1" timeout="${2:-180}" start
  start=$(date +%s)
  while true; do
    if kubectl -n "$NAMESPACE" get "$target" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null | grep -q True; then
      echo "   ✅ $target Ready"
      return 0
    fi
    if [ $(( $(date +%s) - start )) -gt "$timeout" ]; then
      echo "   ⚠️ $target not Ready after ${timeout}s"
      return 1
    fi
    sleep 3
  done
}

# 1. Ensure a kind cluster exists
if ! kind get clusters | grep -qx "$CLUSTER"; then
  echo "🪪 Creating kind cluster '$CLUSTER' ..."
  kind create cluster --name "$CLUSTER" --config "$ROOT/kind-config.yaml"
fi
kubectl cluster-info >/dev/null

# 2. Install cert-manager first: the RabbitMQ operator manifests create
#    cert-manager Certificate/Issuer objects, so the CRDs must already exist.
echo "☸️  Installing cert-manager ${CERT_MANAGER_VERSION} ..."
kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
kubectl -n cert-manager rollout status deployment/cert-manager --timeout=300s
kubectl -n cert-manager rollout status deployment/cert-manager-webhook --timeout=300s
kubectl -n cert-manager rollout status deployment/cert-manager-cainjector --timeout=300s

# 3. Install the RabbitMQ Cluster Operator (creates the rabbitmq-system namespace)
echo "☸️  Installing RabbitMQ Cluster Operator ..."
kubectl apply -f "https://github.com/rabbitmq/cluster-operator/releases/latest/download/cluster-operator.yml"
kubectl -n rabbitmq-system rollout status deployment/rabbitmq-cluster-operator --timeout=300s

# 4. Install the Messaging Topology Operator (declarative exchanges/queues/bindings)
echo "☸️  Installing RabbitMQ Messaging Topology Operator ..."
kubectl apply -f "https://github.com/rabbitmq/messaging-topology-operator/releases/latest/download/messaging-topology-operator-with-certmanager.yaml"
kubectl -n rabbitmq-system rollout status deployment/messaging-topology-operator --timeout=300s

# 5. Build images and load them into kind
"$ROOT/scripts/build.sh"
for app in publisher worker consumer; do
  kind load docker-image "${REGISTRY}/${app}:${TAG}" --name "$CLUSTER"
done

# 6. Namespace + PostgreSQL + RabbitMQ cluster
echo "📦 Applying namespace, PostgreSQL and the RabbitMQ cluster ..."
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/20-postgres.yaml
kubectl apply -f k8s/10-rabbitmq.yaml
kubectl -n "$NAMESPACE" rollout status statefulset/postgres --timeout=180s
echo "⏳ Waiting for the RabbitMQ cluster ..."
kubectl -n "$NAMESPACE" rollout status statefulset/rabbitmq-server --timeout=420s
for _ in $(seq 1 60); do
  kubectl -n "$NAMESPACE" get secret rabbitmq-default-user >/dev/null 2>&1 && break
  sleep 5
done

# 7. Declarative AMQP topology (exchanges, quorum queues, bindings)
echo "📦 Applying AMQP topology ..."
kubectl apply -f k8s/15-topology.yaml
for q in orders-work orders-dlq orders-audit orders-notifications; do
  wait_ready "queue.rabbitmq.com/$q" 180 || true
done

# 8. Applications
echo "📦 Applying applications ..."
kubectl apply -f k8s/30-publisher.yaml
kubectl apply -f k8s/40-worker.yaml
kubectl apply -f k8s/50-consumers.yaml
for app in publisher worker consumer-audit consumer-notifications; do
  repo="$app"
  case "$app" in
    consumer-audit|consumer-notifications) repo=consumer ;;
  esac
  kubectl -n "$NAMESPACE" set image "deployment/${app}" "${app}=${REGISTRY}/${repo}:${TAG}" >/dev/null
done
for app in publisher worker consumer-audit consumer-notifications; do
  kubectl -n "$NAMESPACE" rollout status "deployment/${app}" --timeout=180s
done

cat <<MSG

✅ Deployment complete (namespace: ${NAMESPACE}).

  Management UI : make ports   (then http://localhost:15672)
  Verify        : make verify
  Queue depths  : make queues
  Test suite    : make test

MSG
