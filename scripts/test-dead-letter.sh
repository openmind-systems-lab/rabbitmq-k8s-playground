#!/usr/bin/env bash
set -euo pipefail

# Assert that poison messages are rejected and dead-lettered to orders.dlq.
NAMESPACE="${NAMESPACE:-rabbitmq-playground}"
POISON="${POISON:-5}"

queue_depth() {
  kubectl -n "$NAMESPACE" exec rabbitmq-server-0 -- \
    rabbitmqctl list_queues name messages 2>/dev/null | awk -v q="$1" '$1==q {print $2}'
}

before="$(queue_depth orders.dlq)"
before="${before:-0}"
echo "📊 orders.dlq before: ${before}"

echo "☠️ Publishing ${POISON} poison messages ..."
kubectl -n "$NAMESPACE" exec deployment/publisher -- \
  wget -qO- "http://localhost:8080/publish?count=0&poison=${POISON}"
echo ""
sleep 6

after="$(queue_depth orders.dlq)"
after="${after:-0}"
delta=$(( after - before ))
echo "📊 orders.dlq after: ${after} (delta=${delta}, expected >= ${POISON})"

if [ "$delta" -lt "$POISON" ]; then
  echo "❌ Poison messages were not dead-lettered."
  exit 1
fi

echo "✅ Poison messages were rejected and dead-lettered to orders.dlq."
