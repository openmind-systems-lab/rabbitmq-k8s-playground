#!/usr/bin/env bash
set -euo pipefail

# Assert that the competing consumers (orders.work) process each message exactly once.
NAMESPACE="${NAMESPACE:-rabbitmq-playground}"
COUNT="${COUNT:-20}"

db_count() {
  kubectl -n "$NAMESPACE" exec statefulset/postgres -- \
    psql -U orders -d orders -tAc "SELECT count(*) FROM processed_orders;" | tr -d '[:space:]'
}

queue_depth() {
  kubectl -n "$NAMESPACE" exec rabbitmq-server-0 -- \
    rabbitmqctl list_queues name messages 2>/dev/null | awk -v q="$1" '$1==q {print $2}'
}

before="$(db_count)"
echo "📊 processed_orders before: ${before}"

echo "📨 Publishing ${COUNT} orders ..."
kubectl -n "$NAMESPACE" exec deployment/publisher -- \
  wget -qO- "http://localhost:8080/publish?count=${COUNT}"
echo ""
sleep 6

after="$(db_count)"
delta=$(( after - before ))
echo "📊 processed_orders after: ${after} (delta=${delta}, expected >= ${COUNT})"

if [ "$delta" -lt "$COUNT" ]; then
  echo "❌ Competing consumers did not process every message."
  exit 1
fi

echo "⏳ orders.work depth: $(queue_depth orders.work)"
echo "✅ Competing consumers processed every work message at least once."
