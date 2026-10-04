#!/usr/bin/env bash
set -euo pipefail

# Assert that both fanout consumers (audit / notifications) receive every message.
NAMESPACE="${NAMESPACE:-rabbitmq-playground}"
COUNT="${COUNT:-20}"

mode_count() {
  kubectl -n "$NAMESPACE" exec statefulset/postgres -- \
    psql -U orders -d orders -tAc "SELECT count(*) FROM fanout_events WHERE mode='$1';" | tr -d '[:space:]'
}

echo "📨 Publishing ${COUNT} orders ..."
kubectl -n "$NAMESPACE" exec deployment/publisher -- \
  wget -qO- "http://localhost:8080/publish?count=${COUNT}"
echo ""
sleep 6

audit="$(mode_count audit)"
notifications="$(mode_count notifications)"
echo "🔔 audit=${audit} notifications=${notifications}"

if [ "$audit" -ne "$notifications" ]; then
  echo "❌ Fanout consumers received a different number of messages."
  exit 1
fi

if [ "$audit" -lt "$COUNT" ]; then
  echo "❌ Fanout consumers received fewer messages than published."
  exit 1
fi

echo "✅ Both fanout consumers received the same number of broadcast messages."
