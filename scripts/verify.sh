#!/usr/bin/env bash
set -euo pipefail

# Verify the rabbitmq-k8s-playground PoC is running and behaving as expected.
NAMESPACE="${NAMESPACE:-rabbitmq-playground}"

echo "== Pods =="
kubectl -n "$NAMESPACE" get pods -o wide

echo ""
echo "== RabbitMQ cluster =="
kubectl -n "$NAMESPACE" get rabbitmqcluster

echo ""
echo "== Topology (exchanges / queues / bindings) =="
kubectl -n "$NAMESPACE" get exchanges,queues,bindings

echo ""
echo "== Queue depths =="
kubectl -n "$NAMESPACE" exec rabbitmq-server-0 -- rabbitmqctl list_queues name type messages

echo ""
echo "== Publisher (last 8 lines) =="
kubectl -n "$NAMESPACE" logs --tail=8 deployment/publisher

echo ""
echo "== Worker (last 8 lines) =="
kubectl -n "$NAMESPACE" logs --tail=8 deployment/worker

echo ""
echo "== consumer-audit (last 5 lines) =="
kubectl -n "$NAMESPACE" logs --tail=5 deployment/consumer-audit

echo ""
echo "== consumer-notifications (last 5 lines) =="
kubectl -n "$NAMESPACE" logs --tail=5 deployment/consumer-notifications

echo ""
echo "== processed_orders (last 10) =="
kubectl -n "$NAMESPACE" exec statefulset/postgres -- psql -U orders -d orders -c \
  "SELECT order_nb, routing_key, worker, processed_at FROM processed_orders ORDER BY processed_at DESC LIMIT 10;"

echo ""
echo "== fanout_events counts by mode =="
kubectl -n "$NAMESPACE" exec statefulset/postgres -- psql -U orders -d orders -c \
  "SELECT mode, count(*) AS messages FROM fanout_events GROUP BY mode ORDER BY mode;"
