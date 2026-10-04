#!/usr/bin/env bash
set -euo pipefail

# Show the RabbitMQ default user credentials and port-forward the management UI + AMQP.
NAMESPACE="${NAMESPACE:-rabbitmq-playground}"

# The Cluster Operator generates a random default user at deployment time.
credential() {
  kubectl -n "$NAMESPACE" get secret rabbitmq-default-user \
    -o go-template="{{index .data \"$1\" | base64decode}}"
}

echo "🔐 Default credentials:"
echo "   username: $(credential username)"
echo "   password: $(credential password)"
echo ""
echo "🌐 Management UI : http://localhost:15672"
echo "🔌 AMQP          : localhost:5672"
echo "   (press Ctrl+C to stop)"
echo ""

exec kubectl -n "$NAMESPACE" port-forward svc/rabbitmq 15672:15672 5672:5672
