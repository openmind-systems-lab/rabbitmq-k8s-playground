.DEFAULT_GOAL := help

NAMESPACE ?= rabbitmq-playground
CLUSTER ?= rabbitmq-k8s
REGISTRY ?= ghcr.io/openmind-systems-lab/rabbitmq-k8s-playground
TAG ?= local

export NAMESPACE CLUSTER REGISTRY TAG

.PHONY: help build deploy verify validate status queues ports logs-publisher logs-worker logs-audit logs-notifications test test-work-queue test-fanout test-dead-letter cleanup destroy

help: ## Show the available commands
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the three local application images
	./scripts/build.sh

deploy: ## Create the kind cluster and deploy the complete playground
	./scripts/deploy.sh

verify: ## Verify workloads, queue depths and persisted data
	./scripts/verify.sh

validate: verify ## Alias for verify, consistent with other OMSL playgrounds

status: ## Show the resources in the playground namespace
	kubectl -n $(NAMESPACE) get pods,rabbitmqcluster,exchanges,queues,bindings,svc

queues: ## Show the RabbitMQ queue depths
	kubectl -n $(NAMESPACE) exec rabbitmq-server-0 -- rabbitmqctl list_queues name type messages

ports: ## Port-forward the RabbitMQ management UI and the AMQP port
	./scripts/ports.sh

logs-publisher: ## Follow the publisher logs
	kubectl -n $(NAMESPACE) logs -f deployment/publisher

logs-worker: ## Follow the worker logs
	kubectl -n $(NAMESPACE) logs -f deployment/worker

logs-audit: ## Follow the audit fanout consumer logs
	kubectl -n $(NAMESPACE) logs -f deployment/consumer-audit

logs-notifications: ## Follow the notifications fanout consumer logs
	kubectl -n $(NAMESPACE) logs -f deployment/consumer-notifications

test: test-work-queue test-fanout test-dead-letter ## Run the full test suite

test-work-queue: ## Assert competing consumers process each message once
	./scripts/test-work-queue.sh

test-fanout: ## Assert both fanout consumers receive every message
	./scripts/test-fanout.sh

test-dead-letter: ## Assert poison messages are dead-lettered
	./scripts/test-dead-letter.sh

cleanup: ## Delete playground resources but keep the kind cluster
	./scripts/cleanup.sh

destroy: ## Delete playground resources and the kind cluster
	DELETE_CLUSTER=true ./scripts/cleanup.sh
