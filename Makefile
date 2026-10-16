.DEFAULT_GOAL := help
COMPOSE := docker compose
CLI     := $(COMPOSE) run --rm --no-deps airflow-cli
DAG_ID  := json_medallion_pipeline

# Pick up host ports from .env (if present) so echoed URLs match the running stack
-include .env
AIRFLOW_PORT   ?= 8080
WAREHOUSE_PORT ?= 5433

.PHONY: help init up down restart logs ps seed unpause trigger backfill psql test test-unit test-integration clean reset

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

init: ## Create .env, runtime dirs, build image, migrate Airflow metadata DB
	@test -f .env || cp .env.example .env
	@mkdir -p logs data/landing data/bronze
	$(COMPOSE) build
	$(COMPOSE) up airflow-init

up: init seed ## Start the full stack (Airflow UI at http://localhost:$(AIRFLOW_PORT), airflow/airflow)
	$(COMPOSE) up -d
	@echo "Airflow UI: http://localhost:$(AIRFLOW_PORT)  (airflow / airflow)"
	@echo "Warehouse:  postgresql://warehouse:warehouse@localhost:$(WAREHOUSE_PORT)/warehouse"

down: ## Stop containers (keeps volumes)
	$(COMPOSE) down

restart: down up ## Restart the stack

logs: ## Tail scheduler + webserver logs
	$(COMPOSE) logs -f airflow-scheduler airflow-webserver

ps: ## Show container status
	$(COMPOSE) ps

seed: ## Copy sample JSON into the landing zone (data/landing)
	@mkdir -p data/landing
	cp data/samples/*.json data/landing/
	@ls -1 data/landing

unpause: ## Unpause the main DAG
	$(CLI) airflow dags unpause $(DAG_ID)

trigger: unpause ## Trigger one run of the main DAG
	$(CLI) airflow dags trigger $(DAG_ID)

backfill: ## Backfill: make backfill START=2026-10-01 END=2026-10-03
	$(CLI) airflow dags backfill $(DAG_ID) --start-date $(START) --end-date $(END) --reset-dagruns -y

psql: ## Open psql against the warehouse
	$(COMPOSE) exec warehouse psql -U warehouse -d warehouse

test: test-unit test-integration ## Run all tests

# NB: the official Airflow entrypoint only passes through a few known commands
# (bash, python, airflow, ...); anything else is prefixed with `airflow`, so
# pytest must be invoked as `python -m pytest`.
test-unit: ## Unit + DAG-integrity tests (no warehouse needed)
	$(CLI) python -m pytest -m "not integration" /opt/airflow/tests

test-integration: ## End-to-end SQL tests against the warehouse (stack must be up)
	$(COMPOSE) run --rm airflow-cli python -m pytest -m integration /opt/airflow/tests

clean: ## Remove landed/bronze files and logs (keeps DB volumes)
	rm -rf data/landing/*.json data/bronze/* logs/*

reset: ## Full teardown including DB volumes
	$(COMPOSE) down -v --remove-orphans
	rm -rf data/landing/*.json data/bronze/* logs/*
