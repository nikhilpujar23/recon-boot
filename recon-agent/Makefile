PYTHON      ?= python3
UDIR_FILE   ?= /tmp/udir.txt
DATABASE_URL ?= $(shell grep DATABASE_URL .env 2>/dev/null | cut -d= -f2-)

.PHONY: install migrate seed recon dev test test-eval clean \
        docker-build docker-up docker-down docker-restart \
        docker-migrate docker-seed docker-recon docker-logs docker-ps \
        docker-shell-postgres docker-shell-worker docker-reset

# ── Local dev (no Docker) ─────────────────────────────────────────────────────

install:
	$(PYTHON) -m pip install -e ".[dev,kafka]"

migrate:
	psql "$(DATABASE_URL)" -f migrations/001_init.sql
	psql "$(DATABASE_URL)" -f migrations/002_phase2.sql

seed:
	$(PYTHON) scripts/seed_pg_txns.py
	$(PYTHON) scripts/gen_settlement_file.py

recon:
	$(PYTHON) -m recon.cli recon $(UDIR_FILE)

dev: seed recon

test:
	pytest tests/ -v -m "not eval"

test-eval:
	pytest tests/ -v -m eval

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

# ── Docker ────────────────────────────────────────────────────────────────────

## Build the recon-agent image
docker-build:
	docker compose build --no-cache

## Start all services (detached)
docker-up:
	docker compose up -d

## Stop all services and remove containers (volumes are preserved)
docker-down:
	docker compose down

## Restart a specific service: make docker-restart svc=rules-worker
docker-restart:
	docker compose restart $(svc)

## Apply migrations inside the running postgres container
docker-migrate:
	docker compose exec postgres psql -U recon recon \
	  -f /docker-entrypoint-initdb.d/001_init.sql || true
	docker compose exec postgres psql -U recon recon \
	  -f /docker-entrypoint-initdb.d/002_phase2.sql || true

## Seed pg_transactions and generate a UDIR settlement file inside the container
docker-seed:
	docker compose run --rm --no-deps ingest-worker seed --rows 1000

## Run a one-shot reconciliation pass inside the container
## Usage: make docker-recon file=/data/sftp/udir_2026_04_30.txt
docker-recon:
	docker compose run --rm --no-deps ingest-worker recon $(file)

## Tail logs for all pipeline workers (Ctrl+C to stop — exit 130 is normal)
docker-logs:
	-docker compose logs -f outbox-drainer ingest-worker rules-worker sftp-watcher

## Tail logs for a single service: make docker-log svc=rules-worker
docker-log:
	-docker compose logs -f $(or $(svc),rules-worker)

## Tail logs for all services
docker-logs-all:
	-docker compose logs -f

## Show running service status
docker-ps:
	docker compose ps

## Open a psql shell on the postgres container
docker-shell-postgres:
	-docker compose exec postgres psql -U recon recon

## Open a bash shell inside a running worker (default: rules-worker)
## Usage: make docker-shell-worker svc=ingest-worker
docker-shell-worker:
	-docker compose exec $(or $(svc),rules-worker) bash

## Wipe all Docker volumes (destructive — deletes all data)
docker-reset:
	docker compose down -v
	@echo "All volumes removed."
