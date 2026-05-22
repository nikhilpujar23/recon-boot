.PHONY: build test run infra-up infra-down infra-reset logs help

JAR       := recon-api/target/recon-api-0.1.0-SNAPSHOT.jar
ENV_FILE  := .env
COMPOSE   := docker compose

# ── Build ──────────────────────────────────────────────────────────────────────
build:
	mvn clean install -DskipTests

test:
	mvn test

# ── Run ────────────────────────────────────────────────────────────────────────
run: build
	@if [ -f $(ENV_FILE) ]; then \
	  export $$(grep -v '^#' $(ENV_FILE) | xargs) && java -jar $(JAR); \
	else \
	  echo "No .env file found — copy .env.example to .env and fill in values"; exit 1; \
	fi

# Run with mock LLM (no API key needed — good for local smoke-test)
run-mock: build
	@if [ -f $(ENV_FILE) ]; then \
	  export $$(grep -v '^#' $(ENV_FILE) | xargs) && MOCK_LLM=true java -jar $(JAR); \
	else \
	  MOCK_LLM=true DATABASE_URL=jdbc:postgresql://localhost:5432/recon \
	  DB_USER=recon DB_PASSWORD=recon KAFKA_BROKERS=localhost:9092 \
	  java -jar $(JAR); \
	fi

# ── Infrastructure ─────────────────────────────────────────────────────────────
infra-up:
	$(COMPOSE) up -d postgres kafka kafka-init sftp

infra-up-full:
	$(COMPOSE) up -d

infra-down:
	$(COMPOSE) down

infra-reset:
	$(COMPOSE) down -v
	$(COMPOSE) up -d postgres kafka kafka-init sftp

logs:
	$(COMPOSE) logs -f

# ── Helpers ────────────────────────────────────────────────────────────────────
health:
	curl -s http://localhost:8080/healthz | python3 -m json.tool

cases:
	curl -s -H "Authorization: Bearer $${API_BEARER_TOKEN:-changeme}" \
	     http://localhost:8080/api/v1/cases | python3 -m json.tool

gen-pii-keys:
	@echo "PAN_HMAC_KEY=$$(openssl rand -base64 32)"
	@echo "PAN_AES_KEY=$$(openssl rand -base64 32)"

help:
	@echo ""
	@echo "  make build         Build all modules (skips tests)"
	@echo "  make test          Run all unit tests"
	@echo "  make run           Build + run (reads .env)"
	@echo "  make run-mock      Build + run with MOCK_LLM=true"
	@echo "  make infra-up      Start Postgres + Kafka + SFTP"
	@echo "  make infra-up-full Start full stack (+ Grafana, Tempo, Prometheus)"
	@echo "  make infra-down    Stop infrastructure"
	@echo "  make infra-reset   Wipe volumes and restart infra"
	@echo "  make logs          Tail docker compose logs"
	@echo "  make health        Hit /healthz"
	@echo "  make cases         List reconciliation cases"
	@echo "  make gen-pii-keys  Generate fresh PAN_HMAC_KEY and PAN_AES_KEY"
	@echo ""
