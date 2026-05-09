# Running the Reconciliation Agent

## First-time setup

```bash
# 1. Copy env file and fill in your Anthropic API key
cp .env.example .env

# 2. Build the recon-agent Docker image
make docker-build

# 3. Start all services (Postgres, Kafka, workers, observability)
make docker-up

# 4. Confirm everything is healthy
make docker-ps
```

Postgres auto-runs both migrations on first boot via `docker-entrypoint-initdb.d/`.  
Kafka topics are created by the `kafka-init` container on first boot.

---

## Run a reconciliation

```bash
# Seed 1000 synthetic transactions + generate a UDIR settlement file
make docker-seed

# Watch the pipeline process it
make docker-logs
```

The pipeline flow after `docker-seed`:

```
seed_pg_txns.py  →  pg_transactions table
gen_settlement_file.py  →  /data/sftp/udir_<date>.txt
        ↓
sftp-watcher    detects file → FileArrived → outbox
outbox-drainer  publishes FileArrived → Kafka files.new
ingest-worker   consumes files.new → parses file → ReconRequest per line → outbox
outbox-drainer  publishes ReconRequests → Kafka recon.requests
rules-worker    consumes recon.requests → runs rules engine → writes recon_cases
```

---

## Useful commands

| Command | What it does |
|---|---|
| `make docker-up` | Start all services |
| `make docker-down` | Stop all services (volumes preserved) |
| `make docker-ps` | Show service health status |
| `make docker-logs` | Tail pipeline worker logs (Ctrl+C to stop) |
| `make docker-log svc=rules-worker` | Tail a single service |
| `make docker-logs-all` | Tail every service |
| `make docker-seed` | Seed DB + generate settlement file |
| `make docker-shell-postgres` | Open a psql shell |
| `make docker-shell-worker` | Open bash in rules-worker |
| `make docker-shell-worker svc=ingest-worker` | Open bash in a specific worker |
| `make docker-restart svc=rules-worker` | Restart one service |
| `make docker-reset` | **Destroy all volumes** and start fresh |

---

## Inspect results

```bash
# Open a psql shell
make docker-shell-postgres

# Check match summary
SELECT match_type, resolution, COUNT(*)
FROM recon_cases
GROUP BY 1, 2
ORDER BY 1, 2;

# See cases that need agent review
SELECT case_uid, match_type, notes
FROM recon_cases
WHERE resolution IS NULL
LIMIT 10;
```

---

## Observability

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Tempo (traces) | via Grafana | — |
| Kafka | localhost:9092 | — |
| Postgres | localhost:5432 | recon / recon |
| SFTP | sftp://localhost:2222 | recon / recon |

---

## Re-run after a code change

```bash
# Rebuild image and restart workers only (infra services keep running)
docker compose build ingest-worker rules-worker outbox-drainer sftp-watcher
docker compose up -d ingest-worker rules-worker outbox-drainer sftp-watcher
```

Or for a full clean restart:

```bash
make docker-down
make docker-build
make docker-up
```

---

## Backfill unresolved cases

Re-run the rules engine over existing cases without re-parsing files:

```bash
make docker-shell-worker svc=rules-worker
python -m recon.cli backfill --since 2026-01-01
```

## Replay DLQ

Re-publish failed messages from the dead-letter queue:

```bash
make docker-shell-worker svc=rules-worker
python -m recon.cli dlq replay --max 50
```
