from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from pathlib import Path

import typer

app = typer.Typer(name="recon", help="Autonomous Payment Reconciliation Agent — CLI")


# ── recon recon <file> ────────────────────────────────────────────────────────


@app.command("recon")
def cmd_recon(
    file: Path = typer.Argument(..., help="Path to UDIR settlement file"),
    file_id: str = typer.Option("", help="Override file_id (default: filename stem)"),
    rules_only: bool = typer.Option(False, "--rules-only", help="Skip agent investigation"),
) -> None:
    """Parse a UDIR file, run the rules engine, print a match summary."""
    if not file.exists():
        typer.echo(f"File not found: {file}", err=True)
        raise typer.Exit(1)

    fid = file_id or file.stem
    asyncio.run(_run_recon(fid, file))


async def _run_recon(file_id: str, path: Path) -> None:
    from recon.config import settings
    from recon.ingest.parser import UdirParser
    from recon.ledger.repo import LedgerRepo, create_pool
    from recon.models import SettlementLine
    from recon.rules.engine import RulesEngine

    pool = await create_pool(settings.database_url)
    repo = LedgerRepo(pool)
    # outbox_enabled=False: CLI runs without Kafka broker in the loop
    parser = UdirParser(repo, outbox_enabled=False)
    engine = RulesEngine(repo)
    engine.reset_seen()

    typer.echo(f"Parsing {path} (file_id={file_id}) …")
    written = await parser.parse(file_id, path)
    typer.echo(f"  Persisted {written} settlement lines")

    # Fetch all lines for this file and run rules
    sl_rows = await repo.fetch_settlement_lines_for_file(file_id)

    counters: dict[str, int] = {}
    total = 0

    for row in sl_rows:
        raw = json.loads(row["raw"]) if row["raw"] else {}
        line = SettlementLine(
            file_id=row["file_id"],
            line_no=row["line_no"],
            rrn=row["rrn"],
            utr=row["utr"],
            amount_paise=row["amount_paise"],
            fee_paise=row["fee_paise"],
            net_paise=row["net_paise"],
            status=row["status"],
            txn_date=None,
            payer_vpa=None,
            payee_vpa=None,
            txn_type=None,
            remarks=None,
            raw=raw,
            id=row["id"],
        )
        case = await engine.run(line)
        await repo.insert_recon_case(case)
        counters[case.match_type] = counters.get(case.match_type, 0) + 1
        total += 1

    await pool.close()

    # ── Summary ───────────────────────────────────────────────────────────────
    if total == 0:
        typer.echo("No lines processed.")
        return

    auto_resolved = (
        counters.get("EXACT", 0)
        + counters.get("TOLERANCE", 0)
        + counters.get("DUPLICATE", 0)
    )
    needs_agent = (
        counters.get("AMOUNT_MISMATCH", 0)
        + counters.get("MISSING_LEG", 0)
        + counters.get("UNKNOWN", 0)
    )

    typer.echo(f"\n{'─'*50}")
    typer.echo(f"  Total lines processed : {total}")
    typer.echo(f"  Auto-resolved         : {auto_resolved} ({auto_resolved/total*100:.1f}%)")
    typer.echo(f"  Needs agent / unknown : {needs_agent} ({needs_agent/total*100:.1f}%)")
    typer.echo(f"{'─'*50}")
    for mtype, n in sorted(counters.items()):
        typer.echo(f"    {mtype:<20} {n:>5}")
    typer.echo(f"{'─'*50}")

    typer.echo(f"\nmatched: {auto_resolved} ({auto_resolved/total*100:.1f}%)")
    typer.echo(f"unmatched: {needs_agent} ({needs_agent/total*100:.1f}%)")


# ── recon seed ────────────────────────────────────────────────────────────────


@app.command("seed")
def cmd_seed(
    rows: int = typer.Option(1_000, "--rows", help="Number of pg_transactions to seed"),
) -> None:
    """Seed synthetic pg_transactions and generate a UDIR settlement file."""
    # cli.py is at <root>/src/recon/cli.py — 3 levels up = project root
    # Works both locally (recon-agent/src/recon/cli.py → recon-agent/)
    # and in the container (/app/src/recon/cli.py → /app/)
    root = Path(__file__).parent.parent.parent
    scripts = root / "scripts"

    typer.echo("Seeding pg_transactions …")
    _run_script(scripts / "seed_pg_txns.py")

    typer.echo("Generating UDIR settlement file …")
    _run_script(scripts / "gen_settlement_file.py")


def _run_script(path: Path) -> None:
    result = subprocess.run([sys.executable, str(path)], check=False)
    if result.returncode != 0:
        typer.echo(f"Script failed: {path}", err=True)
        raise typer.Exit(result.returncode)


# ── recon backfill ────────────────────────────────────────────────────────────


@app.command("backfill")
def cmd_backfill(
    since: str = typer.Option(..., help="ISO date, e.g. 2026-04-01"),
    rules_only: bool = typer.Option(False, "--rules-only"),
) -> None:
    """Re-run the rules engine over existing settlement_lines without re-parsing files.

    Useful after a rule change to re-score all UNKNOWN/PENDING cases since `--since`.
    Does NOT re-parse files or touch processed_files; purely reads existing DB rows.
    """
    typer.echo(f"Backfill since {since} (rules_only={rules_only}) …")
    asyncio.run(_run_backfill(since=since, rules_only=rules_only))


async def _run_backfill(since: str, rules_only: bool) -> None:
    from recon.config import settings
    from recon.ledger.repo import LedgerRepo, create_pool
    from recon.models import SettlementLine
    from recon.rules.engine import RulesEngine

    pool = await create_pool(settings.database_url)
    repo = LedgerRepo(pool)
    engine = RulesEngine(repo)
    engine.reset_seen()

    typer.echo(f"  Fetching unresolved cases since {since} …")
    rows = await repo.fetch_unresolved_cases_since(since)
    typer.echo(f"  Found {len(rows)} unresolved case(s) to re-evaluate")

    counters: dict[str, int] = {}
    updated = 0

    for row in rows:
        raw = json.loads(row["raw"]) if row["raw"] else {}
        line = SettlementLine(
            file_id=row["file_id"],
            line_no=row["line_no"],
            rrn=row["rrn"],
            utr=row["utr"],
            amount_paise=row["amount_paise"],
            fee_paise=row["fee_paise"],
            net_paise=row["net_paise"],
            status=row["status"],
            txn_date=None,
            payer_vpa=None,
            payee_vpa=None,
            txn_type=None,
            remarks=None,
            raw=raw,
            id=row["id"],
        )

        case = await engine.run(line)
        # Only update if the rules now resolve the case (move from UNKNOWN → resolved)
        if case.resolution is not None:
            fields = {
                "resolution": case.resolution,
                "resolved_by": case.resolved_by,
                "confidence": case.confidence,
                "notes": case.notes,
                "pg_txn_id": case.pg_txn_id,
            }
            written = await repo.update_recon_case_proposed(case.case_uid, fields)
            if written:
                updated += 1

        counters[case.match_type] = counters.get(case.match_type, 0) + 1

    await pool.close()

    typer.echo(f"\n{'─'*50}")
    typer.echo(f"  Cases evaluated  : {len(rows)}")
    typer.echo(f"  Cases updated    : {updated}")
    typer.echo(f"{'─'*50}")
    for mtype, n in sorted(counters.items()):
        typer.echo(f"    {mtype:<20} {n:>5}")
    typer.echo(f"{'─'*50}")


# ── recon dlq ─────────────────────────────────────────────────────────────────

dlq_app = typer.Typer(help="Dead-letter queue management commands.")
app.add_typer(dlq_app, name="dlq")


@dlq_app.command("replay")
def cmd_dlq_replay(
    topic: str = typer.Option("recon.investigate", help="DLQ source topic to replay"),
    target: str = typer.Option("", help="Target topic (default: original_topic from message)"),
    max_n: int = typer.Option(100, "--max", help="Maximum messages to replay"),
) -> None:
    """Re-publish messages from recon.dlq back to their original topic.

    Reads up to --max messages from `recon.dlq`, extracts the original payload,
    and re-publishes to either the original_topic field or --target.

    Requires the kafka optional dependency:
      pip install 'recon-agent[kafka]'
    """
    asyncio.run(_run_dlq_replay(source_topic=topic, target_topic=target, max_n=max_n))


async def _run_dlq_replay(source_topic: str, target_topic: str, max_n: int) -> None:
    try:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    except ImportError:
        typer.echo(
            "aiokafka not installed. Run: pip install 'recon-agent[kafka]'", err=True
        )
        raise typer.Exit(1)

    from recon.config import settings
    from recon.messaging import TOPIC_RECON_DLQ

    consumer = AIOKafkaConsumer(
        TOPIC_RECON_DLQ,
        bootstrap_servers=settings.kafka_brokers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5_000,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_brokers,
        enable_idempotence=True,
        acks="all",
    )
    await consumer.start()
    await producer.start()
    typer.echo(f"Replaying up to {max_n} messages from {TOPIC_RECON_DLQ} …")

    replayed = 0
    try:
        async for msg in consumer:
            if replayed >= max_n:
                break
            try:
                envelope = json.loads(msg.value.decode("utf-8"))
                original_payload = bytes.fromhex(envelope["payload_b64"])
                dest = target_topic or envelope.get("original_topic", source_topic)
                await producer.send(
                    topic=dest,
                    value=original_payload,
                    key=msg.key,
                )
                replayed += 1
            except Exception as exc:
                typer.echo(f"  Skipped malformed DLQ message: {exc}", err=True)
            await consumer.commit()
    finally:
        await producer.flush()
        await consumer.stop()
        await producer.stop()

    typer.echo(f"Replayed {replayed} message(s).")


# ── recon workers ─────────────────────────────────────────────────────────────


@app.command("workers")
def cmd_workers(
    component: str = typer.Argument(
        "all",
        help="Which component to start: all | ingest | rules | outbox | watcher | agent",
    ),
    watch_dir: Path = typer.Option(Path("/data/sftp"), help="Directory to watch for new files"),
    mock_llm: bool = typer.Option(False, "--mock-llm", help="Use mock LLM (no API calls)"),
) -> None:
    """Start Phase 2/3 workers (Kafka consumers + outbox drainer + watcher + agent).

    Requires the kafka optional dependency:
      pip install 'recon-agent[kafka]'
    """
    asyncio.run(_run_workers(component=component, watch_dir=watch_dir, mock_llm=mock_llm))


async def _run_workers(component: str, watch_dir: Path, mock_llm: bool = False) -> None:
    from recon.config import settings
    from recon.ledger.repo import LedgerRepo, create_pool

    pool = await create_pool(settings.database_url)
    repo = LedgerRepo(pool)

    tasks = []

    if component in ("all", "outbox"):
        from recon.ledger.outbox import OutboxPublisher, listen_for_outbox_notify
        publisher = OutboxPublisher(repo=repo, brokers=settings.kafka_brokers)
        tasks.append(asyncio.create_task(publisher.run(), name="outbox"))
        tasks.append(
            asyncio.create_task(
                listen_for_outbox_notify(pool, publisher), name="outbox-notify"
            )
        )

    if component in ("all", "ingest"):
        from recon.ingest.kafka_consumer import FileIngestConsumer
        ingest = FileIngestConsumer(
            brokers=settings.kafka_brokers,
            watch_dir=watch_dir,
            repo=repo,
            group_prefix=settings.kafka_consumer_group_prefix,
        )
        tasks.append(asyncio.create_task(ingest.run(), name="ingest-consumer"))

    if component in ("all", "rules"):
        from recon.rules.kafka_consumer import ReconRequestConsumer
        rules_consumer = ReconRequestConsumer(
            brokers=settings.kafka_brokers,
            repo=repo,
            group_prefix=settings.kafka_consumer_group_prefix,
        )
        tasks.append(asyncio.create_task(rules_consumer.run(), name="rules-consumer"))

    if component in ("all", "watcher"):
        from recon.ingest.sftp_watcher import SftpWatcher
        watcher = SftpWatcher(watch_dir=watch_dir, repo=repo)
        tasks.append(asyncio.create_task(watcher.run(), name="sftp-watcher"))

    if component in ("all", "agent"):
        from recon.agent.kafka_consumer import AgentConsumer
        agent_consumer = AgentConsumer(
            brokers=settings.kafka_brokers,
            repo=repo,
            group_prefix=settings.kafka_consumer_group_prefix,
            mock_llm=mock_llm,
        )
        tasks.append(asyncio.create_task(agent_consumer.run(), name="agent-consumer"))

    if not tasks:
        typer.echo(f"Unknown component: {component}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Started {len(tasks)} worker task(s): {[t.get_name() for t in tasks]}")
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        typer.echo("Shutting down workers …")
    finally:
        for t in tasks:
            t.cancel()
        await pool.close()


if __name__ == "__main__":
    app()
