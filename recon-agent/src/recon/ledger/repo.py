from __future__ import annotations

import json
import uuid
from typing import Optional

import asyncpg

from recon.models import PgTxn, ReconCase, SettlementLine


class LedgerRepo:
    """Thin async data-access layer. One instance per process; pass conn for tx sharing."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ── Settlement lines ──────────────────────────────────────────────────────

    async def insert_settlement_line(self, line: SettlementLine) -> Optional[int]:
        """Insert one line idempotently. Returns the row id, or None if duplicate."""
        raw_json = json.dumps(line.raw)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO settlement_lines
                    (file_id, line_no, rrn, utr, amount_paise, fee_paise,
                     net_paise, status, raw)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
                ON CONFLICT (file_id, line_no) DO NOTHING
                RETURNING id
                """,
                line.file_id,
                line.line_no,
                line.rrn,
                line.utr,
                line.amount_paise,
                line.fee_paise,
                line.net_paise,
                line.status,
                raw_json,
            )
        return row["id"] if row else None

    # ── PG transactions ───────────────────────────────────────────────────────

    async def lookup_pg_txns(
        self, rrn: Optional[str], utr: Optional[str]
    ) -> list[PgTxn]:
        """Return all PG txns matching either rrn or utr, ordered by id ASC."""
        if not rrn and not utr:
            return []
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, txn_id, rrn, utr, payer_vpa, payee_vpa,
                       amount_paise, status, created_at, updated_at
                FROM   pg_transactions
                WHERE  ($1::text IS NOT NULL AND rrn = $1)
                   OR  ($2::text IS NOT NULL AND utr = $2)
                ORDER  BY id ASC
                """,
                rrn,
                utr,
            )
        return [_row_to_pg_txn(r) for r in rows]

    async def insert_pg_txn_bulk(self, rows: list[dict]) -> int:
        """Seed helper: bulk insert pg_transactions, skip conflicts. Returns inserted count."""
        async with self._pool.acquire() as conn:
            result = await conn.executemany(
                """
                INSERT INTO pg_transactions
                    (txn_id, rrn, utr, payer_vpa, payee_vpa,
                     amount_paise, status, created_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (txn_id) DO NOTHING
                """,
                [
                    (
                        r["txn_id"],
                        r.get("rrn"),
                        r.get("utr"),
                        r.get("payer_vpa"),
                        r.get("payee_vpa"),
                        r["amount_paise"],
                        r["status"],
                        r["created_at"],
                        r["updated_at"],
                    )
                    for r in rows
                ],
            )
        # asyncpg executemany returns the status string of the last statement
        return len(rows)

    # ── Recon cases ───────────────────────────────────────────────────────────

    async def insert_recon_case(self, case: ReconCase) -> bool:
        """Insert case idempotently (by case_uid). Returns True if inserted."""
        notes_json = json.dumps(case.notes)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO recon_cases
                    (case_uid, settlement_line, pg_transaction,
                     match_type, confidence, resolution, resolved_by, notes)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
                ON CONFLICT (case_uid) DO NOTHING
                RETURNING id
                """,
                case.case_uid,
                case.settlement_line_id,
                case.pg_txn_id,
                case.match_type,
                case.confidence,
                case.resolution,
                case.resolved_by,
                notes_json,
            )
        return row is not None

    async def update_recon_case_proposed(
        self, uid: uuid.UUID, fields: dict
    ) -> bool:
        """Conditional update: only writes if resolution IS NULL (write guard)."""
        notes_json = json.dumps(fields.get("notes", {}))
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE recon_cases
                SET    resolution    = $2,
                       resolved_by   = $3,
                       confidence    = $4,
                       notes         = $5::jsonb,
                       pg_transaction = $6,
                       resolved_at   = now()
                WHERE  case_uid = $1
                  AND  resolution IS NULL
                """,
                uid,
                fields.get("resolution"),
                fields.get("resolved_by"),
                fields.get("confidence"),
                notes_json,
                fields.get("pg_txn_id"),
            )
        # result is e.g. "UPDATE 1"
        return result.endswith("1")

    async def fetch_proposed_cases(
        self, limit: int = 50, offset: int = 0
    ) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM recon_cases
                WHERE  resolution = 'PROPOSED'
                ORDER  BY created_at ASC
                LIMIT  $1 OFFSET $2
                """,
                limit,
                offset,
            )
        return [dict(r) for r in rows]

    # ── Processed files (SFTP watcher dedup) ─────────────────────────────────

    async def insert_processed_file(
        self,
        conn,
        file_hash: str,
        file_id: str,
        filename: str,
        bytes_: int,
    ) -> bool:
        """Insert into processed_files. Returns True if new, False if duplicate."""
        row = await conn.fetchrow(
            """
            INSERT INTO processed_files (file_hash, file_id, filename, bytes)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (file_hash) DO NOTHING
            RETURNING file_hash
            """,
            file_hash,
            file_id,
            filename,
            bytes_,
        )
        return row is not None

    async def is_file_processed(self, file_hash: str) -> bool:
        """Return True if this file_hash already exists in processed_files."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM processed_files WHERE file_hash = $1", file_hash
            )
        return row is not None

    # ── Outbox (transactional outbox) ─────────────────────────────────────────

    async def insert_outbox_tx(
        self,
        conn,
        topic: str,
        payload: bytes,
        schema_id: int,
        partition_key: str | None = None,
    ) -> None:
        """Insert an outbox row using an existing connection (for tx sharing)."""
        await conn.execute(
            """
            INSERT INTO outbox (topic, partition_key, payload, schema_id)
            VALUES ($1, $2, $3, $4)
            """,
            topic,
            partition_key,
            payload,
            schema_id,
        )

    async def fetch_outbox_batch(self, batch_size: int = 200) -> list[dict]:
        """Fetch unpublished outbox rows ordered by id ASC."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, topic, partition_key, payload, schema_id
                FROM   outbox
                WHERE  published_at IS NULL
                ORDER  BY id ASC
                LIMIT  $1
                """,
                batch_size,
            )
        return [dict(r) for r in rows]

    async def mark_outbox_published(self, ids: list[int]) -> None:
        """Mark a batch of outbox rows as published."""
        if not ids:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])",
                ids,
            )

    # ── Settlement lines (consumer fetch) ────────────────────────────────────

    async def fetch_settlement_lines_for_file(self, file_id: str) -> list[dict]:
        """Fetch all settlement lines for a given file_id ordered by line_no."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, file_id, line_no, rrn, utr, amount_paise, fee_paise,
                       net_paise, status, raw, ingested_at
                FROM   settlement_lines
                WHERE  file_id = $1
                ORDER  BY line_no ASC
                """,
                file_id,
            )
        return [dict(r) for r in rows]

    async def fetch_settlement_line_by_case(self, file_id: str, line_no: int) -> dict | None:
        """Fetch a single settlement line by (file_id, line_no)."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, file_id, line_no, rrn, utr, amount_paise, fee_paise,
                       net_paise, status, raw, ingested_at
                FROM   settlement_lines
                WHERE  file_id = $1 AND line_no = $2
                """,
                file_id,
                line_no,
            )
        return dict(row) if row else None

    async def fetch_unresolved_cases_since(self, since_date: str) -> list[dict]:
        """Fetch cases with no resolution (NULL) created on or after since_date.
        Used by the backfill CLI command.
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT rc.id, rc.case_uid, sl.file_id, sl.line_no,
                       sl.rrn, sl.utr, sl.amount_paise, sl.fee_paise,
                       sl.net_paise, sl.status, sl.raw
                FROM   recon_cases rc
                JOIN   settlement_lines sl ON sl.id = rc.settlement_line
                WHERE  rc.resolution IS NULL
                  AND  rc.created_at >= $1::date
                ORDER  BY rc.id ASC
                """,
                since_date,
            )
        return [dict(r) for r in rows]

    # ── Agent support ─────────────────────────────────────────────────────────

    async def fetch_recon_case_by_file_line(
        self, file_id: str, line_no: int
    ) -> dict | None:
        """Return the recon_case row for the settlement line identified by
        (file_id, line_no).  Returns None if no case exists yet."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT rc.id, rc.case_uid, rc.settlement_line,
                       rc.pg_transaction, rc.match_type, rc.confidence,
                       rc.resolution, rc.resolved_by, rc.notes, rc.created_at
                FROM   recon_cases rc
                JOIN   settlement_lines sl ON sl.id = rc.settlement_line
                WHERE  sl.file_id = $1 AND sl.line_no = $2
                ORDER  BY rc.id DESC
                LIMIT  1
                """,
                file_id,
                line_no,
            )
        return dict(row) if row else None

    async def search_pg_transactions(
        self,
        rrn: Optional[str] = None,
        utr: Optional[str] = None,
        amount_min: Optional[int] = None,
        amount_max: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> list[dict]:
        """Flexible PG-transaction search used by the agent tool
        search_pg_transactions.  All filters are optional and ANDed together.
        Returns dicts with amount in paise.
        """
        if not any([rrn, utr, amount_min, amount_max, date_from, date_to]):
            return []
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, txn_id, rrn, utr, payer_vpa, payee_vpa,
                       amount_paise, status, created_at, updated_at
                FROM   pg_transactions
                WHERE  ($1::text  IS NULL OR rrn          = $1)
                  AND  ($2::text  IS NULL OR utr          = $2)
                  AND  ($3::int8  IS NULL OR amount_paise >= $3)
                  AND  ($4::int8  IS NULL OR amount_paise <= $4)
                  AND  ($5::date  IS NULL OR created_at   >= $5::date)
                  AND  ($6::date  IS NULL OR created_at   <= $6::date + interval '1 day')
                ORDER  BY id ASC
                LIMIT  50
                """,
                rrn,
                utr,
                amount_min,
                amount_max,
                date_from,
                date_to,
            )
        return [
            {
                "id": r["id"],
                "txn_id": r["txn_id"],
                "rrn": r["rrn"],
                "utr": r["utr"],
                "payer_vpa": r["payer_vpa"],
                "payee_vpa": r["payee_vpa"],
                "amount_paise": r["amount_paise"],
                "status": r["status"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]

    async def fetch_settlement_history_by_rrn(self, rrn: str) -> list[dict]:
        """Return all settlement lines that share the given RRN, across all files.
        Used by the agent to detect duplicate settlement entries.
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, file_id, line_no, rrn, utr,
                       amount_paise, fee_paise, net_paise, status, ingested_at
                FROM   settlement_lines
                WHERE  rrn = $1
                ORDER  BY ingested_at ASC
                """,
                rrn,
            )
        return [
            {
                "id": r["id"],
                "file_id": r["file_id"],
                "line_no": r["line_no"],
                "rrn": r["rrn"],
                "utr": r["utr"],
                "amount_paise": r["amount_paise"],
                "fee_paise": r["fee_paise"],
                "net_paise": r["net_paise"],
                "status": r["status"],
                "ingested_at": r["ingested_at"].isoformat() if r["ingested_at"] else None,
            }
            for r in rows
        ]

    async def insert_agent_trace(
        self,
        case_uid: uuid.UUID,
        prompt_hash: str,
        model: str,
        tools_used: list[str],
        steps: list[dict],
        total_ms: int,
        cost_usd: float,
    ) -> None:
        """Insert a row into agent_traces after a case investigation completes."""
        import json as _json
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO agent_traces
                    (case_uid, prompt_hash, model, tools_used, steps, total_ms, cost_usd)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
                """,
                case_uid,
                prompt_hash,
                model,
                tools_used,
                _json.dumps(steps),
                total_ms,
                cost_usd,
            )

    # ── API queries ───────────────────────────────────────────────────────────

    async def fetch_recon_cases(
        self,
        status: Optional[str] = None,
        match_type: Optional[str] = None,
        after_id: Optional[int] = None,
        limit: int = 50,
    ) -> list[dict]:
        """Cursor-paginated list of recon_cases. after_id is exclusive lower bound."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, case_uid, settlement_line, pg_transaction,
                       match_type, confidence, resolution, resolved_by,
                       notes, created_at, resolved_at
                FROM   recon_cases
                WHERE  ($1::text IS NULL OR resolution = $1)
                  AND  ($2::text IS NULL OR match_type = $2)
                  AND  ($3::int8 IS NULL OR id > $3)
                ORDER  BY id ASC
                LIMIT  $4
                """,
                status,
                match_type,
                after_id,
                limit,
            )
        return [dict(r) for r in rows]

    async def fetch_recon_case_by_uid(self, uid: uuid.UUID) -> Optional[dict]:
        """Fetch a single recon_case by case_uid. Returns None if not found."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, case_uid, settlement_line, pg_transaction,
                       match_type, confidence, resolution, resolved_by,
                       notes, created_at, resolved_at
                FROM   recon_cases
                WHERE  case_uid = $1
                """,
                uid,
            )
        return dict(row) if row else None

    async def fetch_last_agent_trace(self, case_uid: uuid.UUID) -> Optional[dict]:
        """Return the most recent agent_trace for a case, or None."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, case_uid, model, tools_used, total_ms, cost_usd, created_at
                FROM   agent_traces
                WHERE  case_uid = $1
                ORDER  BY created_at DESC
                LIMIT  1
                """,
                case_uid,
            )
        return dict(row) if row else None

    async def resolve_recon_case(
        self,
        uid: uuid.UUID,
        resolution: str,
        resolved_by: str,
        comment: str = "",
    ) -> bool:
        """Conditional update: APPROVED or REJECTED, only if currently PROPOSED.
        Returns True if the row was updated, False if not found or wrong state.
        """
        import json as _json
        extra = _json.dumps({"comment": comment}) if comment else "{}"
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE recon_cases
                SET    resolution  = $2,
                       resolved_by = $3,
                       notes       = COALESCE(notes, '{}'::jsonb) || $4::jsonb,
                       resolved_at = now()
                WHERE  case_uid    = $1
                  AND  resolution  = 'PROPOSED'
                """,
                uid,
                resolution,
                resolved_by,
                extra,
            )
        return result.endswith("1")

    # ── Aggregate helpers (used by CLI summary) ───────────────────────────────

    async def count_cases_by_resolution(self) -> dict[str, int]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT COALESCE(resolution, 'PENDING') AS resolution, COUNT(*) AS n
                FROM   recon_cases
                GROUP  BY 1
                """
            )
        return {r["resolution"]: r["n"] for r in rows}

    async def count_cases_by_match_type(self) -> dict[str, int]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT match_type, COUNT(*) AS n
                FROM   recon_cases
                GROUP  BY 1
                """
            )
        return {r["match_type"]: r["n"] for r in rows}


# ── Pool factory ──────────────────────────────────────────────────────────────


async def create_pool(database_url: str, min_size: int = 2, max_size: int = 10) -> asyncpg.Pool:
    return await asyncpg.create_pool(database_url, min_size=min_size, max_size=max_size)


# ── Row mappers ───────────────────────────────────────────────────────────────


def _row_to_pg_txn(r: asyncpg.Record) -> PgTxn:
    return PgTxn(
        id=r["id"],
        txn_id=r["txn_id"],
        rrn=r["rrn"],
        utr=r["utr"],
        payer_vpa=r["payer_vpa"],
        payee_vpa=r["payee_vpa"],
        amount_paise=r["amount_paise"],
        status=r["status"],
        created_at=r["created_at"],
        updated_at=r["updated_at"],
    )
