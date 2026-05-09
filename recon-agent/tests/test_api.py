"""
Unit tests for the FastAPI service (src/recon/api/main.py).

No live DB or Kafka required; the LedgerRepo dependency is overridden with
an AsyncMock for every test.
"""
from __future__ import annotations

import base64
import json
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from recon.api.main import app, get_repo

# ── Constants ─────────────────────────────────────────────────────────────────

_TOKEN = "changeme"
_AUTH = {"Authorization": f"Bearer {_TOKEN}"}

_UID = uuid.uuid4()
_NOW = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)

_CASE_ROW = {
    "id": 1,
    "case_uid": _UID,
    "settlement_line": 42,
    "pg_transaction": 7,
    "match_type": "AMOUNT_MISMATCH",
    "confidence": 0.96,
    "resolution": "PROPOSED",
    "resolved_by": None,
    "notes": {"rationale": "partial reversal"},
    "created_at": _NOW,
    "resolved_at": None,
}

_TRACE_ROW = {
    "id": 1,
    "case_uid": _UID,
    "model": "claude-sonnet-4-6",
    "tools_used": ["search_pg_transactions"],
    "total_ms": 1500,
    "cost_usd": 0.0012,
    "created_at": _NOW,
}


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_repo(**overrides) -> MagicMock:
    """Return a MagicMock LedgerRepo with sensible async defaults."""
    repo = MagicMock()
    repo.fetch_recon_cases = AsyncMock(return_value=[])
    repo.fetch_recon_case_by_uid = AsyncMock(return_value=None)
    repo.fetch_last_agent_trace = AsyncMock(return_value=None)
    repo.resolve_recon_case = AsyncMock(return_value=True)
    for k, v in overrides.items():
        setattr(repo, k, v)
    return repo


def _override_repo(repo):
    app.dependency_overrides[get_repo] = lambda: repo
    return repo


def _clear_overrides():
    app.dependency_overrides.clear()


# httpx async client helper
async def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


# ── /healthz ─────────────────────────────────────────────────────────────────


class TestHealthz:
    @pytest.mark.asyncio
    async def test_returns_200(self):
        # healthz calls pool.acquire — mock the app state
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value=None)
        mock_acquire = AsyncMock()
        mock_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire = MagicMock(return_value=mock_acquire)

        app.state.pool = mock_pool
        app.state.repo = MagicMock()

        async with await _client() as client:
            resp = await client.get("/healthz")

        assert resp.status_code == 200
        body = resp.json()
        assert body["db"] == "ok"
        assert "kafka" in body
        assert "llm" in body

    @pytest.mark.asyncio
    async def test_db_error_returns_error_status(self):
        mock_pool = MagicMock()
        mock_acquire = MagicMock()
        mock_acquire.__aenter__ = AsyncMock(side_effect=Exception("db down"))
        mock_acquire.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire = MagicMock(return_value=mock_acquire)

        app.state.pool = mock_pool
        app.state.repo = MagicMock()

        async with await _client() as client:
            resp = await client.get("/healthz")

        assert resp.status_code == 200
        assert resp.json()["db"] == "error"

    @pytest.mark.asyncio
    async def test_no_auth_required(self):
        # healthz is public — no Bearer header needed
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value=None)
        mock_acquire = AsyncMock()
        mock_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire = MagicMock(return_value=mock_acquire)

        app.state.pool = mock_pool
        app.state.repo = MagicMock()

        async with await _client() as client:
            resp = await client.get("/healthz")  # no auth header

        assert resp.status_code == 200


# ── Auth & rate limiting ──────────────────────────────────────────────────────


class TestAuth:
    def setup_method(self):
        _override_repo(_make_repo(fetch_recon_cases=AsyncMock(return_value=[])))

    def teardown_method(self):
        _clear_overrides()

    @pytest.mark.asyncio
    async def test_missing_token_returns_401(self):
        async with await _client() as client:
            resp = await client.get("/api/v1/cases")
        assert resp.status_code == 401
        assert resp.json()["error"]["code"] == "UNAUTHORIZED"

    @pytest.mark.asyncio
    async def test_wrong_token_returns_401(self):
        async with await _client() as client:
            resp = await client.get("/api/v1/cases", headers={"Authorization": "Bearer wrong"})
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_correct_token_passes(self):
        async with await _client() as client:
            resp = await client.get("/api/v1/cases", headers=_AUTH)
        assert resp.status_code == 200


# ── GET /api/v1/cases ─────────────────────────────────────────────────────────


class TestListCases:
    def setup_method(self):
        _override_repo(_make_repo())

    def teardown_method(self):
        _clear_overrides()

    @pytest.mark.asyncio
    async def test_empty_result(self):
        async with await _client() as client:
            resp = await client.get("/api/v1/cases", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert body["cases"] == []
        assert body["next_cursor"] is None

    @pytest.mark.asyncio
    async def test_returns_formatted_cases(self):
        repo = _make_repo(fetch_recon_cases=AsyncMock(return_value=[_CASE_ROW]))
        _override_repo(repo)

        async with await _client() as client:
            resp = await client.get("/api/v1/cases", headers=_AUTH)

        body = resp.json()
        assert len(body["cases"]) == 1
        case = body["cases"][0]
        assert case["case_uid"] == str(_UID)
        assert case["match_type"] == "AMOUNT_MISMATCH"
        assert case["confidence"] == 0.96
        assert case["resolution"] == "PROPOSED"
        assert case["settlement_line_id"] == 42
        assert case["pg_transaction_id"] == 7

    @pytest.mark.asyncio
    async def test_next_cursor_set_when_full_page(self):
        # limit=1, returns 1 row → cursor should be set
        rows = [_CASE_ROW]
        repo = _make_repo(fetch_recon_cases=AsyncMock(return_value=rows))
        _override_repo(repo)

        async with await _client() as client:
            resp = await client.get("/api/v1/cases?limit=1", headers=_AUTH)

        body = resp.json()
        assert body["next_cursor"] is not None
        decoded = json.loads(base64.b64decode(body["next_cursor"]))
        assert decoded["id"] == _CASE_ROW["id"]

    @pytest.mark.asyncio
    async def test_no_cursor_when_partial_page(self):
        # limit=10, returns 1 row → no next cursor
        repo = _make_repo(fetch_recon_cases=AsyncMock(return_value=[_CASE_ROW]))
        _override_repo(repo)

        async with await _client() as client:
            resp = await client.get("/api/v1/cases?limit=10", headers=_AUTH)

        assert resp.json()["next_cursor"] is None

    @pytest.mark.asyncio
    async def test_cursor_decoded_and_passed_to_repo(self):
        cursor = base64.b64encode(json.dumps({"id": 99}).encode()).decode()
        repo = _make_repo()
        _override_repo(repo)

        async with await _client() as client:
            await client.get(f"/api/v1/cases?cursor={cursor}", headers=_AUTH)

        repo.fetch_recon_cases.assert_awaited_once()
        _, _, after_id, _ = repo.fetch_recon_cases.call_args[0]
        assert after_id == 99

    @pytest.mark.asyncio
    async def test_invalid_cursor_returns_400(self):
        async with await _client() as client:
            resp = await client.get("/api/v1/cases?cursor=notbase64!!", headers=_AUTH)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_limit_capped_at_200(self):
        repo = _make_repo()
        _override_repo(repo)

        async with await _client() as client:
            await client.get("/api/v1/cases?limit=999", headers=_AUTH)

        _, _, _, limit = repo.fetch_recon_cases.call_args[0]
        assert limit == 200

    @pytest.mark.asyncio
    async def test_status_filter_forwarded(self):
        repo = _make_repo()
        _override_repo(repo)

        async with await _client() as client:
            await client.get("/api/v1/cases?status=PROPOSED", headers=_AUTH)

        status, *_ = repo.fetch_recon_cases.call_args[0]
        assert status == "PROPOSED"


# ── GET /api/v1/cases/{case_uid} ─────────────────────────────────────────────


class TestGetCase:
    def setup_method(self):
        _override_repo(_make_repo())

    def teardown_method(self):
        _clear_overrides()

    @pytest.mark.asyncio
    async def test_not_found_returns_404(self):
        async with await _client() as client:
            resp = await client.get(f"/api/v1/cases/{_UID}", headers=_AUTH)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_found_returns_case(self):
        repo = _make_repo(
            fetch_recon_case_by_uid=AsyncMock(return_value=_CASE_ROW),
            fetch_last_agent_trace=AsyncMock(return_value=None),
        )
        _override_repo(repo)

        async with await _client() as client:
            resp = await client.get(f"/api/v1/cases/{_UID}", headers=_AUTH)

        assert resp.status_code == 200
        body = resp.json()
        assert body["case_uid"] == str(_UID)
        assert "last_agent_trace" not in body

    @pytest.mark.asyncio
    async def test_trace_included_when_present(self):
        repo = _make_repo(
            fetch_recon_case_by_uid=AsyncMock(return_value=_CASE_ROW),
            fetch_last_agent_trace=AsyncMock(return_value=_TRACE_ROW),
        )
        _override_repo(repo)

        async with await _client() as client:
            resp = await client.get(f"/api/v1/cases/{_UID}", headers=_AUTH)

        body = resp.json()
        assert "last_agent_trace" in body
        trace = body["last_agent_trace"]
        assert trace["model"] == "claude-sonnet-4-6"
        assert trace["tools_used"] == ["search_pg_transactions"]

    @pytest.mark.asyncio
    async def test_invalid_uid_returns_400(self):
        async with await _client() as client:
            resp = await client.get("/api/v1/cases/not-a-uuid", headers=_AUTH)
        assert resp.status_code == 400


# ── POST /api/v1/cases/{case_uid}/approve ────────────────────────────────────


class TestApproveCase:
    def setup_method(self):
        _override_repo(_make_repo())

    def teardown_method(self):
        _clear_overrides()

    @pytest.mark.asyncio
    async def test_success_returns_ok(self):
        repo = _make_repo(resolve_recon_case=AsyncMock(return_value=True))
        _override_repo(repo)

        async with await _client() as client:
            resp = await client.post(
                f"/api/v1/cases/{_UID}/approve",
                json={"reviewer_email": "ops@example.com", "comment": "looks good"},
                headers=_AUTH,
            )

        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        repo.resolve_recon_case.assert_awaited_once_with(
            _UID, "APPROVED", "human:ops@example.com", "looks good"
        )

    @pytest.mark.asyncio
    async def test_stale_resolution_returns_409(self):
        approved_row = {**_CASE_ROW, "resolution": "APPROVED"}
        repo = _make_repo(
            resolve_recon_case=AsyncMock(return_value=False),
            fetch_recon_case_by_uid=AsyncMock(return_value=approved_row),
        )
        _override_repo(repo)

        async with await _client() as client:
            resp = await client.post(
                f"/api/v1/cases/{_UID}/approve",
                json={"reviewer_email": "ops@example.com"},
                headers=_AUTH,
            )

        assert resp.status_code == 409
        assert resp.json()["error"]["code"] == "STALE_RESOLUTION"

    @pytest.mark.asyncio
    async def test_not_found_returns_404(self):
        repo = _make_repo(
            resolve_recon_case=AsyncMock(return_value=False),
            fetch_recon_case_by_uid=AsyncMock(return_value=None),
        )
        _override_repo(repo)

        async with await _client() as client:
            resp = await client.post(
                f"/api/v1/cases/{_UID}/approve",
                json={"reviewer_email": "ops@example.com"},
                headers=_AUTH,
            )

        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_missing_reviewer_email_returns_422(self):
        async with await _client() as client:
            resp = await client.post(
                f"/api/v1/cases/{_UID}/approve",
                json={"comment": "no email"},
                headers=_AUTH,
            )
        assert resp.status_code == 422


# ── POST /api/v1/cases/{case_uid}/reject ─────────────────────────────────────


class TestRejectCase:
    def setup_method(self):
        _override_repo(_make_repo())

    def teardown_method(self):
        _clear_overrides()

    @pytest.mark.asyncio
    async def test_success_returns_ok(self):
        repo = _make_repo(resolve_recon_case=AsyncMock(return_value=True))
        _override_repo(repo)

        async with await _client() as client:
            resp = await client.post(
                f"/api/v1/cases/{_UID}/reject",
                json={"reviewer_email": "ops@example.com", "comment": "wrong amount"},
                headers=_AUTH,
            )

        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        repo.resolve_recon_case.assert_awaited_once_with(
            _UID, "REJECTED", "human:ops@example.com", "wrong amount"
        )

    @pytest.mark.asyncio
    async def test_stale_resolution_returns_409(self):
        rejected_row = {**_CASE_ROW, "resolution": "REJECTED"}
        repo = _make_repo(
            resolve_recon_case=AsyncMock(return_value=False),
            fetch_recon_case_by_uid=AsyncMock(return_value=rejected_row),
        )
        _override_repo(repo)

        async with await _client() as client:
            resp = await client.post(
                f"/api/v1/cases/{_UID}/reject",
                json={"reviewer_email": "ops@example.com"},
                headers=_AUTH,
            )

        assert resp.status_code == 409

    @pytest.mark.asyncio
    async def test_approve_sets_approved_resolution(self):
        repo = _make_repo(resolve_recon_case=AsyncMock(return_value=True))
        _override_repo(repo)

        async with await _client() as client:
            await client.post(
                f"/api/v1/cases/{_UID}/approve",
                json={"reviewer_email": "ops@example.com"},
                headers=_AUTH,
            )

        args = repo.resolve_recon_case.call_args[0]
        assert args[1] == "APPROVED"

    @pytest.mark.asyncio
    async def test_reject_sets_rejected_resolution(self):
        repo = _make_repo(resolve_recon_case=AsyncMock(return_value=True))
        _override_repo(repo)

        async with await _client() as client:
            await client.post(
                f"/api/v1/cases/{_UID}/reject",
                json={"reviewer_email": "ops@example.com"},
                headers=_AUTH,
            )

        args = repo.resolve_recon_case.call_args[0]
        assert args[1] == "REJECTED"


# ── X-Request-ID middleware ───────────────────────────────────────────────────


class TestRequestId:
    def setup_method(self):
        _override_repo(_make_repo())

    def teardown_method(self):
        _clear_overrides()

    @pytest.mark.asyncio
    async def test_propagates_provided_request_id(self):
        async with await _client() as client:
            resp = await client.get(
                "/api/v1/cases",
                headers={**_AUTH, "X-Request-ID": "my-req-123"},
            )
        assert resp.headers["X-Request-ID"] == "my-req-123"

    @pytest.mark.asyncio
    async def test_generates_request_id_when_absent(self):
        async with await _client() as client:
            resp = await client.get("/api/v1/cases", headers=_AUTH)
        req_id = resp.headers.get("X-Request-ID", "")
        assert len(req_id) == 36  # UUID4 string length
