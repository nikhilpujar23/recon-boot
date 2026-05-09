"""
Unit tests for the SFTP watcher — file hashing and dedup logic.

No DB, no watchdog event loop required; tests exercise the synchronous
helper _sha256_file_sync and the watcher's internal _write_to_outbox
logic using async mocks.
"""
from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from recon.ingest.sftp_watcher import _sha256_file_sync


# ── SHA-256 helper ────────────────────────────────────────────────────────────


class TestSha256FileSync:
    def _write(self, content: bytes) -> Path:
        f = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
        f.write(content)
        f.close()
        return Path(f.name)

    def test_correct_hash(self):
        data = b"hello world"
        path = self._write(data)
        digest, size = _sha256_file_sync(path)
        expected = hashlib.sha256(data).hexdigest()
        assert digest == expected
        path.unlink()

    def test_correct_byte_count(self):
        data = b"a" * 4096
        path = self._write(data)
        _, size = _sha256_file_sync(path)
        assert size == 4096
        path.unlink()

    def test_empty_file(self):
        path = self._write(b"")
        digest, size = _sha256_file_sync(path)
        assert digest == hashlib.sha256(b"").hexdigest()
        assert size == 0
        path.unlink()

    def test_multi_chunk_file(self):
        # Write > 1 MiB to exercise chunked reading
        data = b"x" * (1024 * 1024 + 512)  # 1 MiB + 512 bytes
        path = self._write(data)
        digest, size = _sha256_file_sync(path)
        assert digest == hashlib.sha256(data).hexdigest()
        assert size == len(data)
        path.unlink()

    def test_same_content_same_hash(self):
        data = b"deterministic"
        p1, p2 = self._write(data), self._write(data)
        d1, _ = _sha256_file_sync(p1)
        d2, _ = _sha256_file_sync(p2)
        assert d1 == d2
        p1.unlink()
        p2.unlink()

    def test_different_content_different_hash(self):
        p1, p2 = self._write(b"aaa"), self._write(b"bbb")
        d1, _ = _sha256_file_sync(p1)
        d2, _ = _sha256_file_sync(p2)
        assert d1 != d2
        p1.unlink()
        p2.unlink()


# ── SftpWatcher._write_to_outbox logic (mocked DB) ───────────────────────────


class TestSftpWatcherOutboxLogic:
    """
    Test the duplicate-detection and outbox-write logic without a live DB
    by mocking repo methods.
    """

    def _make_watcher(self, insert_processed_returns: bool):
        """Create a watcher with a mocked repo."""
        from recon.ingest.sftp_watcher import SftpWatcher

        mock_repo = MagicMock()
        mock_conn = AsyncMock()
        mock_tx = AsyncMock()
        mock_tx.__aenter__ = AsyncMock(return_value=None)
        mock_tx.__aexit__ = AsyncMock(return_value=False)
        mock_conn.transaction = MagicMock(return_value=mock_tx)

        mock_acquire = AsyncMock()
        mock_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire.__aexit__ = AsyncMock(return_value=False)

        mock_repo._pool = MagicMock()
        mock_repo._pool.acquire = MagicMock(return_value=mock_acquire)

        mock_repo.insert_processed_file = AsyncMock(
            return_value=insert_processed_returns
        )
        mock_repo.insert_outbox_tx = AsyncMock(return_value=None)

        # Bypass watchdog import
        with patch("recon.ingest.sftp_watcher._WATCHDOG_AVAILABLE", True):
            watcher = SftpWatcher.__new__(SftpWatcher)
            watcher._watch_dir = Path("/tmp/sftp")
            watcher._repo = mock_repo
        return watcher, mock_repo

    @pytest.mark.asyncio
    async def test_new_file_writes_outbox(self):
        watcher, repo = self._make_watcher(insert_processed_returns=True)
        data = b"settlement data"
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(data)
            path = Path(f.name)

        sha256 = hashlib.sha256(data).hexdigest()
        await watcher._write_to_outbox(
            sha256=sha256,
            file_id="udir_test",
            filename=path.name,
            byte_count=len(data),
            path=path,
        )

        repo.insert_processed_file.assert_awaited_once()
        repo.insert_outbox_tx.assert_awaited_once()
        path.unlink()

    @pytest.mark.asyncio
    async def test_duplicate_file_skips_outbox(self):
        watcher, repo = self._make_watcher(insert_processed_returns=False)

        await watcher._write_to_outbox(
            sha256="a" * 64,
            file_id="dup_file",
            filename="dup.txt",
            byte_count=100,
            path=Path("/tmp/does_not_matter.txt"),
        )

        repo.insert_processed_file.assert_awaited_once()
        repo.insert_outbox_tx.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_file_arrived_message_contains_correct_fields(self):
        """Verify the FileArrived message passed to insert_outbox_tx is correct."""
        from recon.messaging import SCHEMA_FILE_ARRIVED, decode

        watcher, repo = self._make_watcher(insert_processed_returns=True)
        data = b"hello"
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(data)
            path = Path(f.name)

        sha256 = hashlib.sha256(data).hexdigest()
        await watcher._write_to_outbox(
            sha256=sha256,
            file_id="udir_2026_04_30",
            filename=path.name,
            byte_count=len(data),
            path=path,
        )

        call_kwargs = repo.insert_outbox_tx.call_args
        payload = call_kwargs.kwargs["payload"]
        msg = decode(SCHEMA_FILE_ARRIVED, payload)

        assert msg.file_id == "udir_2026_04_30"
        assert msg.sha256 == sha256
        assert msg.bytes == len(data)
        path.unlink()
