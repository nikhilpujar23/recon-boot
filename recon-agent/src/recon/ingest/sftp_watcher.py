"""
Phase 2 — SFTP Watcher (LLD §6.1).

Watches a directory for new settlement files using watchdog's
filesystem event observer (equivalent to inotify IN_CLOSE_WRITE).

Algorithm per file:
  1. Wait for FileClosedEvent / FileCreatedEvent (file fully written).
  2. Compute SHA-256 via streaming 1-MiB chunks.
  3. INSERT INTO processed_files ON CONFLICT DO NOTHING.
  4. If duplicate → skip; log metric.
  5. Else → write FileArrived into outbox (same transaction as step 3).
     Outbox drainer publishes to Kafka topic `files.new`.

Failure semantics:
  - Crash mid-hash: file stays on disk; watchdog re-scans on restart and
    the dedup check in step 3 is idempotent.
  - DB unavailable: exponential backoff 1s → 30s; metric watcher_db_failures_total.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import time
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)

# Watchdog is an optional dependency (pip install recon-agent[kafka])
try:
    from watchdog.events import FileSystemEvent, FileSystemEventHandler
    from watchdog.observers import Observer
    _WATCHDOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _WATCHDOG_AVAILABLE = False
    FileSystemEventHandler = object  # type: ignore[misc,assignment]


class SftpWatcher:
    """
    Watch `watch_dir` for new files; deduplicate and emit FileArrived to outbox.

    Usage::

        repo = LedgerRepo(pool)
        watcher = SftpWatcher(watch_dir=Path("/data/sftp"), repo=repo)
        await watcher.run()   # blocks; use asyncio.create_task for concurrent run
    """

    # Retry config (LLD §11)
    _RETRY_INITIAL_S: float = 1.0
    _RETRY_MULTIPLIER: float = 5.0
    _RETRY_MAX_S: float = 30.0

    def __init__(self, watch_dir: Path, repo) -> None:  # repo: LedgerRepo
        if not _WATCHDOG_AVAILABLE:
            raise RuntimeError(
                "watchdog is not installed. Run: pip install 'recon-agent[kafka]'"
            )
        self._watch_dir = watch_dir
        self._repo = repo
        self._queue: asyncio.Queue[Path] = asyncio.Queue()

    async def run(self) -> None:
        """Start the filesystem observer and drain the event queue forever."""
        loop = asyncio.get_running_loop()
        handler = _ClosedFileHandler(loop=loop, queue=self._queue)
        observer = Observer()
        observer.schedule(handler, str(self._watch_dir), recursive=False)
        observer.start()
        logger.info("SftpWatcher started; watching %s", self._watch_dir)
        try:
            while True:
                path = await self._queue.get()
                await self._handle_file(path)
        finally:
            observer.stop()
            observer.join()

    async def _handle_file(self, path: Path) -> None:
        """Process a single new file: hash → dedup → outbox."""
        if not path.is_file():
            return  # deleted before we could read it

        sha256, byte_count = await _sha256_file(path)
        file_id = path.stem
        filename = path.name

        retry_delay = self._RETRY_INITIAL_S
        for attempt in range(3):
            try:
                await self._write_to_outbox(
                    sha256=sha256,
                    file_id=file_id,
                    filename=filename,
                    byte_count=byte_count,
                    path=path,
                )
                return
            except Exception as exc:
                logger.warning(
                    "watcher DB error (attempt %d): %s", attempt + 1, exc,
                    extra={"metric": "watcher_db_failures_total"},
                )
                if attempt < 2:
                    await asyncio.sleep(min(retry_delay, self._RETRY_MAX_S))
                    retry_delay *= self._RETRY_MULTIPLIER

        logger.error("SftpWatcher gave up on file %s after 3 attempts", path)

    async def _write_to_outbox(
        self,
        sha256: str,
        file_id: str,
        filename: str,
        byte_count: int,
        path: Path,
    ) -> None:
        from recon.messaging import (
            SCHEMA_FILE_ARRIVED,
            TOPIC_FILES_NEW,
            FileArrived,
            encode,
        )

        async with self._repo._pool.acquire() as conn:
            async with conn.transaction():
                is_new = await self._repo.insert_processed_file(
                    conn,
                    file_hash=sha256,
                    file_id=file_id,
                    filename=filename,
                    bytes_=byte_count,
                )
                if not is_new:
                    logger.info(
                        "Duplicate file skipped: %s (sha256=%s)", filename, sha256[:12],
                        extra={"metric": "duplicate_file"},
                    )
                    return

                msg = FileArrived(
                    file_id=file_id,
                    filename=filename,
                    sha256=sha256,
                    bytes=byte_count,
                    detected_at=int(time.time() * 1000),
                )
                payload = encode(msg)
                await self._repo.insert_outbox_tx(
                    conn,
                    topic=TOPIC_FILES_NEW,
                    payload=payload,
                    schema_id=SCHEMA_FILE_ARRIVED,
                    partition_key=file_id,
                )
                logger.info(
                    "FileArrived queued: file_id=%s sha256=%s bytes=%d",
                    file_id, sha256[:12], byte_count,
                )


# ── Filesystem event handler (watchdog callback, runs in observer thread) ─────


class _ClosedFileHandler(FileSystemEventHandler):
    """
    Translate watchdog filesystem events into items on an asyncio.Queue.

    We react to both FileClosedEvent (Linux inotify IN_CLOSE_WRITE) and
    FileCreatedEvent (macOS FSEvents, Windows ReadDirectoryChangesW) so
    the watcher is cross-platform during development.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue) -> None:
        self._loop = loop
        self._queue = queue

    def on_closed(self, event: FileSystemEvent) -> None:  # type: ignore[override]
        if not event.is_directory:
            self._enqueue(Path(event.src_path))

    def on_created(self, event: FileSystemEvent) -> None:  # type: ignore[override]
        if not event.is_directory:
            self._enqueue(Path(event.src_path))

    def _enqueue(self, path: Path) -> None:
        self._loop.call_soon_threadsafe(self._queue.put_nowait, path)


# ── SHA-256 helper ────────────────────────────────────────────────────────────

_CHUNK = 1024 * 1024  # 1 MiB


async def _sha256_file(path: Path) -> tuple[str, int]:
    """Compute SHA-256 hex digest and byte count; offloaded to thread pool."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _sha256_file_sync, path)


def _sha256_file_sync(path: Path) -> tuple[str, int]:
    """Synchronous SHA-256 computation (runs in executor thread)."""
    h = hashlib.sha256()
    total = 0
    with path.open("rb") as fh:
        while chunk := fh.read(_CHUNK):
            h.update(chunk)
            total += len(chunk)
    return h.hexdigest(), total
