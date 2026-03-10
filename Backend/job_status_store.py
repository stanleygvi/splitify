"""Shared job-status persistence for async playlist processing jobs."""

import json
import os
import threading
import time
from typing import Any

DEFAULT_POOL_MIN_CONN = 1
DEFAULT_POOL_MAX_CONN = 4
JOB_STATUS_TABLE_NAME = "playlist_processing_job_status"

_POOL = None
_POOL_LOCK = threading.Lock()
_SCHEMA_READY = False
_SCHEMA_LOCK = threading.Lock()

_MEMORY_JOBS: dict[str, dict[str, Any]] = {}
_MEMORY_JOBS_LOCK = threading.Lock()


def _parse_positive_int(name: str, default_value: int) -> int:
    """Parse positive integer env values with fallback."""
    raw_value = os.getenv(name, str(default_value)).strip()
    if not raw_value.lstrip("-").isdigit():
        return default_value
    parsed = int(raw_value)
    return parsed if parsed > 0 else 1


JOB_STATUS_DB_POOL_MIN = _parse_positive_int(
    "JOB_STATUS_DB_POOL_MIN", DEFAULT_POOL_MIN_CONN
)
JOB_STATUS_DB_POOL_MAX = _parse_positive_int(
    "JOB_STATUS_DB_POOL_MAX", DEFAULT_POOL_MAX_CONN
)


def _database_url() -> str:
    """Return validated DATABASE_URL, if set."""
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        return ""
    if "\x00" in db_url:
        raise ValueError("Invalid DATABASE_URL")
    return db_url


def _db_enabled() -> bool:
    """Return whether DB-backed shared status storage is enabled."""
    enabled_text = os.getenv("JOB_STATUS_DB_ENABLED", "true").strip().lower()
    return enabled_text in {"1", "true", "yes", "on"} and bool(_database_url())


def _pool_bounds() -> tuple[int, int]:
    """Return valid min/max connection pool bounds."""
    min_conn = max(1, JOB_STATUS_DB_POOL_MIN)
    max_conn = max(min_conn, JOB_STATUS_DB_POOL_MAX)
    return min_conn, max_conn


def _load_pool_class():
    """Load psycopg2 threaded connection pool lazily."""
    pool_module = __import__("psycopg2.pool", fromlist=["ThreadedConnectionPool"])
    return pool_module.ThreadedConnectionPool


def _get_pool():
    """Create (or return) pooled DB connector."""
    if not _db_enabled():
        return None

    global _POOL  # pylint: disable=global-statement
    if _POOL is not None:
        return _POOL

    with _POOL_LOCK:
        if _POOL is not None:
            return _POOL
        min_conn, max_conn = _pool_bounds()
        pool_class = _load_pool_class()
        _POOL = pool_class(
            min_conn,
            max_conn,
            _database_url(),
            connect_timeout=10,
        )
    return _POOL


def _acquire_connection():
    """Acquire one connection from DB pool."""
    pool = _get_pool()
    if pool is None:
        return None, None
    return pool, pool.getconn()


def _release_connection(pool, connection) -> None:
    """Return connection to DB pool."""
    if pool is None or connection is None:
        return
    pool.putconn(connection)


def _ensure_schema() -> None:
    """Create DB table/indexes for shared job-status storage."""
    global _SCHEMA_READY  # pylint: disable=global-statement
    if not _db_enabled() or _SCHEMA_READY:
        return

    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        pool, connection = _acquire_connection()
        if connection is None:
            return
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {JOB_STATUS_TABLE_NAME} (
                            job_id TEXT PRIMARY KEY,
                            payload_json TEXT NOT NULL,
                            finished_at DOUBLE PRECISION,
                            updated_at DOUBLE PRECISION NOT NULL
                        )
                        """
                    )
                    cursor.execute(
                        f"""
                        CREATE INDEX IF NOT EXISTS idx_{JOB_STATUS_TABLE_NAME}_finished_at
                        ON {JOB_STATUS_TABLE_NAME} (finished_at)
                        """
                    )
            _SCHEMA_READY = True
        finally:
            _release_connection(pool, connection)


def _set_memory_job_state(job_id: str, **fields) -> None:
    """Set job state in process-local fallback store."""
    with _MEMORY_JOBS_LOCK:
        payload = _MEMORY_JOBS.get(job_id, {})
        payload.update(fields)
        _MEMORY_JOBS[job_id] = payload


def _get_memory_job_state(job_id: str) -> dict[str, Any] | None:
    """Read job state from process-local fallback store."""
    with _MEMORY_JOBS_LOCK:
        payload = _MEMORY_JOBS.get(job_id)
        if payload is None:
            return None
        return dict(payload)


def _prune_memory_jobs(cutoff: float) -> int:
    """Prune old completed jobs from process-local fallback store."""
    removed = 0
    with _MEMORY_JOBS_LOCK:
        old_job_ids = []
        for job_id, payload in _MEMORY_JOBS.items():
            finished_at = payload.get("finished_at")
            if isinstance(finished_at, (int, float)) and float(finished_at) < cutoff:
                old_job_ids.append(job_id)
        for job_id in old_job_ids:
            _MEMORY_JOBS.pop(job_id, None)
            removed += 1
    return removed


def _finished_at_from_fields(fields: dict[str, Any]) -> float | None:
    """Normalize finished_at field value when present."""
    if "finished_at" not in fields:
        return None
    finished_at = fields.get("finished_at")
    if finished_at is None:
        return None
    if isinstance(finished_at, (int, float)):
        return float(finished_at)
    return None


def set_job_state(job_id: str, **fields) -> None:
    """Upsert shared job state for a single job ID."""
    if not job_id:
        return

    if not _db_enabled():
        _set_memory_job_state(job_id, **fields)
        return

    _ensure_schema()
    pool, connection = _acquire_connection()
    if connection is None:
        _set_memory_job_state(job_id, **fields)
        return
    try:
        now = float(time.time())
        payload_json = json.dumps(fields, separators=(",", ":"))
        finished_at = _finished_at_from_fields(fields)
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {JOB_STATUS_TABLE_NAME}
                        (job_id, payload_json, finished_at, updated_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT(job_id) DO UPDATE SET
                        payload_json=(
                            (
                                COALESCE({JOB_STATUS_TABLE_NAME}.payload_json, '{{}}')::jsonb
                                || EXCLUDED.payload_json::jsonb
                            )::text
                        ),
                        finished_at=(
                            CASE
                                WHEN EXCLUDED.payload_json::jsonb ? 'finished_at'
                                THEN EXCLUDED.finished_at
                                ELSE {JOB_STATUS_TABLE_NAME}.finished_at
                            END
                        ),
                        updated_at=EXCLUDED.updated_at
                    """,
                    (job_id, payload_json, finished_at, now),
                )
    except Exception as error:  # pylint: disable=broad-exception-caught
        print(f"Job status DB write failed, using in-memory fallback: {error}")
        _set_memory_job_state(job_id, **fields)
    finally:
        _release_connection(pool, connection)


def get_job_state(job_id: str) -> dict[str, Any] | None:
    """Return shared job status payload for one job ID."""
    if not job_id:
        return None

    if not _db_enabled():
        return _get_memory_job_state(job_id)

    _ensure_schema()
    pool, connection = _acquire_connection()
    if connection is None:
        return _get_memory_job_state(job_id)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT payload_json
                FROM {JOB_STATUS_TABLE_NAME}
                WHERE job_id = %s
                """,
                (job_id,),
            )
            row = cursor.fetchone()
        if not row:
            return _get_memory_job_state(job_id)

        payload_json = row[0]
        if not payload_json:
            return None
        payload = json.loads(payload_json)
        if isinstance(payload, dict):
            return payload
        return None
    except (TypeError, json.JSONDecodeError) as error:
        print(f"Job status DB payload decode failed: {error}")
        return None
    except Exception as error:  # pylint: disable=broad-exception-caught
        print(f"Job status DB read failed, using in-memory fallback: {error}")
        return _get_memory_job_state(job_id)
    finally:
        _release_connection(pool, connection)


def prune_finished_jobs_older_than(cutoff: float) -> int:
    """Delete completed jobs older than cutoff timestamp."""
    if not _db_enabled():
        return _prune_memory_jobs(cutoff)

    _ensure_schema()
    pool, connection = _acquire_connection()
    if connection is None:
        return _prune_memory_jobs(cutoff)
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    DELETE FROM {JOB_STATUS_TABLE_NAME}
                    WHERE finished_at IS NOT NULL
                    AND finished_at < %s
                    """,
                    (cutoff,),
                )
                deleted = cursor.rowcount
        return int(deleted)
    except Exception as error:  # pylint: disable=broad-exception-caught
        print(f"Job status DB prune failed, using in-memory fallback: {error}")
        return _prune_memory_jobs(cutoff)
    finally:
        _release_connection(pool, connection)
