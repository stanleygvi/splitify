"""Postgres-backed shared cache for track audio features and known misses."""

import json
import os
import re
import threading
import time
from typing import Any

SPOTIFY_TRACK_ID_PATTERN = re.compile(r"^[A-Za-z0-9]{22}$")
MIN_TTL_SECONDS = 60
MAX_TTL_SECONDS = 365 * 24 * 60 * 60
DEFAULT_POOL_MIN_CONN = 1
DEFAULT_POOL_MAX_CONN = 8


def _parse_ttl_seconds(env_name: str, default_value: int) -> int:
    """Parse TTL env var safely and clamp to sane bounds."""
    raw_value = os.getenv(env_name, str(default_value)).strip()
    try:
        parsed = int(raw_value)
    except ValueError:
        parsed = default_value
    return max(MIN_TTL_SECONDS, min(parsed, MAX_TTL_SECONDS))


def _database_url() -> str:
    """Return validated DATABASE_URL."""
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        return ""
    if "\x00" in db_url:
        raise ValueError("Invalid DATABASE_URL")
    return db_url


def _parse_pool_size(env_name: str, default_value: int) -> int:
    """Parse positive pool size setting with fallback."""
    raw_value = os.getenv(env_name, str(default_value)).strip()
    if not raw_value.lstrip("-").isdigit():
        return default_value
    parsed = int(raw_value)
    return max(1, parsed)


CACHE_TTL_SECONDS = _parse_ttl_seconds(
    "TRACK_CACHE_TTL_SECONDS", 30 * 24 * 60 * 60
)
MISS_TTL_SECONDS = _parse_ttl_seconds(
    "TRACK_CACHE_MISS_TTL_SECONDS", 7 * 24 * 60 * 60
)
POOL_MIN_CONN = _parse_pool_size("TRACK_CACHE_DB_POOL_MIN", DEFAULT_POOL_MIN_CONN)
POOL_MAX_CONN = _parse_pool_size("TRACK_CACHE_DB_POOL_MAX", DEFAULT_POOL_MAX_CONN)

_POOL = None
_POOL_LOCK = threading.Lock()
_SCHEMA_READY = False
_SCHEMA_LOCK = threading.Lock()


def _cache_enabled() -> bool:
    value = os.getenv("TRACK_CACHE_ENABLED", "true").strip().lower()
    return value in {"1", "true", "yes", "on"} and bool(_database_url())


def _is_valid_track_id(track_id: str | None) -> bool:
    """Return whether a track ID matches Spotify base62 format."""
    return bool(track_id) and bool(SPOTIFY_TRACK_ID_PATTERN.fullmatch(track_id))


def _pool_bounds() -> tuple[int, int]:
    """Return valid min/max pool bounds."""
    min_conn = max(1, POOL_MIN_CONN)
    max_conn = max(min_conn, POOL_MAX_CONN)
    return min_conn, max_conn


def _load_pool_class():
    """Load psycopg2 pooled connection class lazily."""
    pool_module = __import__("psycopg2.pool", fromlist=["ThreadedConnectionPool"])
    return pool_module.ThreadedConnectionPool


def _load_execute_values():
    """Load psycopg2 execute_values helper lazily."""
    extras_module = __import__("psycopg2.extras", fromlist=["execute_values"])
    return extras_module.execute_values


def _get_pool():
    """Create (or return) pooled DB connector."""
    if not _cache_enabled():
        return None
    global _POOL  # pylint: disable=global-statement
    if _POOL is not None:
        return _POOL

    with _POOL_LOCK:
        if _POOL is not None:
            return _POOL
        pool_class = _load_pool_class()
        min_conn, max_conn = _pool_bounds()
        _POOL = pool_class(
            min_conn,
            max_conn,
            _database_url(),
            connect_timeout=10,
        )
    return _POOL


def _acquire_connection():
    """Acquire connection from pool."""
    pool = _get_pool()
    if pool is None:
        return None, None
    return pool, pool.getconn()


def _release_connection(pool, connection):
    """Return connection to pool."""
    if pool is None or connection is None:
        return
    pool.putconn(connection)


def _ensure_schema() -> None:
    """Create cache table if needed."""
    global _SCHEMA_READY  # pylint: disable=global-statement
    if not _cache_enabled():
        return
    if _SCHEMA_READY:
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
                        """
                        CREATE TABLE IF NOT EXISTS track_audio_cache (
                            track_id TEXT PRIMARY KEY,
                            payload_json TEXT,
                            known_missing BOOLEAN NOT NULL DEFAULT FALSE,
                            miss_reason TEXT,
                            source TEXT,
                            updated_at BIGINT NOT NULL
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_track_audio_cache_updated_at
                        ON track_audio_cache (updated_at)
                        """
                    )
            _SCHEMA_READY = True
        finally:
            _release_connection(pool, connection)


def get_cached_track_features(
    track_ids: list[str],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Return cached features and known-miss reasons for provided track IDs."""
    if not _cache_enabled() or not track_ids:
        return {}, {}
    valid_track_ids = [
        track_id for track_id in track_ids if _is_valid_track_id(track_id)
    ]
    if not valid_track_ids:
        return {}, {}

    _ensure_schema()
    now = int(time.time())
    min_feature_updated_at = now - CACHE_TTL_SECONDS
    min_miss_updated_at = now - MISS_TTL_SECONDS

    features_by_track_id: dict[str, dict[str, Any]] = {}
    misses_by_track_id: dict[str, str] = {}

    pool, connection = _acquire_connection()
    if connection is None:
        return {}, {}
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT track_id, payload_json, known_missing, miss_reason
                FROM track_audio_cache
                WHERE track_id = ANY(%s)
                AND (
                    (known_missing = FALSE AND payload_json IS NOT NULL AND updated_at >= %s)
                    OR
                    (known_missing = TRUE AND updated_at >= %s)
                )
                """,
                (valid_track_ids, min_feature_updated_at, min_miss_updated_at),
            )
            rows = cursor.fetchall()
            for track_id, payload_json, known_missing, miss_reason in rows:
                if known_missing:
                    misses_by_track_id[str(track_id)] = (
                        str(miss_reason) if miss_reason else "known_missing_cached"
                    )
                    continue

                if not payload_json:
                    continue
                try:
                    payload = json.loads(payload_json)
                except (TypeError, json.JSONDecodeError):
                    continue
                if isinstance(payload, dict):
                    payload["id"] = str(track_id)
                    features_by_track_id[str(track_id)] = payload
    finally:
        _release_connection(pool, connection)

    return features_by_track_id, misses_by_track_id


def cache_track_features(
    features: list[dict[str, Any]], source: str = "reccobeats"
) -> None:
    """Upsert resolved track features into cache."""
    if not _cache_enabled() or not features:
        return

    _ensure_schema()
    now = int(time.time())
    rows: list[tuple[str, str, bool, str, str, int]] = []
    for feature in features:
        track_id = feature.get("id")
        if not _is_valid_track_id(track_id):
            continue
        payload = dict(feature)
        payload["id"] = str(track_id)
        rows.append(
            (
                str(track_id),
                json.dumps(payload, separators=(",", ":")),
                False,
                "",
                source,
                now,
            )
        )
    if not rows:
        return

    pool, connection = _acquire_connection()
    if connection is None:
        return
    try:
        execute_values = _load_execute_values()
        with connection:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    """
                    INSERT INTO track_audio_cache
                        (track_id, payload_json, known_missing, miss_reason, source, updated_at)
                    VALUES %s
                    ON CONFLICT(track_id) DO UPDATE SET
                        payload_json=excluded.payload_json,
                        known_missing=FALSE,
                        miss_reason='',
                        source=excluded.source,
                        updated_at=excluded.updated_at
                    """,
                    rows,
                    template="(%s, %s, %s, %s, %s, %s)",
                    page_size=500,
                )
    finally:
        _release_connection(pool, connection)


def cache_known_misses(miss_reasons_by_track_id: dict[str, str]) -> None:
    """Upsert known misses so future runs can skip repeated failed lookups."""
    if not _cache_enabled() or not miss_reasons_by_track_id:
        return

    _ensure_schema()
    now = int(time.time())
    rows = [
        (track_id, None, True, reason, "known_miss", now)
        for track_id, reason in miss_reasons_by_track_id.items()
        if _is_valid_track_id(track_id)
    ]
    if not rows:
        return

    pool, connection = _acquire_connection()
    if connection is None:
        return
    try:
        execute_values = _load_execute_values()
        with connection:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    """
                    INSERT INTO track_audio_cache
                        (track_id, payload_json, known_missing, miss_reason, source, updated_at)
                    VALUES %s
                    ON CONFLICT(track_id) DO UPDATE SET
                        payload_json=NULL,
                        known_missing=TRUE,
                        miss_reason=excluded.miss_reason,
                        source=excluded.source,
                        updated_at=excluded.updated_at
                    """,
                    rows,
                    template="(%s, %s, %s, %s, %s, %s)",
                    page_size=500,
                )
    finally:
        _release_connection(pool, connection)
