"""Postgres-backed shared cache for track audio features and known misses."""

import json
import os
import re
import time
from typing import Any

SPOTIFY_TRACK_ID_PATTERN = re.compile(r"^[A-Za-z0-9]{22}$")
MIN_TTL_SECONDS = 60
MAX_TTL_SECONDS = 365 * 24 * 60 * 60


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


CACHE_TTL_SECONDS = _parse_ttl_seconds(
    "TRACK_CACHE_TTL_SECONDS", 30 * 24 * 60 * 60
)
MISS_TTL_SECONDS = _parse_ttl_seconds(
    "TRACK_CACHE_MISS_TTL_SECONDS", 7 * 24 * 60 * 60
)


def _cache_enabled() -> bool:
    value = os.getenv("TRACK_CACHE_ENABLED", "true").strip().lower()
    return value in {"1", "true", "yes", "on"} and bool(_database_url())


def _is_valid_track_id(track_id: str | None) -> bool:
    """Return whether a track ID matches Spotify base62 format."""
    return bool(track_id) and bool(SPOTIFY_TRACK_ID_PATTERN.fullmatch(track_id))


def _get_connection():
    """Create a Postgres connection from DATABASE_URL."""
    db_url = _database_url()
    if not db_url:
        return None
    psycopg2_module = __import__("psycopg2")
    return psycopg2_module.connect(db_url, connect_timeout=10)


def _ensure_schema() -> None:
    """Create cache table if needed."""
    if not _cache_enabled():
        return
    conn = _get_connection()
    if conn is None:
        return
    with conn:
        with conn.cursor() as cursor:
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
    conn.close()


def _chunk_values(values: list[str], size: int = 500) -> list[list[str]]:
    """Split large ID lists to manageable query batches."""
    return [values[i : i + size] for i in range(0, len(values), size)]


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

    conn = _get_connection()
    if conn is None:
        return {}, {}
    try:
        with conn.cursor() as cursor:
            for chunk in _chunk_values(valid_track_ids):
                placeholders = ",".join(["%s"] * len(chunk))
                query = (
                    "SELECT track_id, payload_json, known_missing, miss_reason, updated_at "
                    f"FROM track_audio_cache WHERE track_id IN ({placeholders})"
                )
                cursor.execute(query, chunk)
                rows = cursor.fetchall()
                for track_id, payload_json, known_missing, miss_reason, updated_at in rows:
                    if known_missing:
                        if updated_at >= min_miss_updated_at:
                            misses_by_track_id[str(track_id)] = (
                                str(miss_reason)
                                if miss_reason
                                else "known_missing_cached"
                            )
                        continue

                    if not payload_json or updated_at < min_feature_updated_at:
                        continue
                    try:
                        payload = json.loads(payload_json)
                        if isinstance(payload, dict):
                            payload["id"] = str(track_id)
                            features_by_track_id[str(track_id)] = payload
                    except (TypeError, json.JSONDecodeError):
                        continue
    finally:
        conn.close()

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

    conn = _get_connection()
    if conn is None:
        return
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO track_audio_cache
                        (track_id, payload_json, known_missing, miss_reason, source, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT(track_id) DO UPDATE SET
                        payload_json=excluded.payload_json,
                        known_missing=FALSE,
                        miss_reason='',
                        source=excluded.source,
                        updated_at=excluded.updated_at
                    """,
                    rows,
                )
    finally:
        conn.close()


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

    conn = _get_connection()
    if conn is None:
        return
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO track_audio_cache
                        (track_id, payload_json, known_missing, miss_reason, source, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT(track_id) DO UPDATE SET
                        payload_json=NULL,
                        known_missing=TRUE,
                        miss_reason=excluded.miss_reason,
                        source=excluded.source,
                        updated_at=excluded.updated_at
                    """,
                    rows,
                )
    finally:
        conn.close()
