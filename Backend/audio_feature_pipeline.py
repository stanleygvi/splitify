"""Audio-feature retrieval pipeline with ReccoBeats + Spotify fallback."""

import asyncio
import time

try:
    from Backend.spotify_api import (
        get_reccobeats_audio_features_batch,
        get_track_metadata_map,
        search_track_ids_by_name_artist,
    )
    from Backend.track_utils import dedupe_track_ids, is_valid_spotify_track_id
    from Backend.track_cache import (
        get_cached_track_features,
        cache_track_features,
        cache_known_misses,
    )
except ModuleNotFoundError:
    from spotify_api import (  # type: ignore
        get_reccobeats_audio_features_batch,
        get_track_metadata_map,
        search_track_ids_by_name_artist,
    )
    from track_utils import dedupe_track_ids, is_valid_spotify_track_id  # type: ignore
    from track_cache import (  # type: ignore
        get_cached_track_features,
        cache_track_features,
        cache_known_misses,
    )

FALLBACK_SEARCH_LIMIT = 3
FALLBACK_SEARCH_CONCURRENCY = 6


def log_step_time(step_name, start_time):
    """Print elapsed seconds for a named processing step."""
    elapsed_time = time.time() - start_time
    print(f"{step_name} completed in {elapsed_time:.2f} seconds.")


async def _search_candidates_for_query(
    query_key, auth_token, semaphore, query_cache
):
    """Run one Spotify search for a (track_name, artist_name) query with caching."""
    if query_key in query_cache:
        return query_key, query_cache[query_key]

    track_name, artist_name = query_key
    async with semaphore:
        candidates = await asyncio.to_thread(
            search_track_ids_by_name_artist,
            track_name,
            artist_name,
            auth_token,
            FALLBACK_SEARCH_LIMIT,
        )
    query_cache[query_key] = candidates
    return query_key, candidates


async def get_track_audio_features(track_ids, auth_token):
    """Fetch audio features from ReccoBeats, then try Spotify-search fallback for misses."""
    start_time = time.time()
    unique_track_ids, _ = dedupe_track_ids(track_ids)
    cached_features_by_id, cached_misses_by_id = await asyncio.to_thread(
        get_cached_track_features, unique_track_ids
    )
    track_ids_to_fetch = [
        track_id
        for track_id in unique_track_ids
        if track_id not in cached_features_by_id and track_id not in cached_misses_by_id
    ]

    features = list(cached_features_by_id.values())
    diagnostics = {
        track_id: "ok" for track_id in cached_features_by_id
    } | cached_misses_by_id
    summary = {
        "requested": len(track_ids_to_fetch),
        "matched": 0,
        "not_returned": 0,
        "invalid_response_item_count": 0,
        "unexpected_id_count": 0,
    }

    if track_ids_to_fetch:
        fetched_features, fetched_diagnostics, fetched_summary = await asyncio.to_thread(
            get_reccobeats_audio_features_batch, track_ids_to_fetch, 40, True
        )
        features.extend(fetched_features)
        diagnostics.update(fetched_diagnostics)
        summary = fetched_summary
        await asyncio.to_thread(cache_track_features, fetched_features, "reccobeats")

    print(
        "ReccoBeats diagnostics:",
        f"requested={summary['requested']},",
        f"matched={summary['matched']},",
        f"not_returned={summary['not_returned']},",
        f"invalid_items={summary['invalid_response_item_count']},",
        f"unexpected_ids={summary['unexpected_id_count']},",
        f"cache_hits={len(cached_features_by_id)},",
        f"known_miss_cache_hits={len(cached_misses_by_id)}",
    )

    missing_track_ids = [
        track_id
        for track_id in track_ids_to_fetch
        if diagnostics.get(track_id) != "ok"
    ]
    resolved_count = 0

    if missing_track_ids:
        metadata_map = await asyncio.to_thread(
            get_track_metadata_map, missing_track_ids, auth_token
        )
        candidate_ids_by_missing = {}
        query_keys_by_missing = {}
        query_cache = {}

        for missing_track_id in missing_track_ids:
            metadata = metadata_map.get(missing_track_id)
            if not metadata:
                diagnostics[missing_track_id] = "missing_spotify_metadata"
                continue

            track_name = metadata.get("name", "")
            artists = metadata.get("artists", [])
            primary_artist = artists[0] if artists else ""
            if not track_name or not primary_artist:
                diagnostics[missing_track_id] = "insufficient_track_metadata"
                continue

            query_key = (track_name.strip().lower(), primary_artist.strip().lower())
            query_keys_by_missing[missing_track_id] = query_key

        unique_query_keys = sorted(set(query_keys_by_missing.values()))
        if unique_query_keys:
            semaphore = asyncio.Semaphore(FALLBACK_SEARCH_CONCURRENCY)
            search_results = await asyncio.gather(
                *[
                    _search_candidates_for_query(
                        query_key, auth_token, semaphore, query_cache
                    )
                    for query_key in unique_query_keys
                ]
            )
            query_cache.update(dict(search_results))

        for missing_track_id, query_key in query_keys_by_missing.items():
            candidates = query_cache.get(query_key, [])
            filtered_candidates = [
                candidate
                for candidate in candidates
                if candidate != missing_track_id and is_valid_spotify_track_id(candidate)
            ]
            if not filtered_candidates:
                diagnostics[missing_track_id] = "spotify_search_no_candidates"
                continue
            candidate_ids_by_missing[missing_track_id] = filtered_candidates

        fallback_candidate_ids = dedupe_track_ids(
            [
                candidate
                for candidates in candidate_ids_by_missing.values()
                for candidate in candidates
            ]
        )[0]
        if fallback_candidate_ids:
            fallback_features = await asyncio.to_thread(
                get_reccobeats_audio_features_batch, fallback_candidate_ids, 40, False
            )
            fallback_features_by_id = {
                row["id"]: row
                for row in fallback_features
                if isinstance(row, dict) and row.get("id")
            }

            for missing_track_id, candidates in candidate_ids_by_missing.items():
                replacement = None
                for candidate_id in candidates:
                    replacement = fallback_features_by_id.get(candidate_id)
                    if replacement:
                        break

                if replacement:
                    normalized = dict(replacement)
                    normalized["id"] = missing_track_id
                    features.append(normalized)
                    diagnostics[missing_track_id] = "ok"
                    resolved_count += 1
                elif diagnostics.get(missing_track_id) != "ok":
                    diagnostics[missing_track_id] = "fallback_candidates_not_in_reccobeats"

    unresolved_misses = {
        track_id: reason
        for track_id, reason in diagnostics.items()
        if reason != "ok"
    }
    if unresolved_misses:
        await asyncio.to_thread(cache_known_misses, unresolved_misses)

    features_by_id = {
        row["id"]: row for row in features if isinstance(row, dict) and row.get("id")
    }
    features = list(features_by_id.values())
    if features:
        await asyncio.to_thread(cache_track_features, features, "pipeline_result")

    if resolved_count > 0:
        print(f"Fallback resolved {resolved_count} missing tracks via Spotify search.")

    log_step_time("Fetching audio features", start_time)
    return features, diagnostics
