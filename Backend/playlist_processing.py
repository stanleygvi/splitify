"""Playlist processing orchestration for feature fetch, clustering, and output creation."""

import asyncio
import os
import time
from collections import defaultdict
try:
    from Backend.spotify_api import (
        get_playlist_children,
        create_playlist,
        add_songs,
        get_user_id,
        get_playlist_name,
    )
    from Backend.helpers import calc_slices
    from Backend.grouping import cluster_df
    from Backend.audio_feature_pipeline import get_track_audio_features
    from Backend.cluster_insights import (
        build_cluster_reason,
        build_cluster_trait_summary,
        compute_feature_means,
        small_cluster_is_cohesive,
    )
    from Backend.track_utils import dedupe_track_ids, is_valid_spotify_track_id
except ModuleNotFoundError:
    from spotify_api import (  # type: ignore
        get_playlist_children,
        create_playlist,
        add_songs,
        get_user_id,
        get_playlist_name,
    )
    from helpers import calc_slices  # type: ignore
    from grouping import cluster_df  # type: ignore
    from audio_feature_pipeline import get_track_audio_features  # type: ignore
    from cluster_insights import (  # type: ignore
        build_cluster_reason,
        build_cluster_trait_summary,
        compute_feature_means,
        small_cluster_is_cohesive,
    )
    from track_utils import dedupe_track_ids, is_valid_spotify_track_id  # type: ignore


def _env_positive_int(name: str, default_value: int) -> int:
    """Parse positive integer env values with safe fallback."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default_value
    candidate = raw_value.strip()
    if not candidate:
        return default_value
    try:
        parsed = int(candidate)
    except ValueError:
        return default_value
    return parsed if parsed > 0 else 1


FETCH_PAGE_CONCURRENCY = _env_positive_int("PLAYLIST_FETCH_PAGE_CONCURRENCY", 12)
CREATE_PLAYLIST_CONCURRENCY = _env_positive_int("PLAYLIST_CREATE_CONCURRENCY", 6)
ADD_SONGS_CONCURRENCY = _env_positive_int("PLAYLIST_ADD_CONCURRENCY", 16)

SPLIT_CRITERION_LABELS = {
    "balanced": "Balanced",
    "energy": "Energy",
    "valence": "Mood",
    "danceability": "Danceability",
    "tempo": "Tempo",
    "acousticness": "Acousticness",
    "instrumentalness": "Instrumental",
    "speechiness": "Speechiness",
    "liveness": "Liveness",
    "loudness": "Loudness",
    "custom": "Custom",
}


def log_step_time(step_name, start_time):
    """Print elapsed seconds for a named processing step."""
    elapsed_time = time.time() - start_time
    print(f"{step_name} completed in {elapsed_time:.2f} seconds.")


def _resolve_split_criterion_label(split_criterion: str | None) -> str | None:
    """Return friendly split criterion label for playlist naming and metadata."""
    if not split_criterion:
        return None
    normalized = split_criterion.strip().lower()
    if not normalized:
        return None
    return SPLIT_CRITERION_LABELS.get(normalized, normalized.replace("_", " ").title())


async def get_playlist_track_ids(auth_token, playlist_id):
    """Return all track IDs from the playlist."""
    start_time = time.time()
    track_ids = []
    first_page_response = await get_playlist_children(
        0, playlist_id, auth_token, include_total=True
    )
    total_tracks = (
        first_page_response.get("total", 0)
        if isinstance(first_page_response, dict)
        else 0
    )
    slices = calc_slices(total_tracks)
    responses = [first_page_response] if first_page_response else []
    offsets = list(range(100, slices * 100, 100))
    fetch_concurrency = max(1, min(FETCH_PAGE_CONCURRENCY, len(offsets) or 1))
    semaphore = asyncio.Semaphore(fetch_concurrency)

    async def fetch_page(offset):
        async with semaphore:
            return await get_playlist_children(offset, playlist_id, auth_token)

    if offsets:
        responses.extend(await asyncio.gather(*(fetch_page(offset) for offset in offsets)))
    for response in responses:
        if not response or "items" not in response:
            continue

        for item in response["items"]:
            track = item.get("track") or {}
            track_id = track.get("id")
            if track_id:
                track_ids.append(track_id)

    unique_track_ids, duplicate_count = dedupe_track_ids(track_ids)
    if duplicate_count > 0:
        print(
            f"Removed {duplicate_count} duplicate tracks from source playlist {playlist_id}."
        )
    print(
        "Playlist track fetch stats:",
        f"playlist_id={playlist_id},",
        f"pages={len(responses)},",
        f"tracks={len(unique_track_ids)},",
        f"duration={time.time() - start_time:.2f}s",
    )
    return unique_track_ids


async def create_and_populate_cluster_playlists(
    clustered_tracks,
    feature_by_track_id,
    user_id,
    auth_token,
    playlist_name,
    split_criterion: str | None = None,
):
    """Create playlists for clusters and populate them with grouped tracks."""
    start_time = time.time()
    tracks_by_cluster = defaultdict(list)
    split_criterion_label = _resolve_split_criterion_label(split_criterion)

    for _, row in clustered_tracks.iterrows():
        tracks_by_cluster[int(row["cluster"])].append(row["id"])

    sorted_clusters = sorted(
        tracks_by_cluster.items(), key=lambda item: len(item[1]), reverse=True
    )
    global_means = compute_feature_means(
        list(clustered_tracks["id"]), feature_by_track_id
    )

    cluster_candidates = []
    for cluster_id, cluster_track_ids in sorted_clusters:
        valid_cluster_track_ids = [
            track_id
            for track_id in cluster_track_ids
            if is_valid_spotify_track_id(track_id)
        ]
        valid_cluster_track_ids, _ = dedupe_track_ids(valid_cluster_track_ids)
        if not valid_cluster_track_ids:
            continue
        if len(valid_cluster_track_ids) < 3:
            print(
                f"Skipping cluster {cluster_id} "
                f"({len(valid_cluster_track_ids)} tracks): below minimum size."
            )
            continue
        if len(valid_cluster_track_ids) < 5 and not small_cluster_is_cohesive(
            valid_cluster_track_ids, feature_by_track_id
        ):
            print(
                f"Skipping cluster {cluster_id}: small-cluster cohesion check failed."
            )
            continue

        cluster_means = compute_feature_means(
            valid_cluster_track_ids, feature_by_track_id
        )
        cluster_reason = build_cluster_reason(cluster_means, global_means)
        cluster_trait_summary = build_cluster_trait_summary(
            cluster_means, global_means
        )
        if split_criterion_label:
            playlist_title = f"{playlist_name} [{split_criterion_label}] - {cluster_reason}"
        else:
            playlist_title = f"{playlist_name} - {cluster_reason}"
        if len(playlist_title) > 100:
            playlist_title = playlist_title[:97] + "..."

        criterion_sentence = (
            f"Primary split criterion: {split_criterion_label}. "
            if split_criterion_label
            else ""
        )
        cluster_candidates.append(
            {
                "cluster_id": cluster_id,
                "track_ids": valid_cluster_track_ids,
                "playlist_title": playlist_title,
                "playlist_description": (
                    criterion_sentence
                    + f"Grouped by audio similarity: {cluster_reason}. "
                    f"Trait drivers: {cluster_trait_summary}. "
                    "Made using Splitify: https://splitifytool.com/"
                ),
            }
        )

    if not cluster_candidates:
        print("No eligible clusters to create playlists for.")
        return

    pipeline_start = time.time()
    create_concurrency = max(1, min(CREATE_PLAYLIST_CONCURRENCY, len(cluster_candidates)))
    create_semaphore = asyncio.Semaphore(create_concurrency)
    total_add_jobs = sum(
        calc_slices(len(candidate["track_ids"])) for candidate in cluster_candidates
    )
    add_concurrency = max(1, min(ADD_SONGS_CONCURRENCY, total_add_jobs or 1))
    add_semaphore = asyncio.Semaphore(add_concurrency)

    async def process_cluster(candidate):
        create_duration = 0.0
        add_duration = 0.0
        add_calls = 0
        add_calls_successful = 0
        tracks_added = 0

        async with create_semaphore:
            create_call_start = time.time()
            created_playlist_id = await create_playlist(
                user_id,
                auth_token,
                candidate["playlist_title"],
                candidate["playlist_description"],
            )
            create_duration += time.time() - create_call_start

        if not created_playlist_id:
            return {
                "cluster_created": 0,
                "create_calls": 1,
                "create_time": create_duration,
                "add_calls": add_calls,
                "add_calls_successful": add_calls_successful,
                "tracks_added": tracks_added,
                "add_time": add_duration,
            }

        track_ids = candidate["track_ids"]
        slices = calc_slices(len(track_ids))
        for index in range(0, slices * 100, 100):
            track_slice = track_ids[index : index + 100]
            track_uris = [f"spotify:track:{track_id}" for track_id in track_slice]
            tracks_added += len(track_uris)
            add_calls += 1

            async with add_semaphore:
                # Avoid "Index out of bounds" races by appending without explicit position.
                add_call_start = time.time()
                add_result = await add_songs(created_playlist_id, track_uris, auth_token)
                add_duration += time.time() - add_call_start
            if add_result:
                add_calls_successful += 1

        return {
            "cluster_created": 1,
            "create_calls": 1,
            "create_time": create_duration,
            "add_calls": add_calls,
            "add_calls_successful": add_calls_successful,
            "tracks_added": tracks_added,
            "add_time": add_duration,
        }

    cluster_results = await asyncio.gather(
        *(process_cluster(candidate) for candidate in cluster_candidates)
    )
    clusters_created = sum(result["cluster_created"] for result in cluster_results)
    create_calls = sum(result["create_calls"] for result in cluster_results)
    create_duration = sum(result["create_time"] for result in cluster_results)
    add_calls = sum(result["add_calls"] for result in cluster_results)
    successful_add_calls = sum(
        result["add_calls_successful"] for result in cluster_results
    )
    total_tracks_to_add = sum(result["tracks_added"] for result in cluster_results)
    add_duration = sum(result["add_time"] for result in cluster_results)
    pipeline_duration = time.time() - pipeline_start

    print(
        "Playlist write stats:",
        f"clusters_considered={len(sorted_clusters)},",
        f"clusters_created={clusters_created},",
        f"create_concurrency={create_concurrency},",
        f"add_concurrency={add_concurrency},",
        f"create_calls={create_calls},",
        f"create_time={create_duration:.2f}s,",
        f"add_calls={add_calls},",
        f"add_calls_successful={successful_add_calls},",
        f"tracks_added={total_tracks_to_add},",
        f"add_time={add_duration:.2f}s,",
        f"pipeline_time={pipeline_duration:.2f}s",
    )

    log_step_time("Creating and populating cluster playlists", start_time)


async def process_single_playlist(
    auth_token,
    playlist_id,
    user_id,
    feature_weights: dict[str, float] | None = None,
    split_criterion: str | None = None,
):
    """Split one playlist into cluster playlists."""
    start_time = time.time()
    print(f"Processing {playlist_id}...")
    playlist_name_start = time.time()
    playlist_name_task = asyncio.create_task(
        asyncio.to_thread(get_playlist_name, playlist_id, auth_token)
    )

    track_fetch_start = time.time()
    track_ids = await get_playlist_track_ids(auth_token, playlist_id)
    log_step_time(f"Collect playlist tracks ({playlist_id})", track_fetch_start)
    playlist_name = await playlist_name_task
    log_step_time(
        f"Fetch source playlist name ({playlist_id})",
        playlist_name_start,
    )
    if not track_ids:
        print(f"No tracks found for playlist {playlist_id}")
        return {
            "playlist_id": playlist_id,
            "playlist_name": playlist_name,
            "result": "skipped_no_tracks",
        }

    feature_fetch_start = time.time()
    audio_features, _reccobeats_diagnostics = await get_track_audio_features(
        track_ids, auth_token
    )
    log_step_time(f"Resolve audio features ({playlist_id})", feature_fetch_start)
    if not audio_features:
        print(f"No audio features available for playlist {playlist_id}")
        return {
            "playlist_id": playlist_id,
            "playlist_name": playlist_name,
            "result": "skipped_no_audio_features",
        }

    feature_by_track_id = {
        row["id"]: row for row in audio_features if isinstance(row, dict) and row.get("id")
    }
    if len(feature_by_track_id) < len(track_ids):
        removed = len(track_ids) - len(feature_by_track_id)
        print(f"Dropped {removed} tracks without usable unique audio features.")
    cluster_start = time.time()
    clustered_tracks = await asyncio.to_thread(
        cluster_df, audio_features, feature_weights
    )
    log_step_time(f"Cluster tracks ({playlist_id})", cluster_start)
    if clustered_tracks.empty:
        print(f"Failed to cluster tracks for playlist {playlist_id}")
        return {
            "playlist_id": playlist_id,
            "playlist_name": playlist_name,
            "result": "failed_clustering",
        }
    cluster_sizes = clustered_tracks["cluster"].value_counts().tolist()
    print(
        "Cluster distribution stats:",
        f"clusters={len(cluster_sizes)},",
        f"largest={max(cluster_sizes) if cluster_sizes else 0},",
        f"smallest={min(cluster_sizes) if cluster_sizes else 0}",
    )

    playlist_write_start = time.time()
    await create_and_populate_cluster_playlists(
        clustered_tracks,
        feature_by_track_id,
        user_id,
        auth_token,
        playlist_name,
        split_criterion=split_criterion,
    )
    log_step_time(
        f"Write clustered playlists ({playlist_id})",
        playlist_write_start,
    )

    log_step_time(f"Processing playlist {playlist_id}", start_time)
    return {
        "playlist_id": playlist_id,
        "playlist_name": playlist_name,
        "result": "succeeded",
    }


async def process_playlists(
    auth_token,
    playlist_ids,
    feature_weights: dict[str, float] | None = None,
    split_criterion: str | None = None,
    progress_callback=None,
):
    """Process multiple playlists by splitting with K-means clustering."""
    start_time = time.time()
    print(f"Processing {len(playlist_ids)} playlists...")
    user_id = get_user_id(auth_token)
    total_playlists = len(playlist_ids)
    completed_playlists = 0
    failed_playlists = 0

    def emit_progress(
        last_completed_playlist_id: str | None = None,
        last_completed_playlist_name: str | None = None,
    ):
        if progress_callback is None:
            return
        progress_callback(
            completed_playlists=completed_playlists,
            total_playlists=total_playlists,
            failed_playlists=failed_playlists,
            last_completed_playlist_id=last_completed_playlist_id,
            last_completed_playlist_name=last_completed_playlist_name,
        )

    emit_progress()

    tasks = [
        asyncio.create_task(
            process_single_playlist(
                auth_token,
                playlist_id,
                user_id,
                feature_weights=feature_weights,
                split_criterion=split_criterion,
            )
        )
        for playlist_id in playlist_ids
    ]
    for task in asyncio.as_completed(tasks):
        last_completed_playlist_id = None
        last_completed_playlist_name = None
        try:
            playlist_result = await task
            if isinstance(playlist_result, dict):
                last_completed_playlist_id = playlist_result.get("playlist_id")
                last_completed_playlist_name = playlist_result.get("playlist_name")
        except Exception as error:  # pylint: disable=broad-exception-caught
            failed_playlists += 1
            print(f"Playlist processing task failed: {error}")
        finally:
            completed_playlists += 1
            emit_progress(
                last_completed_playlist_id=last_completed_playlist_id,
                last_completed_playlist_name=last_completed_playlist_name,
            )

    if failed_playlists > 0:
        raise RuntimeError(
            f"{failed_playlists} playlist(s) failed during processing."
        )

    log_step_time("Processing all playlists", start_time)


def process_all(
    auth_token,
    playlist_ids,
    feature_weights: dict[str, float] | None = None,
    split_criterion: str | None = None,
    progress_callback=None,
):
    """Run async playlist processing entrypoint from sync Flask handler."""
    asyncio.run(
        process_playlists(
            auth_token,
            playlist_ids,
            feature_weights=feature_weights,
            split_criterion=split_criterion,
            progress_callback=progress_callback,
        )
    )
