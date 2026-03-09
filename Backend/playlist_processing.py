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
    raw_value = os.getenv(name, str(default_value))
    try:
        parsed = int(raw_value)
    except ValueError:
        return default_value
    return max(1, parsed)


FETCH_PAGE_CONCURRENCY = _env_positive_int("PLAYLIST_FETCH_PAGE_CONCURRENCY", 8)
CREATE_PLAYLIST_CONCURRENCY = _env_positive_int("PLAYLIST_CREATE_CONCURRENCY", 4)
ADD_SONGS_CONCURRENCY = _env_positive_int("PLAYLIST_ADD_CONCURRENCY", 12)


def log_step_time(step_name, start_time):
    """Print elapsed seconds for a named processing step."""
    elapsed_time = time.time() - start_time
    print(f"{step_name} completed in {elapsed_time:.2f} seconds.")


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
    semaphore = asyncio.Semaphore(FETCH_PAGE_CONCURRENCY)

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
    clustered_tracks, feature_by_track_id, user_id, auth_token, playlist_name
):
    """Create playlists for clusters and populate them with grouped tracks."""
    start_time = time.time()
    tracks_by_cluster = defaultdict(list)

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
        playlist_title = f"{playlist_name} - {cluster_reason}"
        if len(playlist_title) > 100:
            playlist_title = playlist_title[:97] + "..."

        cluster_candidates.append(
            {
                "cluster_id": cluster_id,
                "track_ids": valid_cluster_track_ids,
                "playlist_title": playlist_title,
                "playlist_description": (
                    f"Grouped by audio similarity: {cluster_reason}. "
                    f"Trait drivers: {cluster_trait_summary}. "
                    "Made using Splitify: https://splitifytool.com/"
                ),
            }
        )

    if not cluster_candidates:
        print("No eligible clusters to create playlists for.")
        return

    create_start = time.time()
    create_semaphore = asyncio.Semaphore(CREATE_PLAYLIST_CONCURRENCY)

    async def create_cluster_playlist(candidate):
        async with create_semaphore:
            created_playlist_id = await create_playlist(
                user_id,
                auth_token,
                candidate["playlist_title"],
                candidate["playlist_description"],
            )
        if not created_playlist_id:
            return None
        return (candidate, created_playlist_id)

    created_entries = await asyncio.gather(
        *(create_cluster_playlist(candidate) for candidate in cluster_candidates)
    )
    created_entries = [entry for entry in created_entries if entry is not None]
    create_duration = time.time() - create_start

    add_jobs = []
    total_tracks_to_add = 0
    for candidate, created_playlist_id in created_entries:
        track_ids = candidate["track_ids"]
        slices = calc_slices(len(track_ids))
        for index in range(0, slices * 100, 100):
            track_slice = track_ids[index : index + 100]
            track_uris = [f"spotify:track:{track_id}" for track_id in track_slice]
            total_tracks_to_add += len(track_uris)
            add_jobs.append((created_playlist_id, track_uris))

    add_duration = 0.0
    successful_add_calls = 0
    if add_jobs:
        add_start = time.time()
        add_semaphore = asyncio.Semaphore(ADD_SONGS_CONCURRENCY)

        async def add_song_batch(job):
            playlist_id, track_uris = job
            async with add_semaphore:
                # Avoid "Index out of bounds" races by appending without explicit position.
                return await add_songs(playlist_id, track_uris, auth_token)

        add_results = await asyncio.gather(*(add_song_batch(job) for job in add_jobs))
        successful_add_calls = sum(1 for result in add_results if result)
        add_duration = time.time() - add_start

    print(
        "Playlist write stats:",
        f"clusters_considered={len(sorted_clusters)},",
        f"clusters_created={len(created_entries)},",
        f"create_calls={len(cluster_candidates)},",
        f"create_time={create_duration:.2f}s,",
        f"add_calls={len(add_jobs)},",
        f"add_calls_successful={successful_add_calls},",
        f"tracks_added={total_tracks_to_add},",
        f"add_time={add_duration:.2f}s",
    )

    log_step_time("Creating and populating cluster playlists", start_time)


async def process_single_playlist(auth_token, playlist_id, user_id):
    """Split one playlist into cluster playlists."""
    start_time = time.time()
    print(f"Processing {playlist_id}...")
    playlist_name_start = time.time()
    playlist_name = get_playlist_name(playlist_id, auth_token)
    log_step_time(
        f"Fetch source playlist name ({playlist_id})",
        playlist_name_start,
    )

    track_fetch_start = time.time()
    track_ids = await get_playlist_track_ids(auth_token, playlist_id)
    log_step_time(f"Collect playlist tracks ({playlist_id})", track_fetch_start)
    if not track_ids:
        print(f"No tracks found for playlist {playlist_id}")
        return

    feature_fetch_start = time.time()
    audio_features, _reccobeats_diagnostics = await get_track_audio_features(
        track_ids, auth_token
    )
    log_step_time(f"Resolve audio features ({playlist_id})", feature_fetch_start)
    if not audio_features:
        print(f"No audio features available for playlist {playlist_id}")
        return

    feature_by_track_id = {
        row["id"]: row for row in audio_features if isinstance(row, dict) and row.get("id")
    }
    if len(feature_by_track_id) < len(track_ids):
        removed = len(track_ids) - len(feature_by_track_id)
        print(f"Dropped {removed} tracks without usable unique audio features.")
    cluster_start = time.time()
    clustered_tracks = cluster_df(audio_features)
    log_step_time(f"Cluster tracks ({playlist_id})", cluster_start)
    if clustered_tracks.empty:
        print(f"Failed to cluster tracks for playlist {playlist_id}")
        return
    cluster_sizes = clustered_tracks["cluster"].value_counts().tolist()
    print(
        "Cluster distribution stats:",
        f"clusters={len(cluster_sizes)},",
        f"largest={max(cluster_sizes) if cluster_sizes else 0},",
        f"smallest={min(cluster_sizes) if cluster_sizes else 0}",
    )

    playlist_write_start = time.time()
    await create_and_populate_cluster_playlists(
        clustered_tracks, feature_by_track_id, user_id, auth_token, playlist_name
    )
    log_step_time(
        f"Write clustered playlists ({playlist_id})",
        playlist_write_start,
    )

    log_step_time(f"Processing playlist {playlist_id}", start_time)


async def process_playlists(auth_token, playlist_ids):
    """Process multiple playlists by splitting with K-means clustering."""
    start_time = time.time()
    print(f"Processing {len(playlist_ids)} playlists...")
    user_id = get_user_id(auth_token)

    tasks = [
        process_single_playlist(auth_token, playlist_id, user_id)
        for playlist_id in playlist_ids
    ]
    await asyncio.gather(*tasks)

    log_step_time("Processing all playlists", start_time)


def process_all(auth_token, playlist_ids):
    """Run async playlist processing entrypoint from sync Flask handler."""
    asyncio.run(process_playlists(auth_token, playlist_ids))
