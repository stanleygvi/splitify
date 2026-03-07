import asyncio
import time
from collections import defaultdict
import re
from Backend.spotify_api import (
    get_playlist_length,
    get_playlist_children,
    create_playlist,
    add_songs,
    get_user_id,
    get_playlist_name,
    get_reccobeats_audio_features_batch,
)
from Backend.helpers import calc_slices
from Backend.grouping import cluster_df

SPOTIFY_TRACK_ID_PATTERN = re.compile(r"^[A-Za-z0-9]{22}$")
LABEL_FEATURES = [
    "energy",
    "danceability",
    "valence",
    "acousticness",
    "instrumentalness",
    "speechiness",
]
POSITIVE_LABELS = {
    "energy": "High Energy",
    "danceability": "Danceable",
    "valence": "Upbeat",
    "acousticness": "Acoustic",
    "instrumentalness": "Instrumental",
    "speechiness": "Speech-Heavy",
}
NEGATIVE_LABELS = {
    "energy": "Low Energy",
    "danceability": "Less Danceable",
    "valence": "Moody",
    "acousticness": "Electronic",
    "instrumentalness": "Vocal-Forward",
    "speechiness": "Melodic",
}


def log_step_time(step_name, start_time):
    elapsed_time = time.time() - start_time
    print(f"{step_name} completed in {elapsed_time:.2f} seconds.")


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compute_feature_means(track_ids, feature_by_track_id):
    means = {}
    for feature in LABEL_FEATURES:
        values = []
        for track_id in track_ids:
            row = feature_by_track_id.get(track_id, {})
            numeric = _to_float(row.get(feature))
            if numeric is not None:
                values.append(numeric)
        means[feature] = sum(values) / len(values) if values else None
    return means


def _build_cluster_reason(cluster_means, global_means):
    ranked = []
    for feature in LABEL_FEATURES:
        cluster_value = cluster_means.get(feature)
        global_value = global_means.get(feature)
        if cluster_value is None or global_value is None:
            continue
        delta = cluster_value - global_value
        if abs(delta) < 0.08:
            continue
        label = POSITIVE_LABELS[feature] if delta > 0 else NEGATIVE_LABELS[feature]
        ranked.append((abs(delta), label))

    if not ranked:
        return "Balanced Mix"

    ranked.sort(key=lambda item: item[0], reverse=True)
    return " + ".join(label for _, label in ranked[:2])


def _dedupe_track_ids(track_ids):
    """Return unique track ids in original order and duplicate count."""
    unique_ids = []
    seen = set()
    duplicate_count = 0
    for track_id in track_ids:
        if track_id in seen:
            duplicate_count += 1
            continue
        seen.add(track_id)
        unique_ids.append(track_id)
    return unique_ids, duplicate_count


async def get_playlist_track_ids(auth_token, playlist_id):
    """Return all track IDs from the playlist."""
    try:
        slices = calc_slices(get_playlist_length(playlist_id, auth_token))
        track_ids = []

        for i in range(0, slices * 100, 100):
            response = await get_playlist_children(i, playlist_id, auth_token)
            if not response or "items" not in response:
                continue

            for item in response["items"]:
                track = item.get("track") or {}
                track_id = track.get("id")
                if track_id:
                    track_ids.append(track_id)

        unique_track_ids, duplicate_count = _dedupe_track_ids(track_ids)
        if duplicate_count > 0:
            print(
                f"Removed {duplicate_count} duplicate tracks from source playlist {playlist_id}."
            )
        return unique_track_ids
    except Exception as e:
        print(f"Error getting playlist tracks for {playlist_id}: {e}")
        return []


async def get_track_audio_features(track_ids):
    """Fetch audio features from ReccoBeats in batch for Spotify track IDs."""
    start_time = time.time()
    features = await asyncio.to_thread(get_reccobeats_audio_features_batch, track_ids)
    log_step_time("Fetching audio features", start_time)
    return features


async def create_and_populate_cluster_playlists(
    clustered_tracks, feature_by_track_id, user_id, auth_token, playlist_name
):
    """Create one playlist per K-means cluster and add grouped tracks."""
    start_time = time.time()
    try:
        tracks_by_cluster = defaultdict(list)

        for _, row in clustered_tracks.iterrows():
            tracks_by_cluster[int(row["cluster"])].append(row["id"])

        sorted_clusters = sorted(
            tracks_by_cluster.items(), key=lambda item: len(item[1]), reverse=True
        )
        global_means = _compute_feature_means(
            list(clustered_tracks["id"]), feature_by_track_id
        )

        for index, (_, cluster_track_ids) in enumerate(sorted_clusters, start=1):
            valid_cluster_track_ids = [
                track_id
                for track_id in cluster_track_ids
                if isinstance(track_id, str)
                and SPOTIFY_TRACK_ID_PATTERN.fullmatch(track_id)
            ]
            valid_cluster_track_ids, _ = _dedupe_track_ids(valid_cluster_track_ids)
            if not valid_cluster_track_ids:
                continue

            cluster_means = _compute_feature_means(
                valid_cluster_track_ids, feature_by_track_id
            )
            cluster_reason = _build_cluster_reason(cluster_means, global_means)
            playlist_title = f"{playlist_name} - Cluster {index} ({cluster_reason})"
            if len(playlist_title) > 100:
                playlist_title = playlist_title[:97] + "..."

            playlist_id = await create_playlist(
                user_id,
                auth_token,
                playlist_title,
                f"Grouped by audio similarity: {cluster_reason}. "
                "Made using Splitify: https://splitifytool.com/",
            )

            if not playlist_id:
                continue

            slices = calc_slices(len(valid_cluster_track_ids))
            add_tasks = []
            for position in range(0, slices * 100, 100):
                track_slice = valid_cluster_track_ids[position : position + 100]
                track_uris = [f"spotify:track:{track_id}" for track_id in track_slice]
                add_tasks.append(
                    add_songs(playlist_id, track_uris, auth_token, position)
                )

            await asyncio.gather(*add_tasks)

        log_step_time("Creating and populating cluster playlists", start_time)
    except Exception as e:
        print(f"Error creating cluster playlists: {e}")


async def process_single_playlist(auth_token, playlist_id, user_id):
    """Split one playlist into K-means cluster playlists."""
    try:
        start_time = time.time()
        print(f"Processing {playlist_id}...")
        playlist_name = get_playlist_name(playlist_id, auth_token)

        track_ids = await get_playlist_track_ids(auth_token, playlist_id)
        if not track_ids:
            print(f"No tracks found for playlist {playlist_id}")
            return

        audio_features = await get_track_audio_features(track_ids)
        if not audio_features:
            print(f"No audio features available for playlist {playlist_id}")
            return

        feature_by_track_id = {
            row["id"]: row for row in audio_features if isinstance(row, dict) and row.get("id")
        }
        if len(feature_by_track_id) < len(track_ids):
            removed = len(track_ids) - len(feature_by_track_id)
            print(f"Dropped {removed} tracks without usable unique audio features.")
        clustered_tracks = cluster_df(audio_features)
        if clustered_tracks.empty:
            print(f"Failed to cluster tracks for playlist {playlist_id}")
            return

        await create_and_populate_cluster_playlists(
            clustered_tracks, feature_by_track_id, user_id, auth_token, playlist_name
        )

        log_step_time(f"Processing playlist {playlist_id}", start_time)
    except Exception as e:
        print(f"Error processing playlist {playlist_id}: {e}")


async def process_playlists(auth_token, playlist_ids):
    """Process multiple playlists by splitting with K-means clustering."""
    try:
        start_time = time.time()
        print(f"Processing {len(playlist_ids)} playlists...")
        user_id = get_user_id(auth_token)

        tasks = [
            process_single_playlist(auth_token, playlist_id, user_id)
            for playlist_id in playlist_ids
        ]
        await asyncio.gather(*tasks)

        log_step_time("Processing all playlists", start_time)
    except Exception as e:
        print(f"Error processing playlists: {e}")


def process_all(auth_token, playlist_ids):
    try:
        asyncio.run(process_playlists(auth_token, playlist_ids))
    except Exception as e:
        print(f"Error in process_all: {e}")
