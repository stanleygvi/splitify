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


def log_step_time(step_name, start_time):
    elapsed_time = time.time() - start_time
    print(f"{step_name} completed in {elapsed_time:.2f} seconds.")


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

        return track_ids
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
    clustered_tracks, user_id, auth_token, playlist_name
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

        for index, (_, cluster_track_ids) in enumerate(sorted_clusters, start=1):
            valid_cluster_track_ids = [
                track_id
                for track_id in cluster_track_ids
                if isinstance(track_id, str)
                and SPOTIFY_TRACK_ID_PATTERN.fullmatch(track_id)
            ]
            if not valid_cluster_track_ids:
                continue

            playlist_id = await create_playlist(
                user_id,
                auth_token,
                f"{playlist_name} - Cluster {index}",
                " Made using Splitify: https://splitifytool.com/",
            )

            if not playlist_id:
                continue

            slices = calc_slices(len(valid_cluster_track_ids))
            add_tasks = []
            for position in range(0, slices * 100, 100):
                track_slice = valid_cluster_track_ids[position : position + 100]
                track_uris = [f"spotify:track:{track_id}" for track_id in track_slice]
                add_tasks.append(add_songs(playlist_id, track_uris, auth_token, position))

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

        clustered_tracks = cluster_df(audio_features)
        if clustered_tracks.empty:
            print(f"Failed to cluster tracks for playlist {playlist_id}")
            return

        await create_and_populate_cluster_playlists(
            clustered_tracks, user_id, auth_token, playlist_name
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
