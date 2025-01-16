from threading import Thread, Lock
from collections import defaultdict
from Backend.spotify_api import (
    get_playlist_length,
    get_playlist_children,
    create_playlist,
    add_songs,
    get_user_id,
    get_audio_features,
    get_playlist_name,
    get_artists,
)
from Backend.helpers import calc_slices
from Backend.grouping import cluster_df
import time


def extract_ids(playlist_data):
    track_ids = []
    for track in playlist_data:
        if track["track"] and track["track"]["id"]:
            track_ids.append(track["track"]["id"])
    return track_ids


def clean_audio_features(
    audio_features: list[dict[str, float]], remove_keys: list[str]
):
    index = 0
    index_remove = []
    for feature in audio_features:
        if feature:
            for key in remove_keys:
                feature.pop(key, None)
        else:
            index_remove.append(index)
        index += 1
    for i in index_remove:
        audio_features.pop(i)


def process_playlists(auth_token, playlist_ids):
    threads = []
    for playlist_id in playlist_ids:
        length = get_playlist_length(playlist_id, auth_token)
        if length == -1:
            print(f"Error fetching playlist length for {playlist_id}")
            continue

        thread = Thread(
            target=process_single_playlist, args=(auth_token, playlist_id, length)
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


# K MEANS CLUSTERING ----------------------------------------------------------------------
# def process_single_playlist(auth_token, playlist_id, total_length):
#     name = get_playlist_name(playlist_id, auth_token)
#     slices = calc_slices(total_length)
#     playlist_data_store = {"id": playlist_id, "tracks": []}

#     for i in range(0, slices * 100, 100):
#         append_to_playlist_data(i, playlist_id, auth_token, playlist_data_store)
#     if len(playlist_data_store["tracks"]) < 1:
#         print(f"failed to process playlist: {playlist_id}")
#         return
#     user_id = get_user_id(auth_token)
#     grouped = cluster_df(playlist_data_store)
#     num_playlists = len(grouped["cluster"].value_counts())

#     threads = []
#     for num in range(0, num_playlists):
#         cluster = grouped[grouped["cluster"] == num]
#         thread = Thread(
#             target=created_and_populate, args=(cluster, user_id, auth_token, name)
#         )
#         thread.start()
#         threads.append(thread)
#     for thread in threads:
#         thread.join()
# ----------------------------------------------------------------------------------------


def create_and_populate_subgenre_playlist(
    subgenre, tracks, user_id, auth_token, original_playlist_name
):
    """Create and populate a playlist for a specific subgenre."""
    if not tracks:
        return

    playlist_id = create_playlist(
        user_id,
        auth_token,
        f"{original_playlist_name} - {subgenre}",
        f"Split by subgenre: {subgenre}. Made using Splitify: https://splitifytool.com/",
    )

    slices = calc_slices(len(tracks))
    for position in range(0, slices * 100, 100):
        if (position + 100) > len(tracks):
            track_slice = tracks[position:]
        else:
            track_slice = tracks[position : position + 100]

        track_uris = [track["uri"] for track in track_slice]
        status = add_songs(playlist_id, track_uris, auth_token, position)
        time.sleep(0.5)

        if not status or status.get("Error", None):
            print(
                f"Append Error: Playlist {original_playlist_name} - {subgenre}, status {status}, starting from index: {position}"
            )


def get_artist_details(artist_ids, auth_token):
    """Fetch details for multiple artists, including their subgenres."""
    artist_data = {}
    batch_size = 50
    for i in range(0, len(artist_ids), batch_size):
        batch = artist_ids[i : i + batch_size]
        response = get_artists(batch, auth_token)
        if response and "artists" in response:
            for artist in response["artists"]:
                artist_data[artist["id"]] = {
                    "genres": artist.get("genres", []),
                }
    return artist_data


# Subgenre --------------------------------------------------------------------------------------------------------
def process_single_playlist(auth_token, playlist_id, total_length):
    name = get_playlist_name(playlist_id, auth_token)
    slices = calc_slices(total_length)
    playlist_data_store = {"id": playlist_id, "tracks": []}

    for i in range(0, slices * 100, 100):
        append_to_playlist_data(i, playlist_id, auth_token, playlist_data_store)

    if len(playlist_data_store["tracks"]) < 1:
        print(f"Failed to process playlist: {playlist_id}")
        return

    # track_ids = [track["id"] for track in playlist_data_store["tracks"]]
    artist_ids = list(
        {
            track["artist_id"]
            for track in playlist_data_store["tracks"]
            if "artist_id" in track
        }
    )

    artist_data = get_artist_details(artist_ids, auth_token)

    if not artist_data:
        print(f"Failed to fetch artist details for playlist: {playlist_id}")
        return

    subgenre_to_tracks = defaultdict(list)
    for track in playlist_data_store["tracks"]:
        artist_id = track.get("artist_id")
        if artist_id in artist_data:
            subgenres = artist_data[artist_id].get("genres", [])
            for subgenre in subgenres:
                subgenre_to_tracks[subgenre].append(track)

    user_id = get_user_id(auth_token)

    # Create playlists for each subgenre and populate them
    threads = []
    for subgenre, tracks in subgenre_to_tracks.items():
        thread = Thread(
            target=create_and_populate_subgenre_playlist,
            args=(subgenre, tracks, user_id, auth_token, name),
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


def created_and_populate(cluster_df, user_id, auth_token, name):

    slices = calc_slices(len(cluster_df))
    if slices < 1:
        return

    playlist_id = create_playlist(
        user_id,
        auth_token,
        f"Split playlist from {name} ",
        "Made using Splitify: https://splitifytool.com/",
    )
    for position in range(0, slices * 100, 100):
        if (position + 100) > len(cluster_df):
            cluster_slice = cluster_df.iloc[position:]
        else:
            cluster_slice = cluster_df.iloc[position : position + 100]
        track_uris = cluster_slice["uri"].tolist()

        status = add_songs(playlist_id, track_uris, auth_token, position)
        time.sleep(0.5)
        if not status or status.get("Error", None):
            print(
                f"Append Error: Playlist{name} split, status {status} starting from index: {position}"
            )


# K MEANS CLUSTERING ----------------------------------------------------------------------
# def append_to_playlist_data(start_index, playlist_id, auth_token, data_store):
#     response = get_playlist_children(start_index, playlist_id, auth_token)
#     if response and "items" in response:

#         track_ids = extract_ids(response["items"])
#         audio_features = get_audio_features(track_ids, auth_token)
#         clean_audio_features(
#             audio_features, ["type", "id", "track_href", "analysis_url", "duration_ms"]
#         )
#         data_store["tracks"].extend(audio_features)
#         print(
#             f"Appended {len(response["items"])} tracks from playlist starting at index {start_index}"
#         )
#     else:
#         print(f"Failed to append playlist data from index {start_index}")
# ----------------------------------------------------------------------------------------


def fetch_genres(artist_ids, track_id, auth_token, data_store, genre_lock):
    artist_data = get_artists(artist_ids, auth_token)
    if artist_data and "artists" in artist_data:
        genres = set()
        for artist in artist_data["artists"]:
            genres.update(artist.get("genres", []))
        with genre_lock:
            data_store["genres"].append({"track_id": track_id, "genres": list(genres)})

def append_to_playlist_data(start_index, playlist_id, auth_token, data_store):
    response = get_playlist_children(start_index, playlist_id, auth_token)
    if response and "items" in response:
        tracks = response["items"]
        print(tracks)
        track_to_artists = {
            track["track"]["id"]: [artist["id"] for artist in track["track"]["artists"]]
            for track in tracks
            if track["track"] and "artists" in track["track"]
        }
        genre_lock = Lock()
        threads = []

        for track_id, artist_ids in track_to_artists.items():
            thread = Thread(target=fetch_genres, args=(artist_ids, track_id, auth_token, data_store, genre_lock))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        print(
            f"Appended {len(response['items'])} tracks' genres from playlist starting at index {start_index}"
        )
    else:
        print(f"Failed to append playlist data from index {start_index}")
