import requests
import os
import time
import asyncio

SPOTIFY_API_URL = "https://api.spotify.com/v1"
RECCOBEATS_API_URL = "https://api.reccobeats.com/v1"


def spotify_request(
    method, endpoint, auth_token, params=None, data=None, json_data=None
):
    url = f"{SPOTIFY_API_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
    }

    response = requests.request(
        method, url, headers=headers, params=params, data=data, json=json_data
    )

    if response.status_code == 429:  # Rate limited
        retry_after = int(response.headers.get("Retry-After", 1))
        print(f"Rate limited. Retrying after {retry_after} seconds.")
        time.sleep(retry_after)
        return spotify_request(method, endpoint, auth_token, params, data, json_data)

    if response.status_code >= 400:
        print(f"Spotify API request error: {response.status_code}, {response.text}")
        return {}
    return response.json()


def reccobeats_request(method, endpoint, params=None):
    url = f"{RECCOBEATS_API_URL}{endpoint}"
    response = requests.request(method, url, params=params)

    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 1))
        print(f"ReccoBeats rate limited. Retrying after {retry_after} seconds.")
        time.sleep(retry_after)
        return reccobeats_request(method, endpoint, params)

    if response.status_code >= 400:
        print(f"ReccoBeats request error: {response.status_code}, {response.text}")
        return {}

    return response.json()


def is_access_token_valid(auth_token) -> bool:
    response = spotify_request("GET", "/me", auth_token)
    return response != {}


def refresh_access_token(refresh_token) -> str:
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    url = "https://accounts.spotify.com/api/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(url, data=data, headers=headers)
    if response.status_code != 200:
        print(f"Error refreshing access token: {response.status_code}, {response.text}")
        return ""

    token_data = response.json()
    return token_data.get("access_token")


def exchange_code_for_token(code):
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    url = "https://accounts.spotify.com/api/token"
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": "https://splitify-app-96607781f61f.herokuapp.com/callback",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(url, data=data, headers=headers)
    if response.status_code != 200:
        print(
            f"Error exchanging code for token: {response.status_code}, {response.text}"
        )
        return None

    return response.json()


def get_user_id(auth_token):
    response = spotify_request("GET", "/me", auth_token)
    if response:
        return response.get("id")
    return None


def get_all_playlists(auth_token):
    user_id = get_user_id(auth_token)
    if not user_id:
        return None

    endpoint = f"/users/{user_id}/playlists"
    params = {"limit": 50}
    response = spotify_request("GET", endpoint, auth_token, params)
    return response


def get_playlist_length(playlist_id, auth_token):
    endpoint = f"/playlists/{playlist_id}/tracks"
    params = {"fields": "total"}
    response = spotify_request("GET", endpoint, auth_token, params=params)
    if response:
        return response.get("total", 0)
    return -1


def get_playlist_name(playlist_id, auth_token):
    endpoint = f"/playlists/{playlist_id}"
    response = spotify_request("GET", endpoint, auth_token)
    if response:
        return response.get("name", "")
    return ""


async def get_playlist_children(start_index, playlist_id, auth_token):
    endpoint = f"/playlists/{playlist_id}/tracks"
    params = {
        "offset": start_index,
        "limit": 100,
        "fields": "items(track(id,uri))",
    }
    response = spotify_request("GET", endpoint, auth_token, params=params)
    return response


def get_audio_features(track_ids: list[str], auth_token) -> list[dict[str, float]]:
    endpoint = "/audio-features"
    params = {"ids": ",".join(track_ids)}
    response = spotify_request("GET", endpoint, auth_token, params=params)
    assert response
    return response["audio_features"]


def get_reccobeats_audio_features(track_id: str) -> dict[str, float]:
    response = reccobeats_request("GET", f"/track/{track_id}/audio-features")
    if not response:
        return {}

    payload = response.get("audioFeatures", response)
    if not isinstance(payload, dict):
        return {}

    payload["id"] = track_id
    return payload


async def create_playlist(user_id, auth_token, name, description):
    endpoint = f"/users/{user_id}/playlists"
    json_data = {"name": name, "description": description, "public": True}
    response = spotify_request("POST", endpoint, auth_token, json_data=json_data)
    if response:
        return response.get("id")
    return None


async def add_songs(playlist_id, track_uris, auth_token, position):
    endpoint = f"/playlists/{playlist_id}/tracks"
    json_data = {"uris": track_uris, "position": position}
    response = spotify_request("POST", endpoint, auth_token, json_data=json_data)
    await asyncio.sleep(0.5)
    if response:
        return response
    return None


async def get_artists(artist_ids, auth_token):
    """Fetch details for multiple artists in batches of 50."""
    all_artists = {}
    batch_size = 50
    for i in range(0, len(artist_ids), batch_size):
        batch = artist_ids[i : i + batch_size]
        endpoint = "/artists"
        params = {"ids": ",".join(batch)}
        response = spotify_request("GET", endpoint, auth_token, params=params)
        if response and "artists" in response:
            for artist in response["artists"]:
                all_artists[artist["id"]] = artist.get("genres", [])
        await asyncio.sleep(1)
    return all_artists
