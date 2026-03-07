import asyncio
import os
import re
import threading
import time
from urllib.parse import urlparse

import requests

SPOTIFY_API_URL = "https://api.spotify.com/v1"
RECCOBEATS_API_URL = "https://api.reccobeats.com/v1"
SPOTIFY_TRACK_ID_PATTERN = re.compile(r"^[A-Za-z0-9]{22}$")
SPOTIFY_TRACK_PATH_PATTERN = re.compile(r"/track/([A-Za-z0-9]{22})")
REQUEST_TIMEOUT_SECONDS = 15
SPOTIFY_REQUEST_INTERVAL_SECONDS = float(os.getenv("SPOTIFY_REQUEST_INTERVAL_SECONDS", "0.25"))
_SPOTIFY_RATE_LIMIT_LOCK = threading.Lock()
_SPOTIFY_NEXT_ALLOWED_TS = 0.0


def _wait_for_spotify_slot():
    """Global per-process pacing for Spotify API requests."""
    global _SPOTIFY_NEXT_ALLOWED_TS  # pylint: disable=global-statement
    with _SPOTIFY_RATE_LIMIT_LOCK:
        now = time.time()
        if now < _SPOTIFY_NEXT_ALLOWED_TS:
            time.sleep(_SPOTIFY_NEXT_ALLOWED_TS - now)
            now = time.time()
        _SPOTIFY_NEXT_ALLOWED_TS = now + SPOTIFY_REQUEST_INTERVAL_SECONDS


def _apply_spotify_retry_after(retry_after_seconds: int):
    """Push next allowed timestamp forward when Spotify responds with 429."""
    global _SPOTIFY_NEXT_ALLOWED_TS  # pylint: disable=global-statement
    with _SPOTIFY_RATE_LIMIT_LOCK:
        _SPOTIFY_NEXT_ALLOWED_TS = max(
            _SPOTIFY_NEXT_ALLOWED_TS, time.time() + max(retry_after_seconds, 1)
        )


def get_spotify_redirect_uri() -> str:
    app_base_url = os.getenv("APP_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
    return os.getenv("SPOTIFY_REDIRECT_URI", f"{app_base_url}/callback")


def spotify_request(
    method, endpoint, auth_token, params=None, data=None, json_data=None
):
    url = f"{SPOTIFY_API_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
    }

    _wait_for_spotify_slot()

    response = requests.request(
        method,
        url,
        headers=headers,
        params=params,
        data=data,
        json=json_data,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    if response.status_code == 429:  # Rate limited
        retry_after = int(response.headers.get("Retry-After", 1))
        print(f"Rate limited. Retrying after {retry_after} seconds.")
        _apply_spotify_retry_after(retry_after)
        time.sleep(retry_after)
        return spotify_request(method, endpoint, auth_token, params, data, json_data)

    if response.status_code >= 400:
        print(f"Spotify API request error: {response.status_code}, {response.text}")
        return {}
    return response.json()


def reccobeats_request(method, endpoint, params=None):
    url = f"{RECCOBEATS_API_URL}{endpoint}"
    response = requests.request(
        method, url, params=params, timeout=REQUEST_TIMEOUT_SECONDS
    )

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

    response = requests.post(
        url, data=data, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS
    )
    if response.status_code != 200:
        print(f"Error refreshing access token: {response.status_code}, {response.text}")
        return ""

    token_data = response.json()
    return token_data.get("access_token")


def exchange_code_for_token(code):
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    redirect_uri = get_spotify_redirect_uri()
    if not client_id or not client_secret or not redirect_uri:
        print(
            "Missing Spotify OAuth configuration (CLIENT_ID/CLIENT_SECRET/redirect_uri)"
        )
        return None

    url = "https://accounts.spotify.com/api/token"
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(
        url, data=data, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS
    )
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


def _chunk_list(values: list[str], chunk_size: int) -> list[list[str]]:
    return [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]


def _is_spotify_track_id(value: str | None) -> bool:
    return bool(value) and bool(SPOTIFY_TRACK_ID_PATTERN.fullmatch(value))


def _extract_spotify_track_id_from_value(value: str | None) -> str | None:
    if not value or not isinstance(value, str):
        return None

    if _is_spotify_track_id(value):
        return value

    if value.startswith("spotify:track:"):
        candidate = value.split(":")[-1]
        return candidate if _is_spotify_track_id(candidate) else None

    try:
        parsed = urlparse(value)
        if parsed.netloc.endswith("spotify.com"):
            match = SPOTIFY_TRACK_PATH_PATTERN.search(parsed.path)
            if match:
                candidate = match.group(1)
                return candidate if _is_spotify_track_id(candidate) else None
    except Exception:
        return None

    return None


def get_reccobeats_audio_features_batch(
    track_ids: list[str], batch_size: int = 40, include_diagnostics: bool = False
):
    """
    Fetch audio features in batches from ReccoBeats.
    Endpoint: GET /v1/audio-features?ids=<comma-separated spotify track ids>
    """
    all_features = []
    cleaned_track_ids = [track_id for track_id in track_ids if track_id]
    diagnostics = {track_id: "not_returned_by_reccobeats" for track_id in cleaned_track_ids}
    matched_ids = set()
    unexpected_id_count = 0
    invalid_response_item_count = 0
    for id_batch in _chunk_list(cleaned_track_ids, batch_size):
        params = {"ids": ",".join(id_batch)}
        response = reccobeats_request("GET", "/audio-features", params=params)
        if not response:
            continue
        payload = response.get("content", response)
        if not isinstance(payload, list):
            continue

        for item in payload:
            if not isinstance(item, dict):
                continue
            normalized = dict(item)
            spotify_id = (
                _extract_spotify_track_id_from_value(normalized.get("spotifyTrackId"))
                or _extract_spotify_track_id_from_value(normalized.get("spotifyId"))
                or _extract_spotify_track_id_from_value(
                    normalized.get("spotify_track_id")
                )
                or _extract_spotify_track_id_from_value(normalized.get("spotify_id"))
                or _extract_spotify_track_id_from_value(normalized.get("href"))
                or _extract_spotify_track_id_from_value(normalized.get("uri"))
                or _extract_spotify_track_id_from_value(normalized.get("trackUri"))
                or _extract_spotify_track_id_from_value(normalized.get("track_uri"))
            )

            if not _is_spotify_track_id(spotify_id):
                candidate = normalized.get("id")
                extracted = _extract_spotify_track_id_from_value(candidate)
                if _is_spotify_track_id(extracted):
                    spotify_id = extracted
                else:
                    candidate = normalized.get("trackId")
                    extracted = _extract_spotify_track_id_from_value(candidate)
                    if _is_spotify_track_id(extracted):
                        spotify_id = extracted
            if not _is_spotify_track_id(spotify_id):
                candidate = normalized.get("trackHref")
                extracted = _extract_spotify_track_id_from_value(candidate)
                if _is_spotify_track_id(extracted):
                    spotify_id = extracted
            if not _is_spotify_track_id(spotify_id):
                candidate = normalized.get("track_url")
                extracted = _extract_spotify_track_id_from_value(candidate)
                if _is_spotify_track_id(extracted):
                    spotify_id = extracted
            if not _is_spotify_track_id(spotify_id):
                candidate = normalized.get("trackUrl")
                extracted = _extract_spotify_track_id_from_value(candidate)
                if _is_spotify_track_id(extracted):
                    spotify_id = extracted

            if _is_spotify_track_id(spotify_id):
                normalized["id"] = spotify_id
                all_features.append(normalized)
                if spotify_id in diagnostics:
                    diagnostics[spotify_id] = "ok"
                    matched_ids.add(spotify_id)
                else:
                    unexpected_id_count += 1
            else:
                invalid_response_item_count += 1

    if include_diagnostics:
        summary = {
            "requested": len(cleaned_track_ids),
            "matched": len(matched_ids),
            "not_returned": sum(
                1 for reason in diagnostics.values() if reason == "not_returned_by_reccobeats"
            ),
            "unexpected_id_count": unexpected_id_count,
            "invalid_response_item_count": invalid_response_item_count,
        }
        return all_features, diagnostics, summary

    return all_features


def get_track_metadata_map(track_ids: list[str], auth_token) -> dict[str, dict]:
    """Fetch track name/artists for Spotify track IDs in batches of 50."""
    metadata = {}
    for batch in _chunk_list([track_id for track_id in track_ids if track_id], 50):
        response = spotify_request(
            "GET",
            "/tracks",
            auth_token,
            params={"ids": ",".join(batch)},
        )
        tracks = response.get("tracks", []) if isinstance(response, dict) else []
        for track in tracks:
            if not isinstance(track, dict):
                continue
            track_id = track.get("id")
            if not _is_spotify_track_id(track_id):
                continue
            artists = [
                artist.get("name", "")
                for artist in track.get("artists", [])
                if isinstance(artist, dict) and artist.get("name")
            ]
            metadata[track_id] = {
                "name": track.get("name", ""),
                "artists": artists,
            }
    return metadata


def search_track_ids_by_name_artist(
    track_name: str, artist_name: str, auth_token, limit: int = 5
) -> list[str]:
    """Search Spotify by track+artist and return candidate Spotify track IDs."""
    if not track_name or not artist_name:
        return []

    query = f'track:"{track_name}" artist:"{artist_name}"'
    response = spotify_request(
        "GET",
        "/search",
        auth_token,
        params={"q": query, "type": "track", "limit": limit},
    )
    if not isinstance(response, dict):
        return []
    items = (
        response.get("tracks", {}).get("items", [])
        if isinstance(response.get("tracks"), dict)
        else []
    )

    candidate_ids = []
    for item in items:
        if not isinstance(item, dict):
            continue
        candidate_id = item.get("id")
        if _is_spotify_track_id(candidate_id):
            candidate_ids.append(candidate_id)
    return candidate_ids


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
    profiles = await get_artist_profiles(artist_ids, auth_token)
    return {artist_id: profile.get("genres", []) for artist_id, profile in profiles.items()}


async def get_artist_profiles(artist_ids, auth_token):
    """Fetch artist names and genres in batches of 50."""
    all_artists = {}
    batch_size = 50
    for i in range(0, len(artist_ids), batch_size):
        batch = artist_ids[i : i + batch_size]
        endpoint = "/artists"
        params = {"ids": ",".join(batch)}
        response = spotify_request("GET", endpoint, auth_token, params=params)
        if response and "artists" in response:
            for artist in response["artists"]:
                all_artists[artist["id"]] = {
                    "name": artist.get("name", ""),
                    "genres": artist.get("genres", []),
                }
        await asyncio.sleep(1)
    return all_artists
