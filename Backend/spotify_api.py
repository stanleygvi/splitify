"""Spotify/ReccoBeats API client helpers used by backend routes and processing."""

import asyncio
import os
import re
import threading
import time
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter

SPOTIFY_API_URL = "https://api.spotify.com/v1"
RECCOBEATS_API_URL = "https://api.reccobeats.com/v1"
SPOTIFY_TRACK_ID_PATTERN = re.compile(r"^[A-Za-z0-9]{22}$")
SPOTIFY_TRACK_PATH_PATTERN = re.compile(r"/track/([A-Za-z0-9]{22})")
REQUEST_TIMEOUT_SECONDS = 15
SPOTIFY_RETRYABLE_STATUS_CODES = {500, 502, 503, 504}


def _env_non_negative_float(name: str, default_value: float) -> float:
    """Parse a non-negative float env setting with fallback."""
    raw_value = os.getenv(name, str(default_value))
    try:
        parsed = float(raw_value)
    except ValueError:
        return default_value
    return max(0.0, parsed)


def _env_positive_int(name: str, default_value: int) -> int:
    """Parse a positive integer env setting with fallback."""
    raw_value = os.getenv(name, str(default_value))
    try:
        parsed = int(raw_value)
    except ValueError:
        return default_value
    return max(1, parsed)


SPOTIFY_REQUEST_INTERVAL_SECONDS = _env_non_negative_float(
    "SPOTIFY_REQUEST_INTERVAL_SECONDS", 0.05
)
SPOTIFY_MAX_RETRY_ATTEMPTS = _env_positive_int("SPOTIFY_MAX_RETRY_ATTEMPTS", 3)
SPOTIFY_RETRY_BASE_SECONDS = _env_non_negative_float("SPOTIFY_RETRY_BASE_SECONDS", 0.75)
SPOTIFY_MAX_INFLIGHT_REQUESTS = _env_positive_int("SPOTIFY_MAX_INFLIGHT_REQUESTS", 12)
SPOTIFY_POOL_CONNECTIONS = _env_positive_int("SPOTIFY_POOL_CONNECTIONS", 32)
SPOTIFY_POOL_MAXSIZE = _env_positive_int("SPOTIFY_POOL_MAXSIZE", 64)
_SPOTIFY_RATE_LIMIT_LOCK = threading.Lock()
_SPOTIFY_NEXT_ALLOWED_TS = 0.0
_SPOTIFY_INFLIGHT_SEMAPHORE = threading.BoundedSemaphore(SPOTIFY_MAX_INFLIGHT_REQUESTS)
_SPOTIFY_SESSION = None
_SPOTIFY_SESSION_LOCK = threading.Lock()


def _reserve_spotify_slot_delay() -> float:
    """Reserve the next Spotify request slot and return required sleep time."""
    global _SPOTIFY_NEXT_ALLOWED_TS  # pylint: disable=global-statement
    with _SPOTIFY_RATE_LIMIT_LOCK:
        now = time.time()
        reserved_ts = max(now, _SPOTIFY_NEXT_ALLOWED_TS)
        _SPOTIFY_NEXT_ALLOWED_TS = reserved_ts + SPOTIFY_REQUEST_INTERVAL_SECONDS
    return max(0.0, reserved_ts - now)


def _apply_spotify_retry_after(retry_after_seconds: int):
    """Push next allowed timestamp forward when Spotify responds with 429."""
    global _SPOTIFY_NEXT_ALLOWED_TS  # pylint: disable=global-statement
    with _SPOTIFY_RATE_LIMIT_LOCK:
        _SPOTIFY_NEXT_ALLOWED_TS = max(
            _SPOTIFY_NEXT_ALLOWED_TS, time.time() + max(retry_after_seconds, 1)
        )


def _parse_retry_after_seconds(value) -> int:
    """Parse Retry-After seconds safely with a minimum of one second."""
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return 1
    return max(1, parsed)


def _get_spotify_session() -> requests.Session:
    """Return process-wide pooled HTTP session for Spotify requests."""
    global _SPOTIFY_SESSION  # pylint: disable=global-statement
    if _SPOTIFY_SESSION is not None:
        return _SPOTIFY_SESSION

    with _SPOTIFY_SESSION_LOCK:
        if _SPOTIFY_SESSION is None:
            session = requests.Session()
            adapter = HTTPAdapter(
                pool_connections=SPOTIFY_POOL_CONNECTIONS,
                pool_maxsize=SPOTIFY_POOL_MAXSIZE,
                pool_block=True,
            )
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            _SPOTIFY_SESSION = session

    assert _SPOTIFY_SESSION is not None
    return _SPOTIFY_SESSION


def get_spotify_redirect_uri() -> str:
    """Return OAuth callback URI based on env defaults and overrides."""
    app_base_url = os.getenv("APP_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
    return os.getenv("SPOTIFY_REDIRECT_URI", f"{app_base_url}/callback")


def spotify_request(
    method,
    endpoint,
    auth_token,
    params=None,
    data=None,
    json_data=None,
    retry_attempt: int = 0,
):
    """Send a Spotify Web API request with shared throttling and retry handling."""
    url = f"{SPOTIFY_API_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
    }
    session = _get_spotify_session()

    current_retry_attempt = retry_attempt

    while True:
        slot_delay_seconds = _reserve_spotify_slot_delay()
        if slot_delay_seconds > 0:
            time.sleep(slot_delay_seconds)

        try:
            with _SPOTIFY_INFLIGHT_SEMAPHORE:
                response = session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    data=data,
                    json=json_data,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
        except requests.RequestException as error:
            if current_retry_attempt < SPOTIFY_MAX_RETRY_ATTEMPTS:
                sleep_seconds = SPOTIFY_RETRY_BASE_SECONDS * (2 ** current_retry_attempt)
                print(
                    "Spotify request network error. Retrying:",
                    f"attempt={current_retry_attempt + 1},",
                    f"sleep={sleep_seconds:.2f}s,",
                    f"error={error}",
                )
                current_retry_attempt += 1
                time.sleep(sleep_seconds)
                continue
            print(f"Spotify request network error (final): {error}")
            return {}

        if response.status_code == 429:  # Rate limited
            retry_after = _parse_retry_after_seconds(response.headers.get("Retry-After"))
            print(f"Rate limited. Retrying after {retry_after} seconds.")
            _apply_spotify_retry_after(retry_after)
            time.sleep(retry_after)
            continue

        if (
            response.status_code in SPOTIFY_RETRYABLE_STATUS_CODES
            and current_retry_attempt < SPOTIFY_MAX_RETRY_ATTEMPTS
        ):
            sleep_seconds = SPOTIFY_RETRY_BASE_SECONDS * (2 ** current_retry_attempt)
            print(
                "Spotify retryable error. Retrying:",
                f"status={response.status_code},",
                f"attempt={current_retry_attempt + 1},",
                f"sleep={sleep_seconds:.2f}s",
            )
            current_retry_attempt += 1
            time.sleep(sleep_seconds)
            continue

        if response.status_code >= 400:
            print(f"Spotify API request error: {response.status_code}, {response.text}")
            return {}
        return response.json()


def reccobeats_request(method, endpoint, params=None):
    """Send a ReccoBeats API request with basic rate-limit retry handling."""
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
    """Return whether the Spotify access token can successfully call /me."""
    response = spotify_request("GET", "/me", auth_token)
    return response != {}


def refresh_access_token(refresh_token) -> str:
    """Use refresh token to obtain a new Spotify access token."""
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
    """Exchange Spotify OAuth authorization code for tokens."""
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
    """Return Spotify user ID for the current access token."""
    response = spotify_request("GET", "/me", auth_token)
    if response:
        return response.get("id")
    return None


def get_all_playlists(auth_token):
    """Return first page of playlists for the authenticated Spotify user."""
    user_id = get_user_id(auth_token)
    if not user_id:
        return None

    endpoint = f"/users/{user_id}/playlists"
    params = {"limit": 50}
    response = spotify_request("GET", endpoint, auth_token, params)
    return response


def get_playlist_length(playlist_id, auth_token):
    """Return total track count for a Spotify playlist."""
    endpoint = f"/playlists/{playlist_id}/tracks"
    params = {"fields": "total"}
    response = spotify_request("GET", endpoint, auth_token, params=params)
    if response:
        return response.get("total", 0)
    return -1


def get_playlist_name(playlist_id, auth_token):
    """Return display name for a Spotify playlist."""
    endpoint = f"/playlists/{playlist_id}"
    response = spotify_request("GET", endpoint, auth_token)
    if response:
        return response.get("name", "")
    return ""


async def get_playlist_children(start_index, playlist_id, auth_token, include_total=False):
    """Return one page of playlist tracks using offset pagination."""
    fields = "items(track(id))"
    if include_total:
        fields = "total,items(track(id))"
    endpoint = f"/playlists/{playlist_id}/tracks"
    params = {
        "offset": start_index,
        "limit": 100,
        "fields": fields,
    }
    response = await asyncio.to_thread(
        spotify_request, "GET", endpoint, auth_token, params, None, None
    )
    return response


def get_audio_features(track_ids: list[str], auth_token) -> list[dict[str, float]]:
    """Fetch Spotify audio features for a list of track IDs."""
    endpoint = "/audio-features"
    params = {"ids": ",".join(track_ids)}
    response = spotify_request("GET", endpoint, auth_token, params=params)
    assert response
    return response["audio_features"]


def get_reccobeats_audio_features(track_id: str) -> dict[str, float]:
    """Fetch ReccoBeats audio features for a single track ID."""
    response = reccobeats_request("GET", f"/track/{track_id}/audio-features")
    if not response:
        return {}

    payload = response.get("audioFeatures", response)
    if not isinstance(payload, dict):
        return {}

    payload["id"] = track_id
    return payload


def _chunk_list(values: list[str], chunk_size: int) -> list[list[str]]:
    """Split list into fixed-size chunks."""
    return [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]


def _is_spotify_track_id(value: str | None) -> bool:
    """Return whether value matches Spotify 22-char base62 track ID."""
    return bool(value) and bool(SPOTIFY_TRACK_ID_PATTERN.fullmatch(value))


def _extract_spotify_track_id_from_value(value: str | None) -> str | None:
    """Extract Spotify track ID from raw ID/URI/URL string value."""
    if not value or not isinstance(value, str):
        return None

    if _is_spotify_track_id(value):
        return value

    if value.startswith("spotify:track:"):
        candidate = value.split(":")[-1]
        return candidate if _is_spotify_track_id(candidate) else None

    try:
        parsed = urlparse(value)
        host = parsed.hostname
        if host and (host == "spotify.com" or host.endswith(".spotify.com")):
            match = SPOTIFY_TRACK_PATH_PATTERN.search(parsed.path)
            if match:
                candidate = match.group(1)
                return candidate if _is_spotify_track_id(candidate) else None
    except (ValueError, AttributeError):
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
    """Create a Spotify playlist and return its ID."""
    endpoint = f"/users/{user_id}/playlists"
    json_data = {"name": name, "description": description, "public": True}
    response = await asyncio.to_thread(
        spotify_request, "POST", endpoint, auth_token, None, None, json_data
    )
    if response:
        return response.get("id")
    return None


async def add_songs(playlist_id, track_uris, auth_token, position=None):
    """Add tracks to a Spotify playlist at a given insertion position."""
    endpoint = f"/playlists/{playlist_id}/tracks"
    json_data = {"uris": track_uris}
    if position is not None:
        json_data["position"] = position
    response = await asyncio.to_thread(
        spotify_request, "POST", endpoint, auth_token, None, None, json_data
    )
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
