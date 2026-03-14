"""Flask API entrypoint for Spotify auth and playlist processing routes."""

import os
import threading
import time
import uuid
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlencode

from dotenv import load_dotenv
from flask import Flask, request, redirect, jsonify, session
from flask_cors import CORS
from markupsafe import escape

try:
    from Backend.spotify_api import (
        is_access_token_valid,
        refresh_access_token,
        get_all_playlists,
        exchange_code_for_token,
        get_user_id,
        get_spotify_redirect_uri,
    )
    from Backend.playlist_processing import process_all
    from Backend.grouping import normalize_feature_weights
    from Backend.helpers import generate_random_string
    from Backend.job_status_store import (
        set_job_state,
        get_job_state,
        prune_finished_jobs_older_than,
    )
except ModuleNotFoundError:
    from spotify_api import (  # type: ignore
        is_access_token_valid,
        refresh_access_token,
        get_all_playlists,
        exchange_code_for_token,
        get_user_id,
        get_spotify_redirect_uri,
    )
    from playlist_processing import process_all  # type: ignore
    from grouping import normalize_feature_weights  # type: ignore
    from helpers import generate_random_string  # type: ignore
    from job_status_store import (  # type: ignore
        set_job_state,
        get_job_state,
        prune_finished_jobs_older_than,
    )

load_dotenv(Path(__file__).resolve().parent / ".env")


def parse_bool(value: str | None, default: bool = False) -> bool:
    """Parse common env-style truthy values into a boolean."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


REQUIRED_SPOTIFY_SCOPES = {
    "user-read-private",
    "playlist-read-private",
    "playlist-modify-public",
    "playlist-modify-private",
}
SPOTIFY_LOGIN_SCOPE = (
    "user-read-private playlist-read-private "
    "playlist-modify-public playlist-modify-private"
)


def _parse_scope_set(scope_value) -> set[str]:
    """Normalize stored Spotify scope value into a scope-name set."""
    if isinstance(scope_value, str):
        tokens = scope_value.replace(",", " ").split()
        return {token.strip() for token in tokens if token.strip()}

    if isinstance(scope_value, list):
        return {str(token).strip() for token in scope_value if str(token).strip()}

    return set()


def _missing_required_scopes() -> list[str]:
    """Return required Spotify scopes missing from current session."""
    granted_scopes = _parse_scope_set(session.get("auth_scopes"))
    return sorted(REQUIRED_SPOTIFY_SCOPES - granted_scopes)


frontend_url = os.getenv("FRONTEND_URL", "http://127.0.0.1:3000").rstrip("/")
cors_origins = [
    origin.strip()
    for origin in os.getenv("CORS_ORIGINS", frontend_url).split(",")
    if origin.strip()
]
cookie_secure = parse_bool(os.getenv("SESSION_COOKIE_SECURE"), default=False)
cookie_samesite = os.getenv(
    "SESSION_COOKIE_SAMESITE", "None" if cookie_secure else "Lax"
)
cookie_domain = os.getenv("SESSION_COOKIE_DOMAIN")

app = Flask(__name__)

app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY")
app.config["SESSION_PERMANENT"] = False
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=1)
app.config["SESSION_COOKIE_NAME"] = "__session"
app.config["SESSION_COOKIE_HTTPONLY"] = True

app.config["SESSION_COOKIE_SECURE"] = cookie_secure
app.config["SESSION_COOKIE_SAMESITE"] = cookie_samesite
if cookie_domain:
    app.config["SESSION_COOKIE_DOMAIN"] = cookie_domain

CORS(
    app,
    origins=cors_origins,
    supports_credentials=True,
)

JOB_STATUS_TTL_SECONDS = int(os.getenv("JOB_STATUS_TTL_SECONDS", "21600"))


def _clear_auth_session():
    """Clear server-side auth/session values for a stale login state."""
    session.pop("uid", None)
    session.pop("auth_token", None)
    session.pop("refresh_token", None)
    session.pop("auth_scopes", None)


def _unauthorized_session_response(message: str = "Spotify session expired. Please log in again."):
    """Return standardized 401 payload for expired/missing auth sessions."""
    return (
        jsonify(
            {
                "Code": 401,
                "Error": message,
                "reauth": True,
            }
        ),
        401,
    )


def _resolve_active_auth_token():
    """
    Return a valid Spotify auth token for current request.

    Attempts refresh when session token is expired. Returns tuple:
      (auth_token, error_response_or_none)
    """
    auth_token = session.get("auth_token")
    refresh_token = session.get("refresh_token")

    if not auth_token:
        _clear_auth_session()
        return None, _unauthorized_session_response("Authorization required.")

    if is_access_token_valid(auth_token):
        return auth_token, None

    if refresh_token:
        refreshed_token = refresh_access_token(refresh_token)
        if refreshed_token:
            session["auth_token"] = refreshed_token
            return refreshed_token, None

    _clear_auth_session()
    return None, _unauthorized_session_response()


def _prune_old_jobs():
    """Drop old completed jobs to keep status store bounded."""
    cutoff = time.time() - JOB_STATUS_TTL_SECONDS
    prune_finished_jobs_older_than(cutoff)


def _set_job_state(job_id: str, **fields):
    """Update one job status entry in shared status store."""
    set_job_state(job_id, **fields)


def _run_process_playlist_job(
    job_id: str,
    auth_token: str,
    playlist_ids: list[str],
    feature_weights: dict[str, float] | None = None,
    split_criterion: str | None = None,
):
    """Run playlist processing in background and persist status fields."""
    total_playlists = len(playlist_ids)

    def _emit_progress(
        completed_playlists: int,
        total_playlists: int,
        failed_playlists: int = 0,
        last_completed_playlist_id: str | None = None,
        last_completed_playlist_name: str | None = None,
    ):
        safe_total = max(1, int(total_playlists))
        raw_percent = int(round((completed_playlists / safe_total) * 100))
        progress_percent = max(0, min(100, raw_percent))
        _set_job_state(
            job_id,
            completed_playlists=completed_playlists,
            total_playlists=total_playlists,
            failed_playlists=failed_playlists,
            progress_percent=progress_percent,
            last_completed_playlist_id=last_completed_playlist_id,
            last_completed_playlist_name=last_completed_playlist_name,
        )

    _set_job_state(job_id, status="running", started_at=time.time())
    _emit_progress(
        completed_playlists=0,
        total_playlists=total_playlists,
        failed_playlists=0,
    )
    try:
        process_all(
            auth_token,
            playlist_ids,
            feature_weights=feature_weights,
            split_criterion=split_criterion,
            progress_callback=_emit_progress,
        )
        _emit_progress(
            completed_playlists=total_playlists,
            total_playlists=total_playlists,
        )
        _set_job_state(
            job_id,
            status="succeeded",
            finished_at=time.time(),
            error=None,
        )
    except Exception as error:  # pylint: disable=broad-exception-caught
        _set_job_state(
            job_id,
            status="failed",
            finished_at=time.time(),
            error=str(error),
        )


@app.after_request
def add_security_headers(response):
    """Apply baseline security headers for browser-facing responses."""
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    if request.is_secure:
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains; preload"
        )
    return response


@app.route("/login")
@app.route("/api/login")
def login_handler():
    """Start login flow or reuse existing valid session and redirect to frontend."""
    uid = session.get("uid")
    auth_token = session.get("auth_token")
    refresh_token = session.get("refresh_token")

    if uid:
        missing_scopes = _missing_required_scopes()
        if missing_scopes:
            print(
                "Session missing required Spotify scopes.",
                f"missing={','.join(missing_scopes)}",
            )
            return redirect_to_spotify_login()

        if not auth_token:
            return redirect_to_spotify_login()

        if not is_access_token_valid(auth_token):
            if refresh_token:
                new_auth_token = refresh_access_token(refresh_token)
                if not new_auth_token:
                    return redirect_to_spotify_login()
                session["auth_token"] = new_auth_token
                auth_token = new_auth_token
            else:
                return redirect_to_spotify_login()

        return redirect(f"{frontend_url}/input-playlist")
    return redirect_to_spotify_login()


def redirect_to_spotify_login():
    """Build Spotify authorize URL and redirect user to OAuth login."""
    client_id = os.getenv("CLIENT_ID")
    if not client_id:
        return "Missing CLIENT_ID in backend environment", 500

    redirect_uri = get_spotify_redirect_uri()
    if not redirect_uri:
        return "Missing Spotify redirect URI configuration", 500

    state = generate_random_string(16)
    session["oauth_state"] = state

    params = {
        "response_type": "code",
        "client_id": client_id,
        "scope": SPOTIFY_LOGIN_SCOPE,
        "show_dialog": "true",
        "redirect_uri": redirect_uri,
        "state": state,
    }

    url = "https://accounts.spotify.com/authorize?" + urlencode(params)
    return redirect(url)


@app.route("/callback")
@app.route("/api/callback")
def callback_handler():
    """Handle Spotify OAuth callback and persist auth/session cookies."""
    error = request.args.get("error")
    if error:
        return f"Spotify authorization failed: {escape(error)}", 400

    code = request.args.get("code")
    returned_state = request.args.get("state")
    expected_state = session.pop("oauth_state", None)

    if not code:
        return "No code present in callback", 400
    if not expected_state or returned_state != expected_state:
        return "Invalid OAuth state", 400

    token_data = exchange_code_for_token(code)

    if not token_data:
        return "Error exchanging code for token", 500

    auth_token = token_data.get("access_token")
    user_id = get_user_id(auth_token)
    session["uid"] = user_id
    session["auth_token"] = auth_token
    session["refresh_token"] = token_data.get("refresh_token")
    session["auth_scopes"] = sorted(_parse_scope_set(token_data.get("scope", "")))

    return redirect(f"{frontend_url}/input-playlist")


@app.route("/user-playlists")
@app.route("/api/user-playlists")
def get_playlist_handler():
    """Return current user's Spotify playlists based on auth cookie token."""
    auth_token, auth_error = _resolve_active_auth_token()
    if auth_error:
        return auth_error

    playlists = get_all_playlists(auth_token)

    if not playlists:
        if not is_access_token_valid(auth_token):
            _clear_auth_session()
            return _unauthorized_session_response()
        return jsonify({"Code": 502, "Error": "Failed to get playlists"}), 502

    return jsonify(playlists)


@app.route("/process-playlist", methods=["POST"])
@app.route("/api/process-playlist", methods=["POST"])
def process_playlist_handler():
    """Start async processing job for selected playlists."""
    auth_token, auth_error = _resolve_active_auth_token()
    if auth_error:
        return auth_error

    missing_scopes = _missing_required_scopes()
    if missing_scopes:
        return (
            jsonify(
                {
                    "Code": 403,
                    "Error": "Insufficient Spotify scopes. Please re-login.",
                    "missingScopes": missing_scopes,
                }
            ),
            403,
        )

    assert request.json
    playlist_ids = request.json.get("playlistIds", [])
    feature_weights_payload = request.json.get("featureWeights")
    split_criterion_payload = request.json.get("splitCriterion")

    if not playlist_ids:
        return "No playlist IDs provided", 400
    if feature_weights_payload is not None and not isinstance(feature_weights_payload, dict):
        return (
            jsonify(
                {
                    "Code": 400,
                    "Error": "featureWeights must be an object keyed by feature name.",
                }
            ),
            400,
        )
    feature_weights = (
        normalize_feature_weights(feature_weights_payload)
        if isinstance(feature_weights_payload, dict)
        else None
    )
    split_criterion = (
        split_criterion_payload.strip().lower()
        if isinstance(split_criterion_payload, str) and split_criterion_payload.strip()
        else None
    )

    _prune_old_jobs()
    job_id = str(uuid.uuid4())
    _set_job_state(
        job_id,
        status="queued",
        created_at=time.time(),
        finished_at=None,
        error=None,
        playlist_count=len(playlist_ids),
        completed_playlists=0,
        total_playlists=len(playlist_ids),
        failed_playlists=0,
        progress_percent=0,
        last_completed_playlist_id=None,
        last_completed_playlist_name=None,
        feature_weights=feature_weights,
        split_criterion=split_criterion,
    )
    job_thread = threading.Thread(
        target=_run_process_playlist_job,
        args=(job_id, auth_token, playlist_ids, feature_weights, split_criterion),
        daemon=True,
    )
    job_thread.start()

    return jsonify({"jobId": job_id, "status": "queued"}), 202


@app.route("/process-playlist-status/<job_id>")
@app.route("/api/process-playlist-status/<job_id>")
def process_playlist_status_handler(job_id):
    """Return current status for an async playlist processing job."""
    payload = get_job_state(job_id)
    if not payload:
        return jsonify({"Code": 404, "Error": "Job not found"}), 404
    return jsonify(payload), 200


if __name__ == "__main__":
    port = os.getenv("PORT", "8080")
    app.run(host="0.0.0.0", port=int(port))
