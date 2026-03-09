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
    from Backend.helpers import generate_random_string
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
    from helpers import generate_random_string  # type: ignore

load_dotenv(Path(__file__).resolve().parent / ".env")


def parse_bool(value: str | None, default: bool = False) -> bool:
    """Parse common env-style truthy values into a boolean."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


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
_PROCESSING_JOBS: dict[str, dict[str, object]] = {}
_PROCESSING_JOBS_LOCK = threading.Lock()


def get_auth_token_from_request():
    """Return auth token from request cookies, falling back to server session."""
    return session.get("auth_token") or request.cookies.get("auth_token")


def _prune_old_jobs():
    """Drop old completed jobs to keep in-memory status map bounded."""
    cutoff = time.time() - JOB_STATUS_TTL_SECONDS
    with _PROCESSING_JOBS_LOCK:
        old_job_ids = [
            job_id
            for job_id, payload in _PROCESSING_JOBS.items()
            if payload.get("finished_at") and float(payload["finished_at"]) < cutoff
        ]
        for job_id in old_job_ids:
            _PROCESSING_JOBS.pop(job_id, None)


def _set_job_state(job_id: str, **fields):
    """Update one job status entry in a thread-safe way."""
    with _PROCESSING_JOBS_LOCK:
        payload = _PROCESSING_JOBS.get(job_id, {})
        payload.update(fields)
        _PROCESSING_JOBS[job_id] = payload


def _run_process_playlist_job(job_id: str, auth_token: str, playlist_ids: list[str]):
    """Run playlist processing in background and persist status fields."""
    _set_job_state(job_id, status="running", started_at=time.time())
    try:
        process_all(auth_token, playlist_ids)
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
        if not auth_token:
            return redirect_to_spotify_login()

        if not is_access_token_valid(auth_token):
            if refresh_token:
                new_auth_token = refresh_access_token(refresh_token)
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
    scope = "user-read-private playlist-modify-public playlist-read-private"

    params = {
        "response_type": "code",
        "client_id": client_id,
        "scope": scope,
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

    return redirect(f"{frontend_url}/input-playlist")


@app.route("/user-playlists")
@app.route("/api/user-playlists")
def get_playlist_handler():
    """Return current user's Spotify playlists based on auth cookie token."""
    auth_token = get_auth_token_from_request()

    if not auth_token:
        print(f"NO AUTH: {auth_token}")
        return {"Code": 401, "Error": "Authorization token required"}

    playlists = get_all_playlists(auth_token)

    if not playlists:
        return {"Code": 500, "Error": "Failed to get playlists"}

    return jsonify(playlists)


@app.route("/process-playlist", methods=["POST"])
@app.route("/api/process-playlist", methods=["POST"])
def process_playlist_handler():
    """Start async processing job for selected playlists."""
    auth_token = get_auth_token_from_request()

    if not auth_token or not is_access_token_valid(auth_token):
        return "Authorization required", 401

    assert request.json
    playlist_ids = request.json.get("playlistIds", [])

    if not playlist_ids:
        return "No playlist IDs provided", 400

    _prune_old_jobs()
    job_id = str(uuid.uuid4())
    _set_job_state(
        job_id,
        status="queued",
        created_at=time.time(),
        finished_at=None,
        error=None,
        playlist_count=len(playlist_ids),
    )
    job_thread = threading.Thread(
        target=_run_process_playlist_job,
        args=(job_id, auth_token, playlist_ids),
        daemon=True,
    )
    job_thread.start()

    return jsonify({"jobId": job_id, "status": "queued"}), 202


@app.route("/process-playlist-status/<job_id>")
@app.route("/api/process-playlist-status/<job_id>")
def process_playlist_status_handler(job_id):
    """Return current status for an async playlist processing job."""
    with _PROCESSING_JOBS_LOCK:
        payload = _PROCESSING_JOBS.get(job_id)
    if not payload:
        return jsonify({"Code": 404, "Error": "Job not found"}), 404
    return jsonify(payload), 200


if __name__ == "__main__":
    port = os.getenv("PORT", "8080")
    app.run(host="0.0.0.0", port=int(port))
