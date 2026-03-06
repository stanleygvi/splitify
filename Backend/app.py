import os
from pathlib import Path
from flask import Flask, request, redirect, jsonify, make_response, session
from datetime import timedelta
from flask_cors import CORS
from urllib.parse import urlencode
from dotenv import load_dotenv
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

load_dotenv(Path(__file__).resolve().parent / ".env")

def parse_bool(value: str | None, default: bool = False) -> bool:
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

app.config["SESSION_COOKIE_SECURE"] = cookie_secure
app.config["SESSION_COOKIE_SAMESITE"] = cookie_samesite
if cookie_domain:
    app.config["SESSION_COOKIE_DOMAIN"] = cookie_domain

CORS(
    app,
    origins=cors_origins,
    supports_credentials=True,
)


@app.route("/login")
def login_handler():
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

        response = make_response(redirect(f"{frontend_url}/input-playlist"))
        cookie_options = {
            "httponly": True,
            "secure": cookie_secure,
            "samesite": cookie_samesite,
        }
        if cookie_domain:
            cookie_options["domain"] = cookie_domain
        response.set_cookie("auth_token", auth_token, **cookie_options)
        return response
    return redirect_to_spotify_login()


def redirect_to_spotify_login():
    client_id = os.getenv("CLIENT_ID")
    if not client_id:
        return "Missing CLIENT_ID in backend environment", 500

    redirect_uri = get_spotify_redirect_uri()
    if not redirect_uri:
        return "Missing Spotify redirect URI configuration", 500

    state = generate_random_string(16)
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
def callback_handler():
    code = request.args.get("code")

    if not code:
        return "No code present in callback", 400

    token_data = exchange_code_for_token(code)

    if not token_data:
        return "Error exchanging code for token", 500

    auth_token = token_data.get("access_token")
    user_id = get_user_id(auth_token)
    session["uid"] = user_id
    session["auth_token"] = auth_token
    session["refresh_token"] = token_data.get("refresh_token")

    response = make_response(redirect(f"{frontend_url}/input-playlist"))
    cookie_options = {
        "httponly": True,
        "secure": cookie_secure,
        "samesite": cookie_samesite,
    }
    if cookie_domain:
        cookie_options["domain"] = cookie_domain
    response.set_cookie("auth_token", auth_token, **cookie_options)

    return response


@app.route("/user-playlists")
def get_playlist_handler():
    auth_token = request.cookies.get("auth_token")

    if not auth_token:
        print(f"NO AUTH: {auth_token}")
        return {"Code": 401, "Error": "Authorization token required"}

    playlists = get_all_playlists(auth_token)

    if not playlists:
        return {"Code": 500, "Error": "Failed to get playlists"}

    return jsonify(playlists)


@app.route("/process-playlist", methods=["POST"])
def process_playlist_handler():

    auth_token = request.cookies.get("auth_token")

    if not auth_token or not is_access_token_valid(auth_token):
        return "Authorization required", 401

    assert request.json
    playlist_ids = request.json.get("playlistIds", [])

    if not playlist_ids:
        return "No playlist IDs provided", 400

    process_all(auth_token, playlist_ids)

    return jsonify({"message": "Playlists processed successfully!"}), 200


if __name__ == "__main__":
    port = os.getenv("PORT", "8080")
    app.run(host="0.0.0.0", port=int(port))
