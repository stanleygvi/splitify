"""Shared helpers for Spotify track ID handling."""

import re

SPOTIFY_TRACK_ID_PATTERN = re.compile(r"^[A-Za-z0-9]{22}$")


def is_valid_spotify_track_id(track_id):
    """Return whether the value is a valid 22-char base62 Spotify track ID."""
    return isinstance(track_id, str) and bool(SPOTIFY_TRACK_ID_PATTERN.fullmatch(track_id))


def dedupe_track_ids(track_ids):
    """Return unique track IDs in original order and duplicate count."""
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
