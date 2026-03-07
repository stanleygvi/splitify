"""General small helpers used by backend services."""

import random
import string


def calc_slices(length):
    """Return number of 100-item API slices needed for a collection length."""
    return (length + 99) // 100 if length > 0 else 0


def convert_to_string(index, track):
    """Format a track row as compact text for logging/debug output."""
    artists = ", ".join(artist["name"] for artist in track["artists"])
    return f"{{{index}: {track['name']}, {artists}}},"


def generate_random_string(length: int) -> str:
    """Generate a random lowercase token with the requested length."""
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for _ in range(length))
    return result_str
