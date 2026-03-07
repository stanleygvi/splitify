"""Cluster explanation and small-cluster cohesion helpers."""

import numpy as np

LABEL_FEATURES = [
    "energy",
    "danceability",
    "valence",
    "acousticness",
    "instrumentalness",
    "speechiness",
]

POSITIVE_LABELS = {
    "energy": "High Energy",
    "danceability": "Danceable",
    "valence": "Upbeat",
    "acousticness": "Acoustic",
    "instrumentalness": "Instrumental",
    "speechiness": "Speech-Heavy",
}

NEGATIVE_LABELS = {
    "energy": "Low Energy",
    "danceability": "Less Danceable",
    "valence": "Moody",
    "acousticness": "Electronic",
    "instrumentalness": "Vocal-Forward",
    "speechiness": "Melodic",
}


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_feature_means(track_ids, feature_by_track_id):
    """Compute per-feature means for a set of track IDs."""
    means = {}
    for feature in LABEL_FEATURES:
        values = []
        for track_id in track_ids:
            row = feature_by_track_id.get(track_id, {})
            numeric = _to_float(row.get(feature))
            if numeric is not None:
                values.append(numeric)
        means[feature] = sum(values) / len(values) if values else None
    return means


def build_cluster_reason(cluster_means, global_means):
    """Return a short label describing the strongest cluster-vs-global trait deltas."""
    ranked = []
    for feature in LABEL_FEATURES:
        cluster_value = cluster_means.get(feature)
        global_value = global_means.get(feature)
        if cluster_value is None or global_value is None:
            continue
        delta = cluster_value - global_value
        label = POSITIVE_LABELS[feature] if delta > 0 else NEGATIVE_LABELS[feature]
        ranked.append((abs(delta), label))

    if not ranked:
        return "Feature-Similar"

    ranked.sort(key=lambda item: item[0], reverse=True)
    return " + ".join(label for _, label in ranked[:2])


def build_cluster_trait_summary(cluster_means, global_means):
    """Return a longer comma-separated list of trait drivers for a cluster."""
    ranked = []
    for feature in LABEL_FEATURES:
        cluster_value = cluster_means.get(feature)
        global_value = global_means.get(feature)
        if cluster_value is None or global_value is None:
            continue
        delta = cluster_value - global_value
        label = POSITIVE_LABELS[feature] if delta > 0 else NEGATIVE_LABELS[feature]
        ranked.append((abs(delta), label))

    if not ranked:
        return "No strong trait deltas detected"

    ranked.sort(key=lambda item: item[0], reverse=True)
    strong = [label for delta, label in ranked if delta >= 0.06][:3]
    if not strong:
        strong = [label for _, label in ranked[:3]]
    return ", ".join(strong)


def _track_feature_vector(track_id, feature_by_track_id):
    row = feature_by_track_id.get(track_id, {})
    energy = _to_float(row.get("energy"))
    danceability = _to_float(row.get("danceability"))
    valence = _to_float(row.get("valence"))
    acousticness = _to_float(row.get("acousticness"))
    instrumentalness = _to_float(row.get("instrumentalness"))
    liveness = _to_float(row.get("liveness"))
    loudness = _to_float(row.get("loudness"))
    speechiness = _to_float(row.get("speechiness"))
    tempo = _to_float(row.get("tempo"))
    if None in (
        energy,
        danceability,
        valence,
        acousticness,
        instrumentalness,
        liveness,
        loudness,
        speechiness,
        tempo,
    ):
        return None

    loudness_norm = min(max((loudness + 60.0) / 60.0, 0.0), 1.0)
    return np.array(
        [
            energy,
            danceability,
            valence,
            acousticness,
            instrumentalness,
            liveness,
            loudness_norm,
            speechiness,
            min(max(tempo / 200.0, 0.0), 1.5),
        ],
        dtype=float,
    )


def small_cluster_is_cohesive(track_ids, feature_by_track_id, max_mean_distance=0.52):
    """Reject tiny clusters that are too far apart in core vibe features."""
    if len(track_ids) <= 1:
        return False

    vectors = []
    for track_id in track_ids:
        vector = _track_feature_vector(track_id, feature_by_track_id)
        if vector is None:
            return False
        vectors.append(vector)

    matrix = np.vstack(vectors)
    distances = []
    for i, row in enumerate(matrix):
        for j in range(i + 1, len(matrix)):
            distances.append(float(np.linalg.norm(row - matrix[j])))
    if not distances:
        return False
    return (sum(distances) / len(distances)) <= max_mean_distance
