"""Clustering utilities for grouping tracks by audio feature similarity."""

import os

import numpy as np
from sklearn.metrics import pairwise_distances
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler
import pandas as pd

AUDIO_FEATURE_KEYS = [
    "acousticness",
    "danceability",
    "energy",
    "instrumentalness",
    "liveness",
    "loudness",
    "speechiness",
    "tempo",
    "valence",
]
MIN_CLUSTER_SIZE = 3
ABSOLUTE_MAX_CLUSTERS = 12
SIMILARITY_WEIGHT = 0.58
TAIL_COHESION_WEIGHT = 0.22
UNIQUENESS_WEIGHT = 0.12
BIC_WEIGHT = 0.05
BALANCE_WEIGHT = 0.03
COHESION_SPLIT_DISTANCE = 2.0
COHESION_IMPROVEMENT_RATIO = 0.92
DEFAULT_GMM_N_INIT = 3
FEATURE_WEIGHTS = {
    "acousticness": 1.10,
    "danceability": 1.35,
    "energy": 1.55,
    "instrumentalness": 0.85,
    "liveness": 0.70,
    "loudness": 0.75,
    "speechiness": 1.35,
    "tempo": 0.85,
    "valence": 1.55,
}


def _env_positive_int(name: str, default_value: int) -> int:
    """Parse positive integer env settings with fallback."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default_value
    parsed_text = raw_value.strip()
    if not parsed_text or not parsed_text.lstrip("-").isdigit():
        return default_value

    parsed_value = int(parsed_text)
    if parsed_value <= 0:
        return 1
    return parsed_value


MAX_K_CANDIDATES = _env_positive_int("CLUSTER_MAX_K_CANDIDATES", 10)
MAX_OUTPUT_PLAYLISTS = _env_positive_int("MAX_OUTPUT_PLAYLISTS", 10)
LARGE_PLAYLIST_TRACK_THRESHOLD = _env_positive_int(
    "CLUSTER_LARGE_PLAYLIST_THRESHOLD", 700
)
LARGE_PLAYLIST_MAX_K_CANDIDATES = _env_positive_int(
    "CLUSTER_LARGE_MAX_K_CANDIDATES", 4
)
LARGE_PLAYLIST_GMM_N_INIT = _env_positive_int("CLUSTER_LARGE_GMM_N_INIT", 1)
REFINE_MAX_TRACKS = _env_positive_int("CLUSTER_REFINE_MAX_TRACKS", 600)


def _merge_small_clusters(
    scaled_features: np.ndarray, labels: np.ndarray, min_cluster_size: int
) -> np.ndarray:
    """Merge tiny clusters into nearest larger clusters."""
    if len(labels) == 0:
        return labels

    label_counts = pd.Series(labels).value_counts()
    if label_counts.empty:
        return labels

    large_labels = label_counts[label_counts >= min_cluster_size].index.tolist()
    if not large_labels:
        return labels

    # Avoid collapsing everything into one giant cluster.
    if len(large_labels) < 2:
        return labels

    centroids = {}
    for label in label_counts.index:
        points = scaled_features[labels == label]
        if len(points) > 0:
            centroids[label] = points.mean(axis=0)

    merged = labels.copy()
    for label, count in label_counts.items():
        if count >= min_cluster_size or label not in centroids:
            continue

        source_centroid = centroids[label]
        candidates = [candidate for candidate in large_labels if candidate in centroids]
        if not candidates:
            continue

        nearest = min(
            candidates,
            key=lambda candidate: np.linalg.norm(source_centroid - centroids[candidate]),
        )
        merged[labels == label] = nearest

    return merged


def _reindex_labels(labels: np.ndarray) -> np.ndarray:
    """Re-map arbitrary cluster IDs to 0..n-1."""
    unique_labels = sorted(set(labels.tolist()))
    mapping = {label: idx for idx, label in enumerate(unique_labels)}
    return np.array([mapping[label] for label in labels])


def _score_bounds(values: list[float], higher_is_better: bool) -> list[float]:
    """Min-max normalize a metric list to 0..1."""
    lower = min(values)
    upper = max(values)
    if upper == lower:
        return [1.0] * len(values)

    if higher_is_better:
        return [(value - lower) / (upper - lower) for value in values]
    return [(upper - value) / (upper - lower) for value in values]


def _evaluate_labels(scaled_features: np.ndarray, labels: np.ndarray) -> dict[str, float]:
    """Return objective metrics for a clustering candidate."""
    unique_labels = sorted(set(labels.tolist()))
    centroids = []
    intra_distances = []
    cluster_sizes = []

    for label in unique_labels:
        points = scaled_features[labels == label]
        if len(points) == 0:
            continue
        centroid = points.mean(axis=0)
        centroids.append(centroid)
        cluster_sizes.append(len(points))
        intra_distances.extend(np.linalg.norm(points - centroid, axis=1).tolist())

    if not centroids or not intra_distances:
        return {
            "intra": float("inf"),
            "intra_p90": float("inf"),
            "inter": 0.0,
            "imbalance": 1.0,
        }

    centroid_array = np.vstack(centroids)
    if len(centroid_array) > 1:
        distances = pairwise_distances(centroid_array, metric="euclidean")
        upper_indices = np.triu_indices(len(centroid_array), k=1)
        inter_distance = float(np.mean(distances[upper_indices]))
    else:
        inter_distance = 0.0

    cluster_sizes_array = np.array(cluster_sizes, dtype=float)
    imbalance = float(cluster_sizes_array.max() / cluster_sizes_array.sum())

    return {
        "intra": float(np.mean(intra_distances)),
        "intra_p90": float(np.percentile(intra_distances, 90)),
        "inter": inter_distance,
        "imbalance": imbalance,
    }


def _candidate_k_values(k_upper_bound: int, track_count: int) -> list[int]:
    """Return a bounded set of k candidates for GMM search."""
    all_values = list(range(2, k_upper_bound + 1))
    if not all_values:
        return []

    max_candidates = MAX_K_CANDIDATES
    if track_count >= LARGE_PLAYLIST_TRACK_THRESHOLD:
        max_candidates = min(max_candidates, LARGE_PLAYLIST_MAX_K_CANDIDATES)
    if len(all_values) <= max_candidates:
        return all_values

    index_values = np.linspace(
        0, len(all_values) - 1, num=max_candidates, dtype=int
    ).tolist()
    selected_values = sorted({all_values[index] for index in index_values})
    if all_values[-1] not in selected_values:
        selected_values.append(all_values[-1])
    return sorted(set(selected_values))


def _refine_cluster_cohesion(
    scaled_features: np.ndarray, labels: np.ndarray, min_cluster_size: int
) -> np.ndarray:
    """Split broad clusters into two when it clearly improves compactness."""
    refined = labels.copy()
    next_label = int(refined.max()) + 1

    for label in sorted(set(refined.tolist())):
        mask = refined == label
        cluster_points = scaled_features[mask]
        cluster_size = len(cluster_points)
        if cluster_size < (2 * min_cluster_size):
            continue

        centroid = cluster_points.mean(axis=0)
        distances = np.linalg.norm(cluster_points - centroid, axis=1)
        mean_distance = float(np.mean(distances))
        if mean_distance <= COHESION_SPLIT_DISTANCE:
            continue

        split_model = GaussianMixture(
            n_components=2,
            covariance_type="diag",
            n_init=2,
            random_state=0,
        )
        split_model.fit(cluster_points)
        sub_labels = split_model.predict(cluster_points)

        count_zero = int(np.sum(sub_labels == 0))
        count_one = int(np.sum(sub_labels == 1))
        if count_zero < min_cluster_size or count_one < min_cluster_size:
            continue

        sub_centroid_zero = cluster_points[sub_labels == 0].mean(axis=0)
        sub_centroid_one = cluster_points[sub_labels == 1].mean(axis=0)
        sub_dist_zero = np.linalg.norm(
            cluster_points[sub_labels == 0] - sub_centroid_zero, axis=1
        )
        sub_dist_one = np.linalg.norm(
            cluster_points[sub_labels == 1] - sub_centroid_one, axis=1
        )
        new_mean_distance = float((np.sum(sub_dist_zero) + np.sum(sub_dist_one)) / cluster_size)

        if new_mean_distance > (mean_distance * COHESION_IMPROVEMENT_RATIO):
            continue

        cluster_indices = np.where(mask)[0]
        refined[cluster_indices[sub_labels == 0]] = label
        refined[cluster_indices[sub_labels == 1]] = next_label
        next_label += 1

    return refined


def cluster_df(track_audio_features: list[dict]) -> pd.DataFrame:
    """Return dataframe with track id and assigned GMM clusters."""
    if not track_audio_features:
        return pd.DataFrame(columns=["id", "cluster"])

    data = pd.DataFrame(track_audio_features)
    if "id" not in data.columns:
        return pd.DataFrame(columns=["id", "cluster"])

    available_keys = [key for key in AUDIO_FEATURE_KEYS if key in data.columns]
    if len(available_keys) < 2:
        return pd.DataFrame(columns=["id", "cluster"])

    feature_frame = data[available_keys].apply(pd.to_numeric, errors="coerce")
    valid_index = feature_frame.dropna().index

    if len(valid_index) == 0:
        return pd.DataFrame(columns=["id", "cluster"])

    feature_frame = feature_frame.loc[valid_index]
    ids = data.loc[valid_index, "id"]

    if len(feature_frame) == 1:
        return pd.DataFrame({"id": ids.values, "cluster": [0]})

    scaler = StandardScaler()
    scaled = scaler.fit_transform(feature_frame)
    weights = np.array([FEATURE_WEIGHTS.get(key, 1.0) for key in available_keys])
    weighted_scaled = scaled * weights

    track_count = len(feature_frame)
    k_upper_bound = min(
        MAX_OUTPUT_PLAYLISTS,
        ABSOLUTE_MAX_CLUSTERS,
        max(2, track_count // MIN_CLUSTER_SIZE),
        track_count - 1,
    )
    if k_upper_bound < 2:
        return pd.DataFrame({"id": ids.values, "cluster": [0] * track_count})

    candidates = []
    gmm_n_init = (
        LARGE_PLAYLIST_GMM_N_INIT
        if track_count >= LARGE_PLAYLIST_TRACK_THRESHOLD
        else DEFAULT_GMM_N_INIT
    )
    candidate_k_values = _candidate_k_values(k_upper_bound, track_count)

    for k in candidate_k_values:
        model = GaussianMixture(
            n_components=k,
            covariance_type="diag",
            n_init=gmm_n_init,
            random_state=0,
        )
        model.fit(weighted_scaled)
        bic = model.bic(weighted_scaled)
        labels = model.predict(weighted_scaled)
        labels = _merge_small_clusters(weighted_scaled, np.array(labels), MIN_CLUSTER_SIZE)
        if track_count <= REFINE_MAX_TRACKS:
            labels = _refine_cluster_cohesion(weighted_scaled, labels, MIN_CLUSTER_SIZE)
        labels = _reindex_labels(labels)
        if len(set(labels.tolist())) < 2:
            continue

        metrics = _evaluate_labels(weighted_scaled, labels)
        candidates.append(
            {
                "labels": labels,
                "bic": float(bic),
                "intra": metrics["intra"],
                "intra_p90": metrics["intra_p90"],
                "inter": metrics["inter"],
                "imbalance": metrics["imbalance"],
            }
        )

    if not candidates:
        return pd.DataFrame({"id": ids.values, "cluster": [0] * track_count})

    intra_scores = _score_bounds(
        [candidate["intra"] for candidate in candidates], higher_is_better=False
    )
    tail_cohesion_scores = _score_bounds(
        [candidate["intra_p90"] for candidate in candidates], higher_is_better=False
    )
    inter_scores = _score_bounds(
        [candidate["inter"] for candidate in candidates], higher_is_better=True
    )
    bic_scores = _score_bounds(
        [candidate["bic"] for candidate in candidates], higher_is_better=False
    )
    balance_scores = _score_bounds(
        [candidate["imbalance"] for candidate in candidates], higher_is_better=False
    )

    best_index = 0
    best_score = float("-inf")
    for index, _ in enumerate(candidates):
        score = (
            SIMILARITY_WEIGHT * intra_scores[index]
            + TAIL_COHESION_WEIGHT * tail_cohesion_scores[index]
            + UNIQUENESS_WEIGHT * inter_scores[index]
            + BIC_WEIGHT * bic_scores[index]
            + BALANCE_WEIGHT * balance_scores[index]
        )
        if score > best_score:
            best_score = score
            best_index = index

    labels = candidates[best_index]["labels"]

    return pd.DataFrame({"id": ids.values, "cluster": labels})
