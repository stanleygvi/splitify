from sklearn.cluster import KMeans
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


def calc_clusters(track_count: int, default_clusters: int = 3) -> int:
    """Use up to 3 clusters, but never more clusters than tracks."""
    if track_count <= 0:
        return 0
    return min(default_clusters, track_count)


def cluster_df(track_audio_features: list[dict]) -> pd.DataFrame:
    """Return dataframe with track id and assigned K-means cluster."""
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

    clusters = calc_clusters(len(feature_frame))
    model = KMeans(n_clusters=clusters, random_state=0, n_init="auto")
    labels = model.fit_predict(scaled)

    return pd.DataFrame({"id": ids.values, "cluster": labels})
