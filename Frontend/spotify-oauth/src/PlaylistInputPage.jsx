import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import './PlaylistInputPage.css';

const API_BASE_URL =
    import.meta.env.VITE_API_BASE_URL ||
    import.meta.env.REACT_APP_API_BASE_URL ||
    '/api';

const DEFAULT_FEATURE_WEIGHTS = Object.freeze({
    acousticness: 1.10,
    danceability: 1.35,
    energy: 1.55,
    instrumentalness: 0.85,
    liveness: 0.70,
    loudness: 0.75,
    speechiness: 1.35,
    tempo: 0.85,
    valence: 1.55,
});

const FEATURE_CONTROLS = [
    {
        key: 'energy',
        label: 'Energy',
        description: 'Push splits toward intensity and momentum.',
    },
    {
        key: 'valence',
        label: 'Mood',
        description: 'Separate bright, positive songs from darker vibes.',
    },
    {
        key: 'danceability',
        label: 'Danceability',
        description: 'Favor rhythmic, groove-focused tracks.',
    },
    {
        key: 'tempo',
        label: 'Tempo',
        description: 'Prioritize pace differences (slow vs fast).',
    },
    {
        key: 'acousticness',
        label: 'Acousticness',
        description: 'Emphasize unplugged and organic textures.',
    },
    {
        key: 'instrumentalness',
        label: 'Instrumental',
        description: 'Push songs with fewer vocals into their own clusters.',
    },
    {
        key: 'speechiness',
        label: 'Speechiness',
        description: 'Separate rap/spoken-word from melodic vocal tracks.',
    },
    {
        key: 'liveness',
        label: 'Liveness',
        description: 'Differentiate live-feel recordings from studio cuts.',
    },
    {
        key: 'loudness',
        label: 'Loudness',
        description: 'Use overall volume profile as a grouping signal.',
    },
];

const CRITERIA_OPTIONS = {
    balanced: {
        label: 'Balanced',
        description: 'Use all criteria with default tuning.',
        boosts: {},
    },
    energy: {
        label: 'Energy',
        description: 'Split mainly by intensity.',
        boosts: { energy: 2.4, loudness: 1.8, tempo: 1.4 },
    },
    valence: {
        label: 'Mood',
        description: 'Split by emotional tone.',
        boosts: { valence: 2.4, acousticness: 1.4 },
    },
    danceability: {
        label: 'Danceability',
        description: 'Split by groove and rhythm.',
        boosts: { danceability: 2.4, energy: 1.6, tempo: 1.4 },
    },
    tempo: {
        label: 'Tempo',
        description: 'Split by pacing differences.',
        boosts: { tempo: 2.5, energy: 1.7 },
    },
    acousticness: {
        label: 'Acousticness',
        description: 'Split by organic vs synthetic sound.',
        boosts: { acousticness: 2.5, instrumentalness: 1.4 },
    },
    instrumentalness: {
        label: 'Instrumental',
        description: 'Split by vocal vs instrumental presence.',
        boosts: { instrumentalness: 2.7, speechiness: 0.7 },
    },
    speechiness: {
        label: 'Speechiness',
        description: 'Split spoken/rap-heavy tracks.',
        boosts: { speechiness: 2.5, danceability: 1.6 },
    },
    liveness: {
        label: 'Liveness',
        description: 'Split by live-performance feel.',
        boosts: { liveness: 2.6, acousticness: 1.4 },
    },
    loudness: {
        label: 'Loudness',
        description: 'Split by overall perceived loudness.',
        boosts: { loudness: 2.5, energy: 1.8 },
    },
};

const MIN_WEIGHT_PERCENT = 0;
const MAX_WEIGHT_PERCENT = 300;
const WEIGHT_STEP_PERCENT = 1;
const AUTH_REDIRECT_DELAY_MS = 1200;

function weightToPercent(weight) {
    return Math.round(Number(weight) * 100);
}

function clampPercent(percent) {
    const numericPercent = Number(percent);
    if (!Number.isFinite(numericPercent)) {
        return MIN_WEIGHT_PERCENT;
    }
    return Math.max(
        MIN_WEIGHT_PERCENT,
        Math.min(MAX_WEIGHT_PERCENT, Math.round(numericPercent))
    );
}

function percentToWeight(percent) {
    return Number((clampPercent(percent) / 100).toFixed(2));
}

function roundedWeights(weights) {
    return Object.fromEntries(
        Object.entries(weights).map(([key, value]) => [key, Number(Number(value).toFixed(2))])
    );
}

function weightsForCriterion(criterionKey) {
    const criterion = CRITERIA_OPTIONS[criterionKey];
    if (!criterion) {
        return { ...DEFAULT_FEATURE_WEIGHTS };
    }
    return {
        ...DEFAULT_FEATURE_WEIGHTS,
        ...criterion.boosts,
    };
}

function clampProgressPercent(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
        return 0;
    }
    return Math.max(0, Math.min(100, Math.round(numeric)));
}

function statusLabel(status) {
    switch (status) {
    case 'queued':
        return 'Queued';
    case 'running':
        return 'Running';
    case 'succeeded':
        return 'Completed';
    case 'failed':
        return 'Failed';
    default:
        return 'Working';
    }
}

async function parseResponsePayload(response) {
    const contentType = response.headers.get('content-type') || '';

    if (contentType.includes('application/json')) {
        try {
            return await response.json();
        } catch {
            return null;
        }
    }

    const text = await response.text();
    if (!text) {
        return null;
    }
    return { Error: text };
}

function getApiErrorMessage(response, payload, fallbackMessage) {
    const payloadError = (
        (payload && typeof payload === 'object' && (payload.Error || payload.error || payload.message))
        || ''
    );
    if (payloadError) {
        return String(payloadError);
    }
    return `${fallbackMessage} (${response.status} ${response.statusText})`;
}

function shouldReauthenticate(response, payload) {
    if (!response) {
        return false;
    }
    if (response.status === 401) {
        return true;
    }

    const payloadCode = Number(payload?.Code);
    if (payloadCode === 401) {
        return true;
    }

    if (payload?.reauth === true) {
        return true;
    }

    if (response.status === 403) {
        if (Array.isArray(payload?.missingScopes) && payload.missingScopes.length > 0) {
            return true;
        }
        const errorText = String(payload?.Error || payload?.error || '').toLowerCase();
        if (errorText.includes('re-login') || errorText.includes('reauth')) {
            return true;
        }
    }

    return false;
}

function PlaylistInputPage() {
    const [playlists, setPlaylists] = useState([]);
    const [selectedPlaylists, setSelectedPlaylists] = useState([]);
    const [isProcessing, setIsProcessing] = useState(false);
    const [featureWeights, setFeatureWeights] = useState({ ...DEFAULT_FEATURE_WEIGHTS });
    const [activeCriterion, setActiveCriterion] = useState('balanced');
    const [isAdvancedOpen, setIsAdvancedOpen] = useState(false);
    const [jobProgress, setJobProgress] = useState(null);
    const [selectionNotice, setSelectionNotice] = useState('');
    const [authNotice, setAuthNotice] = useState('');
    const authRedirectTimeoutRef = useRef(null);

    const playlistNameById = useMemo(() => (
        Object.fromEntries(
            playlists
            .filter(playlist => playlist && playlist.id)
            .map(playlist => [playlist.id, playlist.name || playlist.id])
        )
    ), [playlists]);

    const startReauthFlow = useCallback((message = 'Spotify session expired. Redirecting to login...') => {
        setAuthNotice(message);
        setSelectionNotice('');
        setIsProcessing(false);
        if (authRedirectTimeoutRef.current) {
            return;
        }
        authRedirectTimeoutRef.current = window.setTimeout(() => {
            window.location.href = `${API_BASE_URL}/login`;
        }, AUTH_REDIRECT_DELAY_MS);
    }, []);

    useEffect(() => () => {
        if (authRedirectTimeoutRef.current) {
            window.clearTimeout(authRedirectTimeoutRef.current);
            authRedirectTimeoutRef.current = null;
        }
    }, []);

    useEffect(() => {
        fetch(`${API_BASE_URL}/user-playlists`, {
            credentials: 'include',
        })
        .then(async response => {
            const payload = await parseResponsePayload(response);
            if (shouldReauthenticate(response, payload)) {
                startReauthFlow('Your Spotify session expired. Redirecting to login...');
                return null;
            }
            if (!response.ok) {
                throw new Error(
                    getApiErrorMessage(response, payload, 'Failed to fetch playlists')
                );
            }
            return payload;
        })
        .then(data => {
            if (!data) {
                return;
            }
            if (data && data.items) {
                setPlaylists(data.items);
            } else {
                throw new Error('Unexpected data structure from server');
            }
        })
        .catch(error => {
            if (authRedirectTimeoutRef.current) {
                return;
            }
            console.error("There was an error fetching the playlists:", error);
            setSelectionNotice(error.message || 'Unable to load playlists right now.');
        });
    }, [startReauthFlow]);

    const applyCriterion = (criterionKey) => {
        if (!CRITERIA_OPTIONS[criterionKey]) {
            return;
        }
        setFeatureWeights(weightsForCriterion(criterionKey));
        setActiveCriterion(criterionKey);
    };

    const resetWeights = () => {
        applyCriterion('balanced');
    };

    const handleWeightChange = (featureKey, nextPercent) => {
        setFeatureWeights(prev => ({
            ...prev,
            [featureKey]: percentToWeight(nextPercent),
        }));
        setActiveCriterion('custom');
    };

    const toggleAdvanced = () => {
        setIsAdvancedOpen((prev) => !prev);
    };

    const handlePlaylistSelection = (id) => {
        setSelectionNotice('');
        if (selectedPlaylists.includes(id)) {
            setSelectedPlaylists(prev => prev.filter(playlistId => playlistId !== id));
        } else {
            setSelectedPlaylists(prev => [...prev, id]);
        }
    };

    const clearSelectedPlaylists = () => {
        setSelectionNotice('');
        setSelectedPlaylists([]);
    };

    const handleProcessPlaylists = () => {
        console.log("Selected Playlists:", selectedPlaylists);
        if (!selectedPlaylists.length) {
            setSelectionNotice('Select at least one playlist to start processing.');
            return;
        }
        setAuthNotice('');
        setSelectionNotice('');
        setIsProcessing(true);
        setJobProgress({
            status: 'queued',
            jobId: null,
            completedPlaylists: 0,
            totalPlaylists: selectedPlaylists.length,
            failedPlaylists: 0,
            progressPercent: 0,
            lastCompletedPlaylistId: null,
            lastCompletedPlaylistName: null,
            error: null,
        });

        fetch(`${API_BASE_URL}/process-playlist`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            credentials: 'include',
            body: JSON.stringify({
                playlistIds: selectedPlaylists,
                featureWeights: roundedWeights(featureWeights),
                splitCriterion: activeCriterion,
            })
        })
        .then(async response => {
            const payload = await parseResponsePayload(response);
            if (shouldReauthenticate(response, payload)) {
                startReauthFlow(
                    response.status === 403
                        ? 'Spotify permissions changed. Redirecting to login...'
                        : 'Spotify session expired. Redirecting to login...'
                );
                return null;
            }
            if (!response.ok) {
                throw new Error(getApiErrorMessage(response, payload, 'Failed to start processing'));
            }
            return payload;
        })
        .then(data => {
            if (!data) {
                return null;
            }
            if (!data || !data.jobId) {
                throw new Error("Missing jobId from backend response");
            }
            console.log("Started processing job:", data.jobId);
            setJobProgress(prev => ({
                ...(prev || {}),
                status: data.status || 'queued',
                jobId: data.jobId,
            }));
            return data.jobId;
        })
        .then(jobId => {
            if (!jobId) {
                return null;
            }
            const pollDelayMs = 4000;
            const pollJob = () => fetch(
                `${API_BASE_URL}/process-playlist-status/${jobId}`,
                { credentials: 'include' }
            )
            .then(async response => {
                const payload = await parseResponsePayload(response);
                if (response.status === 404) {
                    setTimeout(pollJob, pollDelayMs);
                    return null;
                }
                if (shouldReauthenticate(response, payload)) {
                    startReauthFlow('Your Spotify session expired. Redirecting to login...');
                    return null;
                }
                if (!response.ok) {
                    throw new Error(getApiErrorMessage(response, payload, 'Failed to fetch job status'));
                }
                return payload;
            })
            .then(statusPayload => {
                if (!statusPayload) {
                    return;
                }
                const status = statusPayload.status;
                const totalPlaylists = Number(
                    statusPayload.total_playlists
                    ?? statusPayload.playlist_count
                    ?? selectedPlaylists.length
                );
                const completedPlaylists = Number(statusPayload.completed_playlists ?? 0);
                const failedPlaylists = Number(statusPayload.failed_playlists ?? 0);
                const derivedProgress = (
                    totalPlaylists > 0
                        ? Math.round((completedPlaylists / totalPlaylists) * 100)
                        : (status === 'succeeded' ? 100 : 0)
                );
                const progressPercent = clampProgressPercent(
                    statusPayload.progress_percent ?? derivedProgress
                );
                const lastCompletedPlaylistId = (
                    statusPayload.last_completed_playlist_id || null
                );
                const reportedPlaylistName = (
                    typeof statusPayload.last_completed_playlist_name === 'string'
                        ? statusPayload.last_completed_playlist_name
                        : null
                );
                const lastCompletedPlaylistName = (
                    reportedPlaylistName
                    || (lastCompletedPlaylistId ? playlistNameById[lastCompletedPlaylistId] : null)
                );
                setJobProgress(prev => ({
                    ...(prev || {}),
                    status,
                    jobId,
                    completedPlaylists: Number.isFinite(completedPlaylists)
                        ? completedPlaylists
                        : 0,
                    totalPlaylists: Number.isFinite(totalPlaylists) && totalPlaylists > 0
                        ? totalPlaylists
                        : selectedPlaylists.length,
                    failedPlaylists: Number.isFinite(failedPlaylists) ? failedPlaylists : 0,
                    progressPercent,
                    lastCompletedPlaylistId,
                    lastCompletedPlaylistName,
                    error: statusPayload.error || null,
                }));
                if (status === "succeeded") {
                    setIsProcessing(false);
                    return;
                }
                if (status === "failed") {
                    setIsProcessing(false);
                    return;
                }
                setTimeout(pollJob, pollDelayMs);
            });

            return pollJob();
        })
        .catch(error => {
            if (authRedirectTimeoutRef.current) {
                return;
            }
            console.error("There was a problem with the fetch operation:", error);
            setJobProgress(prev => ({
                ...(prev || {}),
                status: 'failed',
                error: error.message || 'Unexpected error',
            }));
            setIsProcessing(false);
        });
    };

    return (
        <div className="playlist-page">
            <h2 className="playlist-title">Select Playlists to Process</h2>
            <p className="playlist-subtitle">
                Tune how tracks are split by audio traits, then choose one or more playlists.
            </p>
            {authNotice && (
                <p className="auth-notice" role="status" aria-live="polite">{authNotice}</p>
            )}
            <section className="weights-panel">
                <div className="weights-header">
                    <h3>Split Criteria</h3>
                    <button
                        type="button"
                        className="weights-reset-button"
                        onClick={resetWeights}
                        disabled={isProcessing}
                    >
                        Reset to Balanced
                    </button>
                </div>
                <p className="weights-note">
                    Pick a primary criterion first. Use Advanced only if you want manual tuning.
                </p>
                <div className="criteria-row">
                    {Object.entries(CRITERIA_OPTIONS).map(([criterionKey, criterion]) => (
                        <button
                            key={criterionKey}
                            type="button"
                            className={`criteria-button ${activeCriterion === criterionKey ? 'active' : ''}`}
                            onClick={() => applyCriterion(criterionKey)}
                            disabled={isProcessing}
                        >
                            <span className="criteria-label">{criterion.label}</span>
                            <span className="criteria-description">{criterion.description}</span>
                        </button>
                    ))}
                </div>
                <div className="advanced-toggle-row">
                    <button
                        type="button"
                        className={`advanced-toggle-button ${isAdvancedOpen ? 'open' : ''}`}
                        onClick={toggleAdvanced}
                        disabled={isProcessing}
                    >
                        {isAdvancedOpen ? 'Hide Advanced Sliders' : 'Show Advanced Sliders'}
                    </button>
                    <span className="advanced-summary">
                        {activeCriterion === 'custom'
                            ? 'Custom weighting active'
                            : `${CRITERIA_OPTIONS[activeCriterion].label} preset active`}
                    </span>
                </div>
                {isAdvancedOpen && (
                    <ul className="weights-list">
                        {FEATURE_CONTROLS.map(control => {
                            const sliderId = `weight-${control.key}`;
                            const currentWeight = featureWeights[control.key];
                            const currentPercent = clampPercent(weightToPercent(currentWeight));
                            const sliderFillPercent = (currentPercent / MAX_WEIGHT_PERCENT) * 100;
                            return (
                                <li key={control.key} className="weight-item">
                                    <label htmlFor={sliderId} className="weight-label">
                                        <span className="weight-name">{control.label}</span>
                                        <span className="weight-hint">{control.description}</span>
                                    </label>
                                    <div className="weight-input-row">
                                        <span className="weight-bound">0%</span>
                                        <input
                                            id={sliderId}
                                            className="weight-slider"
                                            type="range"
                                            min={MIN_WEIGHT_PERCENT}
                                            max={MAX_WEIGHT_PERCENT}
                                            step={WEIGHT_STEP_PERCENT}
                                            value={currentPercent}
                                            style={{ '--slider-fill': `${sliderFillPercent}%` }}
                                            onChange={(event) =>
                                                handleWeightChange(control.key, event.target.value)
                                            }
                                            onPointerDown={(event) => event.stopPropagation()}
                                            onClick={(event) => event.stopPropagation()}
                                            disabled={isProcessing}
                                        />
                                        <span className="weight-bound">300%</span>
                                        <span className="weight-value">{currentPercent}%</span>
                                    </div>
                                </li>
                            );
                        })}
                    </ul>
                )}
            </section>
            <ul className="playlist-list">
                {playlists.map(playlist => {
                    const isSelected = selectedPlaylists.includes(playlist.id);
                    const imageUrl = playlist.images?.[0]?.url;
                    const trackCount = playlist.tracks?.total ?? 0;
                    return (
                        <li
                            key={playlist.id}
                            className={`playlist-card ${isSelected ? 'selected' : ''}`}
                        >
                            <button
                                type="button"
                                className="playlist-card-button"
                                onClick={() => handlePlaylistSelection(playlist.id)}
                                aria-pressed={isSelected}
                            >
                                <div className="playlist-image-frame">
                                    {imageUrl ? (
                                        <img
                                            className="playlist-image"
                                            src={imageUrl}
                                            alt={`${playlist.name} cover`}
                                            width={300}
                                            height={300}
                                            loading="lazy"
                                        />
                                    ) : (
                                        <div className="playlist-fallback-image" aria-hidden="true">
                                            {playlist.name?.trim()?.[0]?.toUpperCase() || '♪'}
                                        </div>
                                    )}
                                    <span className="playlist-selected-pill">Selected</span>
                                    <div className="playlist-overlay">
                                        <p className="playlist-name">{playlist.name}</p>
                                        <p className="playlist-stats">{trackCount} tracks</p>
                                    </div>
                                </div>
                            </button>
                        </li>
                    );
                })}
            </ul>
            <div className="playlist-actions-row">
                <button
                    type="button"
                    className="clear-selection-button"
                    onClick={clearSelectedPlaylists}
                    disabled={isProcessing || selectedPlaylists.length === 0}
                >
                    Clear Selection
                </button>
                <span className="selection-count">
                    {selectedPlaylists.length} playlist
                    {selectedPlaylists.length === 1 ? '' : 's'} selected
                </span>
            </div>
            {selectionNotice && (
                <p className="selection-notice">{selectionNotice}</p>
            )}
            <button
                className="process-button"
                onClick={handleProcessPlaylists}
                disabled={isProcessing || selectedPlaylists.length === 0}
            >
                {isProcessing ? "Processing..." : "Process Selected Playlists"}
            </button>
            {jobProgress && (
                <section className="progress-panel" aria-live="polite">
                    <div className="progress-header-row">
                        <h3 className="progress-title">Processing Progress</h3>
                        <span className={`progress-status-pill ${jobProgress.status || 'running'}`}>
                            {statusLabel(jobProgress.status)}
                        </span>
                    </div>
                    <p className="progress-meta">
                        {jobProgress.completedPlaylists || 0}
                        {' / '}
                        {jobProgress.totalPlaylists || 0}
                        {' playlists completed'}
                        {(jobProgress.failedPlaylists || 0) > 0
                            ? ` • ${jobProgress.failedPlaylists} failed`
                            : ''}
                    </p>
                    <div className="progress-track" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={jobProgress.progressPercent || 0}>
                        <div
                            className="progress-fill"
                            style={{ width: `${jobProgress.progressPercent || 0}%` }}
                        />
                    </div>
                    <p className="progress-percent">{jobProgress.progressPercent || 0}%</p>
                    {jobProgress.lastCompletedPlaylistName && (
                        <p className="progress-last-item">
                            Last completed: {jobProgress.lastCompletedPlaylistName}
                        </p>
                    )}
                    {jobProgress.error && (
                        <p className="progress-error">{jobProgress.error}</p>
                    )}
                    {jobProgress.status === 'succeeded' && (
                        <div className="progress-complete">
                            <p className="progress-complete-title">All playlists are ready.</p>
                            <p className="progress-complete-text">
                                Your split playlists were created in Spotify. If they do not appear
                                immediately, refresh your Spotify app.
                            </p>
                        </div>
                    )}
                </section>
            )}
        </div>
    );
}

export default PlaylistInputPage;
