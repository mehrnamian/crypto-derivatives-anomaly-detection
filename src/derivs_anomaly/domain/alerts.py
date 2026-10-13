from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import timedelta

from derivs_anomaly.domain.records import AnomalyScore, MergedAlert


def merge_nearby_alerts(
    scores: Iterable[AnomalyScore],
    *,
    window: timedelta = timedelta(minutes=15),
) -> list[MergedAlert]:
    """Merge adjacent alerts within ``window`` without crossing model versions."""
    if window < timedelta(0):
        raise ValueError("window must not be negative")

    alerts = [score for score in scores if score.is_alert]
    if any(score.minute.tzinfo is None or score.minute.utcoffset() is None for score in alerts):
        raise ValueError("alert timestamps must be timezone-aware")
    if any(not math.isfinite(score.score) for score in alerts):
        raise ValueError("alert scores must be finite")
    alerts.sort(key=lambda score: score.minute)
    if not alerts:
        return []

    merged: list[MergedAlert] = []
    group: list[AnomalyScore] = [alerts[0]]
    for alert in alerts[1:]:
        previous = group[-1]
        if (
            alert.model_version == previous.model_version
            and alert.minute - previous.minute <= window
        ):
            group.append(alert)
            continue
        merged.append(_summarize(group))
        group = [alert]
    merged.append(_summarize(group))
    return merged


def _summarize(group: list[AnomalyScore]) -> MergedAlert:
    strongest = max(group, key=lambda score: score.score)
    return MergedAlert(
        started_at=group[0].minute,
        ended_at=group[-1].minute,
        score=strongest.score,
        model_version=strongest.model_version,
        alert_count=len(group),
    )