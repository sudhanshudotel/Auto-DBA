"""Z-score anomaly detector over `pg_stat_statements`.

State is persisted in `auto_dba.query_history` (see stats_store.py); restarts
no longer wipe the rolling latency window.
"""

import logging
from typing import Dict, List

import numpy as np
from pydantic import BaseModel

from . import stats_store
from .db import fetch

logger = logging.getLogger(__name__)

Z_SCORE_THRESHOLD = 3.0
MIN_SAMPLES_FOR_Z = 5


class HealthReport(BaseModel):
    scanned: int
    anomalies_found: int
    reports: List[Dict]


async def check_database_health() -> HealthReport:
    try:
        rows = await fetch(
            """
            SELECT queryid, query, calls, total_exec_time
            FROM pg_stat_statements
            WHERE calls > 5
            ORDER BY total_exec_time DESC
            LIMIT 50
            """
        )
    except Exception as exc:
        if 'relation "pg_stat_statements" does not exist' in str(exc):
            return HealthReport(
                scanned=0,
                anomalies_found=0,
                reports=[{"error": "pg_stat_statements not installed"}],
            )
        raise

    reports: List[Dict] = []

    for row in rows:
        qid = str(row["queryid"])
        calls: int = row["calls"]
        total_time: float = row["total_exec_time"]
        query_text: str = row["query"]

        history = await stats_store.load_history(qid)
        if history is None:
            await stats_store.seed_history(qid, query_text, calls, total_time)
            continue

        calls_delta = calls - history.last_calls
        time_delta = total_time - history.last_total_time
        if calls_delta <= 0:
            continue

        current_latency = time_delta / calls_delta
        latencies = await stats_store.append_latency(qid, calls, total_time, current_latency)

        if len(latencies) < MIN_SAMPLES_FOR_Z:
            continue

        # Use the prior window (excluding the just-appended sample) as the baseline,
        # so the new sample is compared to history rather than to itself.
        baseline = np.array(latencies[:-1])
        mean = float(np.mean(baseline))
        std = float(np.std(baseline)) or 0.0001
        z_score = (current_latency - mean) / std
        if z_score <= Z_SCORE_THRESHOLD:
            continue

        full = np.array(latencies)
        reports.append(
            {
                "query": query_text,
                "queryid": qid,
                "anomaly": True,
                "z_score": round(z_score, 2),
                "current_latency_ms": round(current_latency, 2),
                "baseline_mean_ms": round(mean, 2),
                "baseline_stddev_ms": round(std, 2),
                "p50": round(float(np.percentile(full, 50)), 2),
                "p95": round(float(np.percentile(full, 95)), 2),
                "p99": round(float(np.percentile(full, 99)), 2),
                "message": f"Current Latency > mu + ({Z_SCORE_THRESHOLD} * sigma)",
            }
        )

    return HealthReport(
        scanned=len(rows),
        anomalies_found=len(reports),
        reports=reports,
    )
