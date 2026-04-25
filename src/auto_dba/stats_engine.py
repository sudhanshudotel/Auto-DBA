import numpy as np
import scipy.stats as stats
from pydantic import BaseModel
from typing import Dict, List
from .db import fetch

class QueryHistory(BaseModel):
    latencies: List[float] = []
    last_calls: int = 0
    last_total_time: float = 0.0
    query: str = ""

# In-memory store
history_store: Dict[str, QueryHistory] = {}

class HealthReport(BaseModel):
    scanned: int
    anomalies_found: int
    reports: List[Dict]

async def check_database_health() -> HealthReport:
    try:
        rows = await fetch("""
            SELECT queryid, query, calls, total_exec_time 
            FROM pg_stat_statements 
            WHERE calls > 5 
            ORDER BY total_exec_time DESC 
            LIMIT 50
        """)
    except Exception as e:
        if 'relation "pg_stat_statements" does not exist' in str(e):
            return HealthReport(scanned=0, anomalies_found=0, reports=[{"error": "pg_stat_statements not installed"}])
        raise e

    reports = []
    for row in rows:
        qid = str(row['queryid'])
        calls = row['calls']
        total_time = row['total_exec_time']
        query_text = row['query']

        if qid not in history_store:
            history_store[qid] = QueryHistory(last_calls=calls, last_total_time=total_time, query=query_text)
            continue

        qhist = history_store[qid]
        calls_delta = calls - qhist.last_calls
        time_delta = total_time - qhist.last_total_time

        if calls_delta > 0:
            current_latency_x = time_delta / calls_delta
            qhist.latencies.append(current_latency_x)

            if len(qhist.latencies) > 100:
                qhist.latencies.pop(0)

            qhist.last_calls = calls
            qhist.last_total_time = total_time

            arr = np.array(qhist.latencies)
            if len(arr) >= 5:
                mean = np.mean(arr)
                std = np.std(arr) or 0.0001
                z_score = (current_latency_x - mean) / std

                if z_score > 3:
                    reports.append({
                        "query": query_text,
                        "queryid": qid,
                        "anomaly": True,
                        "z_score": round(z_score, 2),
                        "current_latency_ms": round(current_latency_x, 2),
                        "baseline_mean_ms": round(mean, 2),
                        "baseline_stddev_ms": round(std, 2),
                        "p50": round(np.percentile(arr, 50), 2),
                        "p95": round(np.percentile(arr, 95), 2),
                        "p99": round(np.percentile(arr, 99), 2),
                        "message": "Current Latency > \u03bc + (3 * \u03c3)"
                    })

    return HealthReport(
        scanned=len(rows),
        anomalies_found=len(reports),
        reports=reports
    )
