"""Postgres-backed persistent state for Auto-DBA.

Owns two tables under the `auto_dba` schema:

- `auto_dba.query_history` — rolling latency baselines per `pg_stat_statements.queryid`
- `auto_dba.optimizations` — applied DDLs awaiting + completed verification

Both tables are created idempotently on startup via `bootstrap()`.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel

from .db import execute, fetch

LATENCY_WINDOW = 100


SCHEMA_DDL = [
    "CREATE SCHEMA IF NOT EXISTS auto_dba",
    """
    CREATE TABLE IF NOT EXISTS auto_dba.query_history (
        queryid          TEXT PRIMARY KEY,
        query            TEXT NOT NULL,
        last_calls       BIGINT NOT NULL,
        last_total_time  DOUBLE PRECISION NOT NULL,
        latencies        DOUBLE PRECISION[] NOT NULL DEFAULT '{}',
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS auto_dba.optimizations (
        id                       SERIAL PRIMARY KEY,
        queryid                  TEXT NOT NULL,
        ddl                      TEXT NOT NULL,
        expected_improvement_pct REAL NOT NULL,
        baseline_p95_ms          REAL,
        applied_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
        verified_at              TIMESTAMPTZ,
        actual_improvement_pct   REAL
    )
    """,
]


async def bootstrap() -> None:
    """Create schema + tables if missing. Safe to run on every startup."""
    for stmt in SCHEMA_DDL:
        await execute(stmt)


class QueryHistoryRow(BaseModel):
    queryid: str
    query: str
    last_calls: int
    last_total_time: float
    latencies: list[float]


async def load_history(queryid: str) -> Optional[QueryHistoryRow]:
    rows = await fetch(
        """
        SELECT queryid, query, last_calls, last_total_time, latencies
        FROM auto_dba.query_history WHERE queryid = $1
        """,
        queryid,
    )
    if not rows:
        return None
    r = rows[0]
    return QueryHistoryRow(
        queryid=r["queryid"],
        query=r["query"],
        last_calls=r["last_calls"],
        last_total_time=r["last_total_time"],
        latencies=list(r["latencies"] or []),
    )


async def seed_history(queryid: str, query: str, calls: int, total_time: float) -> None:
    """First-time observation: record current counters, no latencies yet."""
    await execute(
        """
        INSERT INTO auto_dba.query_history (queryid, query, last_calls, last_total_time, latencies)
        VALUES ($1, $2, $3, $4, '{}')
        ON CONFLICT (queryid) DO NOTHING
        """,
        queryid,
        query,
        calls,
        total_time,
    )


async def append_latency(
    queryid: str, calls: int, total_time: float, latency_ms: float
) -> list[float]:
    """Append one latency sample (capped at LATENCY_WINDOW) and bump counters.

    Returns the post-update latency window — used by the caller for Z-score math
    without a second round-trip.
    """
    rows = await fetch(
        """
        UPDATE auto_dba.query_history
        SET last_calls       = $2,
            last_total_time  = $3,
            latencies        = (
                CASE WHEN array_length(latencies, 1) >= $5
                     THEN latencies[2:array_length(latencies, 1)]
                     ELSE latencies
                END
            ) || ARRAY[$4::double precision],
            updated_at       = now()
        WHERE queryid = $1
        RETURNING latencies
        """,
        queryid,
        calls,
        total_time,
        latency_ms,
        LATENCY_WINDOW,
    )
    return list(rows[0]["latencies"]) if rows else []


class OptimizationRecord(BaseModel):
    id: int
    queryid: str
    ddl: str
    expected_improvement_pct: float
    baseline_p95_ms: Optional[float]
    actual_improvement_pct: Optional[float]


async def record_optimization(
    queryid: str, ddl: str, expected_improvement_pct: float, baseline_p95_ms: Optional[float]
) -> int:
    rows = await fetch(
        """
        INSERT INTO auto_dba.optimizations
            (queryid, ddl, expected_improvement_pct, baseline_p95_ms)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        queryid,
        ddl,
        expected_improvement_pct,
        baseline_p95_ms,
    )
    return int(rows[0]["id"])


async def load_optimization(opt_id: int) -> Optional[OptimizationRecord]:
    rows = await fetch(
        """
        SELECT id, queryid, ddl, expected_improvement_pct, baseline_p95_ms, actual_improvement_pct
        FROM auto_dba.optimizations WHERE id = $1
        """,
        opt_id,
    )
    if not rows:
        return None
    r = rows[0]
    return OptimizationRecord(
        id=r["id"],
        queryid=r["queryid"],
        ddl=r["ddl"],
        expected_improvement_pct=r["expected_improvement_pct"],
        baseline_p95_ms=r["baseline_p95_ms"],
        actual_improvement_pct=r["actual_improvement_pct"],
    )


async def clear_latency_window(queryid: str) -> None:
    """Reset the rolling latency window for one queryid.

    Called at DDL apply time so post-DDL samples accumulate against an empty
    baseline — this lets `verify_optimization_tool` compare cleanly against
    `baseline_p95_ms` (snapshotted into auto_dba.optimizations before the clear).
    """
    await execute(
        """
        UPDATE auto_dba.query_history
        SET latencies = '{}', updated_at = now()
        WHERE queryid = $1
        """,
        queryid,
    )


async def record_verification(opt_id: int, actual_improvement_pct: float) -> None:
    await execute(
        """
        UPDATE auto_dba.optimizations
        SET actual_improvement_pct = $2,
            verified_at            = now()
        WHERE id = $1
        """,
        opt_id,
        actual_improvement_pct,
    )
