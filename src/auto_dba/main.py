import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Optional

import numpy as np
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field

from . import stats_store
from .analyst import OptimizationPlan, get_optimization_plan
from .db import TIMEOUT_DDL, close_pool, execute, get_pool
from .guardrail import SimulationResult, simulate_impact
from .logging import configure as configure_logging
from .logging import request_context
from .stats_engine import HealthReport, check_database_health

log = logging.getLogger(__name__)

MIN_SAMPLES_FOR_VERIFY = 5


@asynccontextmanager
async def lifespan(_server: FastMCP) -> AsyncIterator[None]:
    configure_logging()
    log.info("auto-dba starting up")
    await get_pool()
    await stats_store.bootstrap()
    log.info("auto-dba ready")
    try:
        yield
    finally:
        log.info("auto-dba shutting down")
        await close_pool()


app = FastMCP("auto-dba", lifespan=lifespan)


@app.tool()
async def check_database_health_tool() -> HealthReport:
    """Returns current performance stats and statistical anomalies based on Z-score algorithm."""
    with request_context("check_database_health"):
        return await check_database_health()


@app.tool()
async def get_optimization_plan_tool(
    query_text: str = Field(description="The exact slow query text to analyze with EXPLAIN."),
) -> OptimizationPlan:
    """Run EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) on a slow query and return the raw plan.

    Interpreting the plan is the calling LLM's job. Heuristics that work well:
    1. Look for `Seq Scan` on large relations.
    2. Look for `Nested Loop` joins with high estimated cost.
    3. Form a performance hypothesis grounded in missing indexes.
    4. Propose a `CREATE INDEX CONCURRENTLY` DDL to fix it, then call `simulate_impact_tool`.

    Note: this tool actually executes the query (EXPLAIN ANALYZE). It is gated to SELECT-only,
    but is not a free dry-run.
    """
    with request_context("get_optimization_plan"):
        return await get_optimization_plan(query_text)


@app.tool()
async def simulate_impact_tool(
    ddl_statement: str = Field(
        description="The EXACT DDL statement to simulate (e.g. CREATE INDEX CONCURRENTLY...)."
    ),
) -> SimulationResult:
    """Validates proposed DDL via AST parser and estimates risk + duration.

    The target relation is extracted from the DDL itself; no separate table_name argument is needed.
    """
    with request_context("simulate_impact"):
        return await simulate_impact(ddl_statement)


class ExecutionResult(BaseModel):
    optimization_id: int
    target: str
    execution_time_ms: float
    expected_improvement_pct: float
    baseline_p95_ms: Optional[float]
    message: str


@app.tool()
async def execute_optimization_tool(
    ddl_statement: str = Field(description="The validated DDL statement to execute."),
    queryid: str = Field(
        description="The pg_stat_statements queryid this DDL is intended to optimize. "
        "Get it from check_database_health_tool."
    ),
    expected_improvement_percentage: float = Field(description="The expected Delta P%."),
) -> ExecutionResult:
    """Re-validates the DDL, snapshots a baseline, applies it, and returns an optimization_id
    that should be passed to `verify_optimization_tool` after the workload has run for a while."""
    with request_context("execute_optimization"):
        sim = await simulate_impact(ddl_statement)
        if not sim.is_allowed:
            raise ValueError(f"DDL is blocked by guardrails: {sim.message}")

        history = await stats_store.load_history(queryid)
        baseline_p95: Optional[float] = None
        if history and len(history.latencies) >= MIN_SAMPLES_FOR_VERIFY:
            baseline_p95 = float(np.percentile(np.array(history.latencies), 95))

        opt_id = await stats_store.record_optimization(
            queryid=queryid,
            ddl=ddl_statement,
            expected_improvement_pct=expected_improvement_percentage,
            baseline_p95_ms=baseline_p95,
        )

        log.info("applying DDL", extra={"target": sim.table_name, "optimization_id": opt_id})
        start = time.time()
        await execute(ddl_statement, timeout=TIMEOUT_DDL)
        duration_ms = (time.time() - start) * 1000

        await stats_store.clear_latency_window(queryid)

        return ExecutionResult(
            optimization_id=opt_id,
            target=sim.table_name,
            execution_time_ms=duration_ms,
            expected_improvement_pct=expected_improvement_percentage,
            baseline_p95_ms=baseline_p95,
            message=(
                "DDL applied. Run check_database_health_tool a few times to populate the "
                f"post-DDL window, then call verify_optimization_tool({opt_id})."
            ),
        )


class VerificationResult(BaseModel):
    optimization_id: int
    queryid: str
    status: str  # "VERIFIED" | "STILL_GATHERING" | "NO_BASELINE"
    baseline_p95_ms: Optional[float]
    current_p95_ms: Optional[float]
    samples_collected: int
    expected_improvement_pct: float
    actual_improvement_pct: Optional[float]
    message: str


@app.tool()
async def verify_optimization_tool(
    optimization_id: int = Field(description="ID returned by execute_optimization_tool."),
) -> VerificationResult:
    """Compares post-DDL p95 latency against the baseline snapshot and persists the result."""
    with request_context("verify_optimization"):
        record = await stats_store.load_optimization(optimization_id)
        if record is None:
            raise ValueError(f"No optimization with id {optimization_id}")

        history = await stats_store.load_history(record.queryid)
        samples = len(history.latencies) if history else 0

        if record.baseline_p95_ms is None:
            return VerificationResult(
                optimization_id=optimization_id,
                queryid=record.queryid,
                status="NO_BASELINE",
                baseline_p95_ms=None,
                current_p95_ms=None,
                samples_collected=samples,
                expected_improvement_pct=record.expected_improvement_pct,
                actual_improvement_pct=None,
                message="No baseline was captured at apply time (insufficient pre-DDL samples).",
            )

        if samples < MIN_SAMPLES_FOR_VERIFY:
            return VerificationResult(
                optimization_id=optimization_id,
                queryid=record.queryid,
                status="STILL_GATHERING",
                baseline_p95_ms=record.baseline_p95_ms,
                current_p95_ms=None,
                samples_collected=samples,
                expected_improvement_pct=record.expected_improvement_pct,
                actual_improvement_pct=None,
                message=(
                    f"Only {samples} post-DDL samples collected; "
                    f"need {MIN_SAMPLES_FOR_VERIFY}. Run check_database_health_tool more often."
                ),
            )

        current_p95 = float(np.percentile(np.array(history.latencies), 95))  # type: ignore[union-attr]
        improvement = ((record.baseline_p95_ms - current_p95) / record.baseline_p95_ms) * 100
        await stats_store.record_verification(optimization_id, improvement)

        return VerificationResult(
            optimization_id=optimization_id,
            queryid=record.queryid,
            status="VERIFIED",
            baseline_p95_ms=record.baseline_p95_ms,
            current_p95_ms=round(current_p95, 2),
            samples_collected=samples,
            expected_improvement_pct=record.expected_improvement_pct,
            actual_improvement_pct=round(improvement, 2),
            message=(
                f"p95 went from {record.baseline_p95_ms:.2f}ms to {current_p95:.2f}ms "
                f"({improvement:+.1f}% improvement; expected {record.expected_improvement_pct:+.1f}%)."
            ),
        )


if __name__ == "__main__":
    app.run()
