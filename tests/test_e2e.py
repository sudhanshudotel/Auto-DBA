"""End-to-end test of the full optimization loop against docker-compose Postgres."""

from __future__ import annotations

import pytest

from auto_dba import stats_store
from auto_dba.guardrail import simulate_impact
from auto_dba.main import (
    execute_optimization_tool,
    simulate_impact_tool,
    verify_optimization_tool,
)
from tests.conftest import requires_db

pytestmark = requires_db

TEST_TABLE = "auto_dba_e2e_orders"


@pytest.fixture
async def seed_table(clean_meta_schema):
    from auto_dba.db import execute, fetch

    await execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")
    await execute(
        f"""
        CREATE TABLE {TEST_TABLE} (
            id SERIAL PRIMARY KEY,
            user_id INT NOT NULL,
            amount NUMERIC NOT NULL
        )
        """
    )
    # Tiny dataset is enough — we're testing the loop, not perf.
    await execute(
        f"""
        INSERT INTO {TEST_TABLE} (user_id, amount)
        SELECT (random() * 100)::int, (random() * 100)::numeric
        FROM generate_series(1, 1000)
        """
    )
    await fetch(f"SELECT * FROM {TEST_TABLE} WHERE user_id = 1")  # warm up
    yield
    await execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")


async def test_simulate_then_execute_then_verify(seed_table):
    ddl = f"CREATE INDEX CONCURRENTLY idx_e2e_user_id ON {TEST_TABLE}(user_id)"

    # Drop a stale index from a previous failed run, if any.
    from auto_dba.db import execute as raw_execute
    await raw_execute("DROP INDEX IF EXISTS idx_e2e_user_id")

    sim = await simulate_impact(ddl)
    assert sim.is_allowed is True
    assert sim.table_name == TEST_TABLE

    # Seed a fake baseline so execute can snapshot a baseline_p95_ms.
    qid = "fake-qid-e2e"
    await stats_store.seed_history(qid, "SELECT * FROM t", calls=0, total_time=0.0)
    for latency in [50.0, 55.0, 48.0, 52.0, 60.0, 53.0]:
        await stats_store.append_latency(qid, calls=0, total_time=0.0, latency_ms=latency)

    exec_result = await execute_optimization_tool(
        ddl_statement=ddl,
        queryid=qid,
        expected_improvement_percentage=50.0,
    )
    opt_id = exec_result.optimization_id
    assert exec_result.baseline_p95_ms is not None
    assert exec_result.target == TEST_TABLE

    # Right after apply: latency window is cleared, so verification must report STILL_GATHERING.
    pre = await verify_optimization_tool(optimization_id=opt_id)
    assert pre.status == "STILL_GATHERING"

    # Simulate post-DDL samples coming in (lower than the baseline → improvement).
    for latency in [10.0, 12.0, 11.0, 9.5, 10.5, 11.5]:
        await stats_store.append_latency(qid, calls=0, total_time=0.0, latency_ms=latency)

    post = await verify_optimization_tool(optimization_id=opt_id)
    assert post.status == "VERIFIED"
    assert post.actual_improvement_pct is not None
    assert post.actual_improvement_pct > 50  # baseline ~55, current ~11 → ~80% improvement
    assert post.baseline_p95_ms == pre.baseline_p95_ms

    # Persisted to auto_dba.optimizations.
    record = await stats_store.load_optimization(opt_id)
    assert record is not None
    assert record.actual_improvement_pct is not None


async def test_simulate_impact_tool_rejects_unsafe_ddl(seed_table):
    """simulate_impact_tool is the path the LLM hits — verify it doesn't raise on rejection."""
    sim = await simulate_impact_tool(
        ddl_statement=f"CREATE INDEX CONCURRENTLY idx_x ON {TEST_TABLE}(id); DROP TABLE {TEST_TABLE}"
    )
    assert sim.is_allowed is False
    assert sim.status == "REJECTED"


async def test_execute_optimization_blocked_for_unsafe_ddl(seed_table):
    with pytest.raises(ValueError, match="blocked by guardrails"):
        await execute_optimization_tool(
            ddl_statement="UPDATE users SET x = 1",
            queryid="any",
            expected_improvement_percentage=10.0,
        )
