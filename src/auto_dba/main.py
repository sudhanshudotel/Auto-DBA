import time
import re
from mcp.server.fastmcp import FastMCP
from pydantic import Field
from .stats_engine import check_database_health, HealthReport
from .analyst import get_optimization_plan, OptimizationPlan
from .guardrail import simulate_impact, SimulationResult
from .db import execute

# Create FastMCP server
app = FastMCP("auto-dba")

@app.tool()
async def check_database_health_tool() -> HealthReport:
    """Returns current performance stats and statistical anomalies based on Z-score algorithm."""
    return await check_database_health()

@app.tool()
async def get_optimization_plan_tool(
    query_text: str = Field(description="The exact slow query text to analyze with EXPLAIN.")
) -> OptimizationPlan:
    """Analyzes a specific slow query using EXPLAIN and returns JSON optimization plan hints."""
    return await get_optimization_plan(query_text)

@app.tool()
async def simulate_impact_tool(
    ddl_statement: str = Field(description="The EXACT DDL statement to simulate (e.g. CREATE INDEX CONCURRENTLY...)."),
    table_name: str = Field(description="The name of the table being modified.")
) -> SimulationResult:
    """Validates proposed DDL (simulate a dry run), estimating risk and time."""
    return await simulate_impact(ddl_statement, table_name)

@app.tool()
async def execute_optimization_tool(
    ddl_statement: str = Field(description="The validated DDL statement to execute."),
    expected_improvement_percentage: float = Field(description="The expected Delta P%.")
) -> str:
    """Applies the DDL and provides a placeholder to verify Performance Improvement percentage."""
    # 1. Re-validate
    match = re.search(r'ON\s+([A-Za-z0-9_]+)', ddl_statement, re.IGNORECASE)
    table_name = match.group(1) if match else 'unknown'

    sim = await simulate_impact(ddl_statement, table_name)
    if not sim.is_allowed:
        raise ValueError("DDL is blocked by Guardrails.")

    # 2. Execute
    start_time = time.time()
    try:
        await execute(ddl_statement)
    except Exception as e:
        raise RuntimeError(f"Execution failed: {e}")
    duration_ms = (time.time() - start_time) * 1000

    return (
        f"Optimization applied successfully.\n"
        f"Execution Time: {duration_ms:.2f}ms\n"
        f"Expected Improvement: {expected_improvement_percentage}%\n"
        f"Actual Verification: Pending next telemetry cycle."
    )

if __name__ == "__main__":
    app.run()
