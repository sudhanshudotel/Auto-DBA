import json

from pydantic import BaseModel

from .db import TIMEOUT_EXPLAIN, fetch


class OptimizationPlan(BaseModel):
    query_text: str
    execution_plan: dict


async def get_optimization_plan(query_text: str) -> OptimizationPlan:
    if not query_text.strip().lower().startswith("select"):
        raise ValueError("Only SELECT queries are supported for EXPLAIN analysis.")

    explain_query = f"EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) {query_text}"
    rows = await fetch(explain_query, timeout=TIMEOUT_EXPLAIN)

    raw_plan: dict = {}
    if rows:
        plan_field = rows[0]["QUERY PLAN"]
        plan_list = json.loads(plan_field) if isinstance(plan_field, str) else plan_field
        if plan_list:
            raw_plan = plan_list[0]

    return OptimizationPlan(query_text=query_text, execution_plan=raw_plan)
