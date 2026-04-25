import json
from pydantic import BaseModel
from .db import fetch

class OptimizationPlan(BaseModel):
    system_instructions: str
    query_text: str
    execution_plan: dict

async def get_optimization_plan(query_text: str) -> OptimizationPlan:
    if not query_text.strip().lower().startswith("select"):
        raise ValueError("Only SELECT queries are supported for EXPLAIN analysis.")

    explain_query = f"EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) {query_text}"
    
    try:
        res = await fetch(explain_query)
        if res:
            raw_plan_json_str = res[0]['QUERY PLAN']
            raw_plan = json.loads(raw_plan_json_str)[0] if isinstance(raw_plan_json_str, str) else raw_plan_json_str[0]
        else:
            raw_plan = {}
    except Exception as e:
        raise RuntimeError(f"Failed to explain query: {str(e)}")

    context_prompt = """
System Prompt instructions for parsing this plan:
1. Look for Node Types "Seq Scan" on large tables.
2. Look for "Nested Loop" joins with high cost.
3. Formulate a "Performance Hypothesis" based on missing indexes.
4. Propose a specific "CREATE INDEX CONCURRENTLY" DDL statement to fix.
    """

    return OptimizationPlan(
        system_instructions=context_prompt.strip(),
        query_text=query_text,
        execution_plan=raw_plan
    )
