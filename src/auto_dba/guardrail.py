import math
from pydantic import BaseModel, Field
from .db import fetch

class SimulationResult(BaseModel):
    ddl_statement: str
    table_name: str
    is_allowed: bool
    table_size_gb: float
    estimated_time_mins: int
    risk_score: int
    status: str
    message: str

async def simulate_impact(ddl_statement: str, table_name: str) -> SimulationResult:
    upper_ddl = ddl_statement.strip().upper()
    
    is_allowed = (
        upper_ddl.startswith('CREATE INDEX CONCURRENTLY') or
        upper_ddl.startswith('ANALYZE') or
        upper_ddl.startswith('REINDEX')
    )
    
    is_blocked = any(kw in upper_ddl for kw in ["DROP", "TRUNCATE", "DELETE", "UPDATE"])
    
    if not is_allowed or is_blocked:
        raise ValueError("DDL statement violates zero-trust safety model. Only CREATE INDEX CONCURRENTLY, ANALYZE, or REINDEX are allowed.")

    size_bytes = 0
    try:
        size_res = await fetch("SELECT pg_total_relation_size($1) as size", table_name)
        if size_res:
            size_bytes = size_res[0]['size'] or 0
    except Exception:
        pass  # Table might not exist yet

    size_gb = size_bytes / (1024 * 1024 * 1024)
    
    risk_score = 2
    if size_gb > 10:
        risk_score = 8
    elif size_gb > 1:
        risk_score = 6

    estimated_time_mins = math.ceil(size_gb * 1)
    
    status = "ACTION_REQUIRED" if risk_score > 5 else "AUTO_APPROVED"
    message = (
        f"Risk Score {risk_score}: Human-in-the-loop approval required due to table size." 
        if risk_score > 5 else "Low risk. Safe to auto-run."
    )

    return SimulationResult(
        ddl_statement=ddl_statement,
        table_name=table_name,
        is_allowed=is_allowed,
        table_size_gb=round(size_gb, 2),
        estimated_time_mins=estimated_time_mins,
        risk_score=risk_score,
        status=status,
        message=message
    )
