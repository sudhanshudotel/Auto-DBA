import logging
import math
from dataclasses import dataclass
from typing import Optional

from pglast import parse_sql
from pglast.ast import IndexStmt, RangeVar, ReindexStmt, VacuumStmt
from pglast.enums.parsenodes import ReindexObjectType
from pydantic import BaseModel

from .db import fetch
from .risk import RiskConfig, RiskFactors, score as score_risk

logger = logging.getLogger(__name__)

# A single, process-wide risk policy. Swap in a different RiskConfig (e.g. with
# populated write_rate_band / index_count_band) once the corresponding telemetry
# queries are wired into _gather_factors below.
RISK_POLICY = RiskConfig()


class SimulationResult(BaseModel):
    ddl_statement: str
    table_name: str
    is_allowed: bool
    table_size_gb: float
    estimated_time_mins: int
    risk_score: int
    status: str
    message: str


@dataclass(frozen=True)
class _ParsedDDL:
    stmt_kind: str  # "INDEX" | "ANALYZE" | "REINDEX"
    schema: Optional[str]
    relation: Optional[str]


def _qualified(rel: Optional[RangeVar]) -> tuple[Optional[str], Optional[str]]:
    if rel is None:
        return None, None
    return rel.schemaname, rel.relname


def _classify(ddl: str) -> _ParsedDDL:
    """Parse DDL and return the allow-listed kind + target relation, or raise ValueError."""
    parsed = parse_sql(ddl)

    if len(parsed) != 1:
        raise ValueError(
            f"Multi-statement input is not allowed ({len(parsed)} statements found)."
        )

    stmt = parsed[0].stmt

    if isinstance(stmt, IndexStmt):
        if not stmt.concurrent:
            raise ValueError("Only CREATE INDEX CONCURRENTLY is allowed.")
        schema, relation = _qualified(stmt.relation)
        return _ParsedDDL("INDEX", schema, relation)

    if isinstance(stmt, VacuumStmt):
        if stmt.is_vacuumcmd:
            raise ValueError("VACUUM is not allowed; only ANALYZE.")
        # ANALYZE may target one or more relations, or none (whole-DB analyze).
        schema, relation = (None, None)
        if stmt.rels:
            first = stmt.rels[0]
            schema, relation = _qualified(getattr(first, "relation", None))
        return _ParsedDDL("ANALYZE", schema, relation)

    if isinstance(stmt, ReindexStmt):
        allowed_kinds = (
            ReindexObjectType.REINDEX_OBJECT_INDEX,
            ReindexObjectType.REINDEX_OBJECT_TABLE,
        )
        if stmt.kind not in allowed_kinds:
            raise ValueError(
                "Only REINDEX INDEX or REINDEX TABLE is allowed (DATABASE/SCHEMA/SYSTEM rejected)."
            )
        schema, relation = _qualified(stmt.relation)
        return _ParsedDDL("REINDEX", schema, relation)

    raise ValueError(
        f"Statement type {type(stmt).__name__} is not in the allowlist "
        "(allowed: CREATE INDEX CONCURRENTLY, ANALYZE, REINDEX INDEX/TABLE)."
    )


# Heuristic minutes-per-GB by statement kind. These are rough rules-of-thumb,
# not benchmarks — real time depends on hardware, fillfactor, and bloat.
_TIME_PER_GB_MINS = {"INDEX": 5, "REINDEX": 3, "ANALYZE": 1}


async def _table_size_gb(qualified: str) -> tuple[float, Optional[str]]:
    """Look up pg_total_relation_size. Returns (size_gb, soft_error_message)."""
    try:
        rows = await fetch("SELECT pg_total_relation_size($1::regclass) AS size", qualified)
    except Exception as exc:
        msg = str(exc)
        # asyncpg surfaces "relation \"X\" does not exist" via UndefinedTableError;
        # match by the canonical fragment so we don't depend on the exception class import.
        if "does not exist" in msg:
            return 0.0, f"relation {qualified!r} does not exist yet"
        logger.exception("Failed to look up size for %s", qualified)
        raise
    size_bytes = (rows[0]["size"] if rows else 0) or 0
    return size_bytes / (1024**3), None


async def _gather_factors(qualified: Optional[str], is_concurrent: bool) -> tuple[RiskFactors, Optional[str]]:
    """Collect inputs needed by the active RiskConfig.

    Today the default policy uses size only. To enable write-rate or index-count
    bands, populate the corresponding _Band on RISK_POLICY and add the matching
    query here (pg_stat_user_tables for write rate, pg_indexes for index count).
    """
    if not qualified:
        return RiskFactors(table_size_gb=0.0, is_concurrent=is_concurrent), None
    size_gb, soft = await _table_size_gb(qualified)
    return RiskFactors(table_size_gb=size_gb, is_concurrent=is_concurrent), soft


async def simulate_impact(ddl_statement: str) -> SimulationResult:
    try:
        parsed = _classify(ddl_statement)
    except ValueError as exc:
        return SimulationResult(
            ddl_statement=ddl_statement,
            table_name="unknown",
            is_allowed=False,
            table_size_gb=0.0,
            estimated_time_mins=0,
            risk_score=10,
            status="REJECTED",
            message=str(exc),
        )
    except Exception as exc:
        # Parse errors from pglast surface as pglast.parser.ParseError; flatten to rejection.
        return SimulationResult(
            ddl_statement=ddl_statement,
            table_name="unknown",
            is_allowed=False,
            table_size_gb=0.0,
            estimated_time_mins=0,
            risk_score=10,
            status="REJECTED",
            message=f"parse error: {exc}",
        )

    qualified = (
        f"{parsed.schema}.{parsed.relation}" if parsed.schema else (parsed.relation or "")
    )
    is_concurrent = parsed.stmt_kind == "INDEX"  # Only IndexStmt has the CONCURRENTLY flavor.

    if qualified:
        factors, soft_error = await _gather_factors(qualified, is_concurrent)
    else:
        factors = RiskFactors(table_size_gb=0.0, is_concurrent=is_concurrent)
        soft_error = "no specific relation (whole-DB ANALYZE)"

    risk = score_risk(factors, RISK_POLICY)
    estimated = math.ceil(factors.table_size_gb * _TIME_PER_GB_MINS[parsed.stmt_kind])
    if soft_error:
        message = soft_error
    elif risk.total > 5:
        message = f"Risk Score {risk.total}: human-in-the-loop approval required due to table size."
    else:
        message = "Low risk. Safe to auto-run."

    return SimulationResult(
        ddl_statement=ddl_statement,
        table_name=qualified or "unknown",
        is_allowed=True,
        table_size_gb=round(factors.table_size_gb, 2),
        estimated_time_mins=estimated,
        risk_score=risk.total,
        status=risk.status,
        message=message,
    )
