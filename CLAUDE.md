# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

Local Postgres (with `pg_stat_statements` preloaded via [postgresql.conf](postgresql.conf) and [init.sql](init.sql)):
```bash
docker compose up -d
```

Run the MCP server (this is what Cursor/Claude invoke via [.cursor/mcp.json](.cursor/mcp.json) and [.claudecode/config.json](.claudecode/config.json)):
```bash
uvx --directory . auto-dba
```

Dev workflow:
```bash
uv sync
uv run scripts/seed_slow_query.py     # populates 100k rows + fires baseline + anomaly query
uv run pytest                          # all tests (DB tests skip if DATABASE_URL is unset)
uv run pytest tests/test_guardrail.py  # pure unit tests, no DB needed
uv run pytest tests/test_e2e.py        # full loop, requires docker compose up
uv run ruff check .                    # lint
```

`DATABASE_URL` is read by [src/auto_dba/db.py](src/auto_dba/db.py) via `python-dotenv` and is **required** — there is no localhost fallback. Copy [.env.example](.env.example) to `.env` for local runs. The MCP launcher configs inject it directly via the `env` block.

## Architecture

Auto-DBA is a **FastMCP server** that exposes a **5-tool optimization loop** for PostgreSQL. The pipeline is intentionally staged so the LLM client (Cursor/Claude) drives each step explicitly rather than chaining server-side:

1. `check_database_health_tool` → [stats_engine.py](src/auto_dba/stats_engine.py) polls `pg_stat_statements`, persists rolling latency baselines into `auto_dba.query_history`, and flags Z-score anomalies.
2. `get_optimization_plan_tool` → [analyst.py](src/auto_dba/analyst.py) runs `EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON)` and returns the raw plan. Plan-interpretation guidance lives in the tool's docstring (which FastMCP surfaces to the LLM), not in the response body.
3. `simulate_impact_tool` → [guardrail.py](src/auto_dba/guardrail.py) parses DDL with `pglast` (a libpg_query binding), validates against an AST allowlist, and scores risk via [risk.py](src/auto_dba/risk.py).
4. `execute_optimization_tool` → [main.py](src/auto_dba/main.py) re-runs `simulate_impact`, snapshots `baseline_p95_ms` into `auto_dba.optimizations`, applies the DDL, **clears the rolling window** so post-DDL samples are isolated, and returns an `optimization_id`.
5. `verify_optimization_tool` → compares the post-DDL p95 (from the cleared-then-refilled rolling window) against the baseline snapshot and persists `actual_improvement_pct` back to `auto_dba.optimizations`.

Tool registration + lifespan live in [main.py](src/auto_dba/main.py); the entry point is `auto_dba.main:app.run` (see [pyproject.toml](pyproject.toml)).

State lives in two tables, created idempotently by [stats_store.py](src/auto_dba/stats_store.py) on startup:
- `auto_dba.query_history` — rolling latency window per `queryid` (capped at 100 samples; trimmed in SQL).
- `auto_dba.optimizations` — applied DDL + baseline_p95_ms + actual_improvement_pct after verification.

### Non-obvious invariants

- **Guardrail is AST-based, not regex.** [guardrail.py](src/auto_dba/guardrail.py) uses `pglast.parse_sql` to enforce: exactly one statement, top-level type ∈ {`IndexStmt(concurrent=True)`, `VacuumStmt(is_vacuumcmd=False)`, `ReindexStmt(kind ∈ {INDEX, TABLE})`}. **When adding new DDL types**, extend the type/attribute checks in `_classify` and add a parser test in [tests/test_guardrail.py](tests/test_guardrail.py); do not introduce string-matching paths.
- **Risk policy is pluggable.** [risk.py](src/auto_dba/risk.py) is pure functions; [guardrail.py](src/auto_dba/guardrail.py) holds a single `RISK_POLICY = RiskConfig()`. Default config reproduces the original 2/6/8 size-only behavior. To enable write-rate or index-count bands, populate the corresponding `_Band` and add the matching telemetry query in `_gather_factors`.
- **Verification depends on clearing the rolling window at apply time.** `execute_optimization_tool` calls `stats_store.clear_latency_window(queryid)` *after* the DDL is applied so subsequent `check_database_health_tool` polls populate a clean post-DDL distribution. The pre-DDL baseline is preserved separately in `auto_dba.optimizations.baseline_p95_ms`.
- **`simulate_impact` returns `is_allowed=False` instead of raising.** Callers must inspect `result.is_allowed` and `result.status`; only true server errors (DB unreachable, etc.) propagate as exceptions.
- **DDL execution has no command timeout.** [db.py](src/auto_dba/db.py) sets `TIMEOUT_DDL = None` because `CREATE INDEX CONCURRENTLY` legitimately runs for hours on large tables. Stats polling and EXPLAIN have shorter, configurable timeouts.
- **`statement_cache_size=0` is required.** [db.py](src/auto_dba/db.py) sets this so the same code works against a Supabase transaction pooler (port 6543) and PgBouncer in transaction mode. Do not remove it.
- **`get_optimization_plan` runs `EXPLAIN ANALYZE`**, which actually executes the query. It's gated to `SELECT` only, but be aware this is not a free dry-run.
- **`DATABASE_URL` is required** — there is no localhost fallback; missing DSN raises at startup so misconfigurations surface fast.

### Postgres requirements

`pg_stat_statements` must be loaded via `shared_preload_libraries` and the extension created in the target DB. The Docker setup handles both via [postgresql.conf](postgresql.conf) and [init.sql](init.sql); for managed Postgres (Supabase et al.) ensure both are enabled or `check_database_health_tool` returns the "not installed" error path. The `auto_dba` meta-schema and its tables are created automatically by the lifespan handler — no manual migration needed.
