# Auto-DBA

Auto-DBA is a self-healing performance loop for PostgreSQL, packaged as a **Model Context Protocol (MCP) server**. The LLM client (Cursor / Claude Code) drives a 5-tool loop: detect anomalies → propose an index → simulate impact → apply DDL → verify the win.

## Zero-Trust Safety Model

Out-of-the-box guardrails ([src/auto_dba/guardrail.py](src/auto_dba/guardrail.py)):

- **AST-based allowlist.** DDL is parsed with `pglast` (libpg_query). Only top-level `CREATE INDEX CONCURRENTLY`, `ANALYZE`, and `REINDEX INDEX/TABLE` are accepted. `;`-injection is rejected at parse time; substring tricks (a table named `drop_log`, comments containing `DROP`) cannot fool the parser.
- **Pluggable risk model** ([src/auto_dba/risk.py](src/auto_dba/risk.py)). Composable bands for table size, write rate, and index count. Default policy reproduces the original size-only thresholds (`> 1 GB → 6`, `> 10 GB → 8`); anything `> 5` returns `ACTION_REQUIRED`.
- **Re-validation on execute.** `execute_optimization_tool` re-parses the DDL through the same guardrail before applying — never trusts that the client previously validated.

## Setup

Prereqs: Python 3.12+, [uv](https://github.com/astral-sh/uv), Docker (for local Postgres).

```bash
# 1. Local Postgres with pg_stat_statements preloaded.
docker compose up -d

# 2. Configure the DSN.
cp .env.example .env

# 3. Run the MCP server (Cursor/Claude invoke this via .cursor/mcp.json / .claudecode/config.json).
uvx --directory . auto-dba
```

Auto-DBA creates an `auto_dba` meta-schema on first run with two tables (`query_history`, `optimizations`) for persistent baselines and verification results. **Restarting the server no longer wipes Z-score baselines**.

### Exercising the loop

```bash
uv sync
uv run scripts/seed_slow_query.py   # populates 100k rows + a slow user_id query
```

Then drive the tools through your MCP client:

1. `check_database_health_tool` — polls `pg_stat_statements`, flags Z-score anomalies (`z > 3` over a rolling 100-sample window).
2. `get_optimization_plan_tool(query_text)` — runs `EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON)`. Returns the raw plan; the LLM proposes a `CREATE INDEX CONCURRENTLY` based on the docstring guidance.
3. `simulate_impact_tool(ddl_statement)` — parses, allowlists, and risk-scores the DDL.
4. `execute_optimization_tool(ddl_statement, queryid, expected_improvement_percentage)` — snapshots `baseline_p95_ms`, applies the DDL, returns an `optimization_id`.
5. `verify_optimization_tool(optimization_id)` — after the workload has driven new samples through, computes actual p95 improvement and persists it.

### Tests

```bash
uv run pytest                        # full suite (DB tests skip without DATABASE_URL)
uv run pytest tests/test_guardrail.py tests/test_risk.py   # pure unit tests, no DB
```
