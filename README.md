# Auto-DBA ⚡ (Python Architecture)

Auto-DBA is an autonomous, self-healing performance loop for PostgreSQL built as an **Model Context Protocol (MCP) Server**. It dynamically detects performance anomalies using a rolling $Z$-score logic (via `numpy`) and proposes optimization schemas via an LLM agent.

## 🛡️ Zero-Trust Safety Model

Auto-DBA enforces strict Guardrails logic out-of-the-box (`src/auto_dba/guardrail.py`):
- **Simulation**: Generates dry run estimates for table size and index build time.
- **SQL Sanitizer**: Blocks ALL actions by default except `CREATE INDEX CONCURRENTLY`, `ANALYZE`, and `REINDEX`. Destructive operations (`DROP`, `TRUNCATE`, `DELETE`) are hard-blocked.
- **Risk Scoring**: Estimates DDL risk from 1 to 10 based on table sizes. Anything $> 5$ returns an `ACTION_REQUIRED` status, preventing fully autonomous execution without a Human-in-the-loop review.

## 🚀 One-Click Setup (Zero-Install Vibe-Coding)

Auto-DBA relies on [uv](https://github.com/astral-sh/uv) and the FastMCP integration for a lightning-fast, zero-friction setup.

### Prerequisites
- Python 3.12+ 
- `uv` installed (`pip install uv` or natively via curl/brew)
- Docker (for local Postgres instances)

### Installation
1. Start the PostgreSQL metrics container:
   \`\`\`bash
   docker compose up -d
   \`\`\`

2. Run the MCP Server directly via `uvx`:
   \`\`\`bash
   # Cursor and Claude will automatically invoke this via their config bindings!
   uvx --directory . auto-dba
   \`\`\`

### Testing the "Slow Query" Loop
1. Install dependencies into the local venv (for testing scripts):
   \`\`\`bash
   uv sync
   \`\`\`
2. Run the test script to populate 100k rows and fire a slow query.
   \`\`\`bash
   uv run test_slow_query.py
   \`\`\`

## Tools Exposed
- `check_database_health_tool`: Polls `pg_stat_statements`, evaluates recent Δt against μ ± 3σ using `numpy`, returning metrics.
- `get_optimization_plan_tool`: Analyzes query EXPLAIN JSON against an LLM context system for fast Seq-Scan detection.
- `simulate_impact_tool`: Dry runs the proposed index layout over `pg_total_relation_size`.
- `execute_optimization_tool`: Executes the validated query safely using `CONCURRENTLY`.
