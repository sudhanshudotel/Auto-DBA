"""Shared fixtures.

DB-backed tests (test_e2e, test_stats_engine_db) require a running Postgres
with `pg_stat_statements` preloaded — see [docker-compose.yml](docker-compose.yml).
They are skipped automatically when DATABASE_URL is not set.

Pure unit tests (test_guardrail, test_risk) do not require Postgres.
"""

from __future__ import annotations

import os

import pytest


def _have_db() -> bool:
    return bool(os.getenv("DATABASE_URL"))


requires_db = pytest.mark.skipif(
    not _have_db(),
    reason="DATABASE_URL not set; skipping DB-backed tests (run `docker compose up -d`)",
)


@pytest.fixture
async def clean_meta_schema():
    """Bootstrap auto_dba.* and wipe rows so each test starts clean.

    Tables are not dropped — that would interfere with parallel sessions; we just
    truncate.
    """
    if not _have_db():
        pytest.skip("DATABASE_URL not set")

    from auto_dba import stats_store
    from auto_dba.db import close_pool, execute

    await stats_store.bootstrap()
    await execute("TRUNCATE auto_dba.query_history, auto_dba.optimizations RESTART IDENTITY")
    yield
    await close_pool()
