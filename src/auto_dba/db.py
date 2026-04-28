"""asyncpg pool wrapper.

`statement_cache_size=0` is required: it lets the same code work against the
Supabase transaction pooler (port 6543) and PgBouncer in transaction mode,
which do not support asyncpg's prepared-statement cache.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import asyncpg
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_pool: Optional[asyncpg.Pool] = None

# Default per-call timeouts (seconds). DDL execution is intentionally unbounded
# because CREATE INDEX CONCURRENTLY can legitimately run for hours.
TIMEOUT_DEFAULT = 30.0
TIMEOUT_STATS_POLL = 10.0
TIMEOUT_EXPLAIN = 60.0
TIMEOUT_DDL: Optional[float] = None

# Pool sizing — small by default since this server typically has one client (the
# LLM driving the loop). AUTO_DBA_POOL_MAX overrides for deployments that fan out.
POOL_MIN_SIZE = 1
POOL_MAX_SIZE = int(os.getenv("AUTO_DBA_POOL_MAX", "5"))


def _dsn() -> str:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL is not set. Copy .env.example to .env or set it in the environment."
        )
    return dsn


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=_dsn(),
            statement_cache_size=0,
            command_timeout=TIMEOUT_DEFAULT,
            min_size=POOL_MIN_SIZE,
            max_size=POOL_MAX_SIZE,
        )
        if _pool is None:
            raise RuntimeError("Failed to create connection pool.")
        # Validate connectivity early so misconfigured DSNs surface at startup,
        # not at the first tool call.
        async with _pool.acquire() as conn:
            await conn.execute("SELECT 1")
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def fetch(query: str, *args: Any, timeout: Optional[float] = TIMEOUT_DEFAULT):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args, timeout=timeout)


async def execute(query: str, *args: Any, timeout: Optional[float] = TIMEOUT_DEFAULT):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.execute(query, *args, timeout=timeout)
