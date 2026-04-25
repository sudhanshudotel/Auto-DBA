import os
import logging
import asyncpg
from typing import Optional
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            logger.warning("DATABASE_URL not found in environment. Using default local instance.")
            dsn = "postgresql://postgres:postgres@localhost:5432/postgres"
        
        # Determine if we're connecting to Supabase or a cloud provider
        # Supabase transaction poolers (port 6543) do not support asyncpg prepared statements.
        # Setting statement_cache_size=0 ensures compatibility with PgBouncer.
        _pool = await asyncpg.create_pool(
            dsn=dsn,
            statement_cache_size=0  # Supabase pooler compatibility
        )
        if _pool is None:
            raise RuntimeError("Failed to create connection pool.")
    return _pool

async def fetch(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)

async def execute(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.execute(query, *args)
