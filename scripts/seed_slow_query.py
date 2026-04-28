"""Seeds a 100k-row table and fires a baseline + anomaly query against it.

Usage:
    uv run scripts/seed_slow_query.py

Mirrors the README "Testing the slow-query loop" workflow. Run after `docker compose up -d`.
The MCP server's check_database_health_tool should subsequently flag the user_id lookup as an anomaly.
"""

import asyncio
import logging
import random
import time

from dotenv import load_dotenv

from auto_dba.db import execute, fetch

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("Setting up test table...")
    await execute(
        """
        CREATE TABLE IF NOT EXISTS test_orders (
            id SERIAL PRIMARY KEY,
            user_id INT NOT NULL,
            amount DECIMAL NOT NULL,
            status VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    count_res = await fetch("SELECT COUNT(*) FROM test_orders")
    if count_res and int(count_res[0]["count"]) == 0:
        logger.info("Inserting mock data (100,000 rows)...")
        await execute(
            """
            INSERT INTO test_orders (user_id, amount, status)
            SELECT
                (random() * 10000)::int,
                (random() * 1000)::decimal,
                CASE WHEN random() > 0.5 THEN 'COMPLETED' ELSE 'PENDING' END
            FROM generate_series(1, 100000)
            """
        )

    logger.info("Running fast queries to build baseline...")
    for _ in range(50):
        await fetch("SELECT * FROM test_orders WHERE id = $1", random.randint(1, 100000))

    logger.info("Running slow query to trigger Z-score anomaly (no index on user_id)...")
    start = time.time()
    await fetch("SELECT * FROM test_orders WHERE user_id = 42")
    logger.warning(f"Slow query took {(time.time() - start) * 1000:.2f}ms")

    logger.info("Done. check_database_health_tool should flag user_id lookup after a few polls.")


if __name__ == "__main__":
    asyncio.run(main())
