import asyncio
import time
import random
import logging
from src.auto_dba.db import execute, fetch
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    logger.info("Setting up test table...")
    
    await execute("""
        CREATE TABLE IF NOT EXISTS test_orders (
            id SERIAL PRIMARY KEY,
            user_id INT NOT NULL,
            amount DECIMAL NOT NULL,
            status VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    logger.info("Inserting mock data (100,000 rows)...")
    count_res = await fetch("SELECT COUNT(*) FROM test_orders;")
    if count_res and int(count_res[0]['count']) == 0:
        await execute("""
            INSERT INTO test_orders (user_id, amount, status)
            SELECT 
                (random() * 10000)::int, 
                (random() * 1000)::decimal, 
                CASE WHEN random() > 0.5 THEN 'COMPLETED' ELSE 'PENDING' END
            FROM generate_series(1, 100000);
        """)

    logger.info("Running Fast queries to build baseline (Z-Score Baseline)...")
    for _ in range(50):
        await fetch("SELECT * FROM test_orders WHERE id = $1", random.randint(1, 100000))

    logger.info("Running Slow query to trigger Z-Score anomaly (No Index on user_id)...")
    start = time.time()
    await fetch("SELECT * FROM test_orders WHERE user_id = 42;")
    logger.warning(f"Slow query took {(time.time() - start) * 1000:.2f}ms")

    logger.info("Test setup complete. The Stats Engine should now detect the anomaly for user_id lookup.")

if __name__ == "__main__":
    asyncio.run(main())
