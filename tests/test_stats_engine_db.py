"""DB-backed test for the stats engine.

Verifies the documented invariants of the Z-score loop end-to-end:

1. First poll seeds the baseline; no anomaly is possible.
2. With ≥ MIN_SAMPLES_FOR_Z latency samples, a sufficiently large spike trips the threshold.
3. Persistence: dropping the in-process state and re-running picks up the existing row
   (in v0.1 this required 5 fresh polls every restart; v1 must not regress).
"""

from __future__ import annotations

import pytest

from auto_dba import stats_engine, stats_store
from tests.conftest import requires_db

pytestmark = requires_db


async def test_seed_then_anomaly(clean_meta_schema, monkeypatch: pytest.MonkeyPatch):
    qid = "qid-test-1"

    # Stub pg_stat_statements polling. Each call returns one row with monotonically
    # increasing (calls, total_exec_time) so the engine sees deltas.
    polls: list[tuple[int, float]] = []
    captured: list = []

    async def fake_fetch(query: str, *args, **kwargs):
        if "pg_stat_statements" in query:
            if not polls:
                return []
            calls, total = polls.pop(0)
            return [
                {
                    "queryid": qid,
                    "query": "SELECT * FROM t WHERE x = $1",
                    "calls": calls,
                    "total_exec_time": total,
                }
            ]
        # All other fetches go to the real DB (e.g. stats_store reads).
        from auto_dba.db import fetch as real_fetch

        result = await real_fetch(query, *args, **kwargs)
        captured.append((query, result))
        return result

    monkeypatch.setattr(stats_engine, "fetch", fake_fetch)

    # Poll 1: seed only.
    polls.append((10, 100.0))  # avg latency: irrelevant on seed
    report = await stats_engine.check_database_health()
    assert report.anomalies_found == 0
    history = await stats_store.load_history(qid)
    assert history is not None
    assert history.last_calls == 10

    # Polls 2–5: stable ~10 ms/call (each delta = 10 calls × 100 ms = 1000 ms).
    for i in range(2, 6):
        polls.append((10 * i, 100.0 * i))
        await stats_engine.check_database_health()

    history = await stats_store.load_history(qid)
    assert history is not None
    assert len(history.latencies) == 4
    # All latencies should be ~10 ms (1000 ms / 10 calls).
    assert all(9.99 < lat < 10.01 for lat in history.latencies)

    # Poll 6: a spike — +10 calls but +5000 ms (500 ms/call).
    polls.append((60, 5500.0))
    spike_report = await stats_engine.check_database_health()
    assert spike_report.anomalies_found == 1
    assert spike_report.reports[0]["queryid"] == qid
    assert spike_report.reports[0]["z_score"] > 3.0


async def test_persistence_across_engine_restart(clean_meta_schema, monkeypatch: pytest.MonkeyPatch):
    """In v0.1 history_store was a module-level dict — restarts wiped it. v1 persists."""
    qid = "qid-test-persist"

    polls: list[tuple[int, float]] = [(5, 50.0)]

    async def fake_fetch(query: str, *args, **kwargs):
        if "pg_stat_statements" in query:
            if not polls:
                return []
            calls, total = polls.pop(0)
            return [
                {"queryid": qid, "query": "SELECT 1", "calls": calls, "total_exec_time": total}
            ]
        from auto_dba.db import fetch as real_fetch
        return await real_fetch(query, *args, **kwargs)

    monkeypatch.setattr(stats_engine, "fetch", fake_fetch)

    await stats_engine.check_database_health()
    before = await stats_store.load_history(qid)
    assert before is not None

    # Simulate a restart by closing the pool — state should still be on disk.
    from auto_dba.db import close_pool
    await close_pool()

    after = await stats_store.load_history(qid)
    assert after is not None
    assert after.last_calls == before.last_calls
    assert after.last_total_time == before.last_total_time
