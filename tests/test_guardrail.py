"""Unit tests for the AST-based guardrail. No live database required.

The guardrail only touches Postgres in the size-lookup branch (`_table_size_gb`),
so we monkeypatch that helper for tests that exercise allowed DDL. Rejection
paths short-circuit before any DB call.
"""

import pytest

from auto_dba import guardrail


@pytest.fixture(autouse=True)
def _stub_table_size(monkeypatch: pytest.MonkeyPatch):
    """Force a deterministic table size so risk math is testable without Postgres."""

    async def fake_size(qualified: str):
        return 0.5, None  # below the 1 GB threshold → low risk

    monkeypatch.setattr(guardrail, "_table_size_gb", fake_size)


# ---------- Allowed DDL ----------

async def test_create_index_concurrently_unqualified():
    sim = await guardrail.simulate_impact(
        "CREATE INDEX CONCURRENTLY idx_users_id ON users(id)"
    )
    assert sim.is_allowed is True
    assert sim.table_name == "users"
    assert sim.status == "AUTO_APPROVED"


async def test_create_index_concurrently_schema_qualified():
    sim = await guardrail.simulate_impact(
        "CREATE INDEX CONCURRENTLY idx_users_id ON public.users(id)"
    )
    assert sim.is_allowed is True
    assert sim.table_name == "public.users"


async def test_create_index_concurrently_quoted_identifier():
    sim = await guardrail.simulate_impact(
        'CREATE INDEX CONCURRENTLY idx_u_id ON "User"(id)'
    )
    assert sim.is_allowed is True
    assert sim.table_name == "User"


async def test_substring_table_name_no_longer_false_positive():
    """A table named `drop_log` would fail the v0.1 substring blocklist."""
    sim = await guardrail.simulate_impact(
        "CREATE INDEX CONCURRENTLY idx_dl ON drop_log(id)"
    )
    assert sim.is_allowed is True
    assert sim.table_name == "drop_log"


async def test_analyze_specific_table():
    sim = await guardrail.simulate_impact("ANALYZE public.users")
    assert sim.is_allowed is True
    assert sim.table_name == "public.users"


async def test_analyze_whole_database():
    sim = await guardrail.simulate_impact("ANALYZE")
    assert sim.is_allowed is True
    # No specific relation; soft message rather than rejection.
    assert sim.table_name == "unknown"


async def test_reindex_table():
    sim = await guardrail.simulate_impact("REINDEX TABLE users")
    assert sim.is_allowed is True
    assert sim.table_name == "users"


async def test_reindex_index():
    sim = await guardrail.simulate_impact("REINDEX INDEX idx_users_id")
    assert sim.is_allowed is True


# ---------- Rejected DDL ----------

async def test_multi_statement_rejected():
    sim = await guardrail.simulate_impact(
        "CREATE INDEX CONCURRENTLY foo ON users(id); DROP TABLE users;"
    )
    assert sim.is_allowed is False
    assert sim.status == "REJECTED"
    assert "Multi-statement" in sim.message


async def test_create_index_without_concurrently_rejected():
    sim = await guardrail.simulate_impact("CREATE INDEX idx_u ON users(id)")
    assert sim.is_allowed is False
    assert "CONCURRENTLY" in sim.message


async def test_drop_index_rejected():
    sim = await guardrail.simulate_impact("DROP INDEX idx_u")
    assert sim.is_allowed is False


async def test_update_rejected():
    sim = await guardrail.simulate_impact("UPDATE users SET name = 'x' WHERE id = 1")
    assert sim.is_allowed is False


async def test_delete_rejected():
    sim = await guardrail.simulate_impact("DELETE FROM users WHERE id = 1")
    assert sim.is_allowed is False


async def test_truncate_rejected():
    sim = await guardrail.simulate_impact("TRUNCATE users")
    assert sim.is_allowed is False


async def test_vacuum_rejected_only_analyze_allowed():
    sim = await guardrail.simulate_impact("VACUUM users")
    assert sim.is_allowed is False
    assert "VACUUM" in sim.message


async def test_reindex_database_rejected():
    sim = await guardrail.simulate_impact("REINDEX DATABASE postgres")
    assert sim.is_allowed is False


async def test_reindex_schema_rejected():
    sim = await guardrail.simulate_impact("REINDEX SCHEMA public")
    assert sim.is_allowed is False


async def test_garbage_sql_rejected():
    sim = await guardrail.simulate_impact("this is not sql")
    assert sim.is_allowed is False
    assert sim.status == "REJECTED"


async def test_comment_does_not_smuggle_drop_through_blocklist():
    """v0.1 substring check would have correctly caught DROP-in-comment, but the
    AST parser doesn't even surface the comment — verify allowed cases with
    embedded comments still pass cleanly."""
    sim = await guardrail.simulate_impact(
        "CREATE INDEX CONCURRENTLY /* harmless */ idx_u ON users(id)"
    )
    assert sim.is_allowed is True


# ---------- Risk + duration ----------

async def test_low_risk_under_1gb(monkeypatch: pytest.MonkeyPatch):
    async def small(qualified: str):
        return 0.1, None
    monkeypatch.setattr(guardrail, "_table_size_gb", small)
    sim = await guardrail.simulate_impact("CREATE INDEX CONCURRENTLY i ON users(id)")
    assert sim.risk_score == 2
    assert sim.status == "AUTO_APPROVED"


async def test_action_required_over_1gb(monkeypatch: pytest.MonkeyPatch):
    async def medium(qualified: str):
        return 5.0, None
    monkeypatch.setattr(guardrail, "_table_size_gb", medium)
    sim = await guardrail.simulate_impact("CREATE INDEX CONCURRENTLY i ON users(id)")
    assert sim.risk_score == 6
    assert sim.status == "ACTION_REQUIRED"


async def test_action_required_over_10gb(monkeypatch: pytest.MonkeyPatch):
    async def huge(qualified: str):
        return 50.0, None
    monkeypatch.setattr(guardrail, "_table_size_gb", huge)
    sim = await guardrail.simulate_impact("CREATE INDEX CONCURRENTLY i ON users(id)")
    assert sim.risk_score == 8
    assert sim.status == "ACTION_REQUIRED"
