"""Pure unit tests for risk.score(). No I/O, no async."""

from auto_dba.risk import RiskConfig, RiskFactors, _Band, score


def test_default_config_small_table():
    s = score(RiskFactors(table_size_gb=0.1))
    assert s.size_score == 2
    assert s.total == 2
    assert s.status == "AUTO_APPROVED"


def test_default_config_medium_table():
    s = score(RiskFactors(table_size_gb=5.0))
    assert s.size_score == 6
    assert s.total == 6
    assert s.status == "ACTION_REQUIRED"


def test_default_config_huge_table():
    s = score(RiskFactors(table_size_gb=50.0))
    assert s.size_score == 8
    assert s.total == 8
    assert s.status == "ACTION_REQUIRED"


def test_write_rate_band_composes():
    cfg = RiskConfig(write_rate_band=_Band(thresholds=((1000, 3), (100, 1))))
    s = score(RiskFactors(table_size_gb=5.0, writes_per_minute=2000), cfg)
    # size 6 + write 3 = 9
    assert s.write_rate_score == 3
    assert s.total == 9


def test_index_count_band_composes():
    cfg = RiskConfig(index_count_band=_Band(thresholds=((10, 2),)))
    s = score(RiskFactors(table_size_gb=0.1, existing_index_count=15), cfg)
    # size 2 + idx 2 = 4
    assert s.index_count_score == 2
    assert s.total == 4


def test_total_clamped_to_10():
    cfg = RiskConfig(
        write_rate_band=_Band(thresholds=((0, 5),)),
        index_count_band=_Band(thresholds=((0, 5),)),
    )
    s = score(RiskFactors(table_size_gb=50.0, writes_per_minute=100, existing_index_count=10), cfg)
    # size 8 + write 5 + idx 5 = 18 → clamp to 10
    assert s.total == 10


def test_non_concurrent_penalty_applied_only_when_configured():
    cfg = RiskConfig(non_concurrent_penalty=4)
    s = score(RiskFactors(table_size_gb=0.1, is_concurrent=False), cfg)
    assert s.non_concurrent_penalty == 4
    assert s.total == 6


def test_non_concurrent_penalty_zero_when_concurrent():
    cfg = RiskConfig(non_concurrent_penalty=4)
    s = score(RiskFactors(table_size_gb=0.1, is_concurrent=True), cfg)
    assert s.non_concurrent_penalty == 0
    assert s.total == 2


def test_zero_size_returns_zero_size_score():
    s = score(RiskFactors(table_size_gb=0.0))
    assert s.size_score == 0
    assert s.total == 0


def test_band_threshold_inclusive_boundaries():
    """Threshold is exclusive — value must strictly exceed bound."""
    band = _Band(thresholds=((1.0, 5),))
    assert band.score(1.0) == 0
    assert band.score(1.0001) == 5
