"""Composable risk scoring for proposed DDL.

Pure functions, no I/O. Inputs are gathered by the guardrail layer; this module
only consumes them. Score is the sum of three independently-tunable bands:

- size_score          — table size (drives index-build cost)
- write_rate_score    — writes/min (drives lock and bloat impact)
- index_count_score   — existing index count (more indexes => more write tax post-create)

Total is clamped to [0, 10]. The default `RiskConfig` reproduces the v0.1
behavior (size-only, 2/6/8) so callers without telemetry see no regression.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence


@dataclass(frozen=True)
class RiskFactors:
    table_size_gb: float
    existing_index_count: int = 0
    writes_per_minute: float = 0.0
    is_concurrent: bool = True


@dataclass(frozen=True)
class _Band:
    """A simple step function: returns `score` for the lowest threshold the value clears."""

    thresholds: Sequence[tuple[float, int]]
    """Pairs of (lower_bound_exclusive, score), sorted high-to-low."""

    def score(self, value: float) -> int:
        for bound, points in self.thresholds:
            if value > bound:
                return points
        return 0


@dataclass(frozen=True)
class RiskConfig:
    size_band: _Band = field(
        default_factory=lambda: _Band(thresholds=((10, 8), (1, 6), (0, 2)))
    )
    # Off by default — set to a populated _Band to opt in once telemetry is wired.
    write_rate_band: _Band = field(default_factory=lambda: _Band(thresholds=()))
    index_count_band: _Band = field(default_factory=lambda: _Band(thresholds=()))
    non_concurrent_penalty: int = 0


@dataclass(frozen=True)
class RiskScore:
    total: int
    size_score: int
    write_rate_score: int
    index_count_score: int
    non_concurrent_penalty: int

    @property
    def status(self) -> str:
        return "ACTION_REQUIRED" if self.total > 5 else "AUTO_APPROVED"


def score(factors: RiskFactors, config: RiskConfig | None = None) -> RiskScore:
    cfg = config or RiskConfig()
    size_s = cfg.size_band.score(factors.table_size_gb)
    write_s = cfg.write_rate_band.score(factors.writes_per_minute)
    idx_s = cfg.index_count_band.score(float(factors.existing_index_count))
    penalty = 0 if factors.is_concurrent else cfg.non_concurrent_penalty
    total = max(0, min(10, size_s + write_s + idx_s + penalty))
    return RiskScore(
        total=total,
        size_score=size_s,
        write_rate_score=write_s,
        index_count_score=idx_s,
        non_concurrent_penalty=penalty,
    )
