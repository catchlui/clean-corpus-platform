"""Global deduplication metrics. Required for verifiable dedupe."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class FingerprintMetrics:
    """Metrics: duplication rate, cross-dataset collision, chunk reuse, source dominance, top duplicated."""

    total_checked: int = 0
    total_dropped: int = 0
    total_kept: int = 0
    total_kept_linked: int = 0
    simhash_hits: int = 0
    minhash_hits: int = 0
    chunk_hash_hits: int = 0
    cross_dataset_collisions: int = 0
    source_drop_counts: Dict[str, int] = field(default_factory=dict)
    source_kept_counts: Dict[str, int] = field(default_factory=dict)
    top_duplicated_sources: List[tuple] = field(default_factory=list)

    @property
    def duplication_rate_pct(self) -> float:
        if self.total_checked == 0:
            return 0.0
        return 100.0 * self.total_dropped / self.total_checked

    @property
    def cross_dataset_collision_rate_pct(self) -> float:
        if self.total_checked == 0:
            return 0.0
        return 100.0 * self.cross_dataset_collisions / self.total_checked

    def record_decision(self, source: str, dropped: bool, kept_linked: bool, match_type: str) -> None:
        self.total_checked += 1
        if dropped:
            self.total_dropped += 1
            self.source_drop_counts[source] = self.source_drop_counts.get(source, 0) + 1
            if match_type == "simhash":
                self.simhash_hits += 1
            elif match_type == "minhash":
                self.minhash_hits += 1
            elif match_type == "chunk_hash":
                self.chunk_hash_hits += 1
        else:
            if kept_linked:
                self.total_kept_linked += 1
            else:
                self.total_kept += 1
            self.source_kept_counts[source] = self.source_kept_counts.get(source, 0) + 1

    def record_cross_dataset_collision(self) -> None:
        self.cross_dataset_collisions += 1

    def compute_top_duplicated_sources(self, n: int = 20) -> None:
        self.top_duplicated_sources = sorted(
            self.source_drop_counts.items(),
            key=lambda x: -x[1],
        )[:n]

    def summary(self) -> str:
        self.compute_top_duplicated_sources()
        lines = [
            "=== Global Fingerprint Metrics ===",
            f"Total checked: {self.total_checked}",
            f"Total dropped (duplicates): {self.total_dropped}",
            f"Total kept: {self.total_kept}",
            f"Total kept (linked): {self.total_kept_linked}",
            f"Duplication rate: {self.duplication_rate_pct:.2f}%",
            f"Cross-dataset collisions: {self.cross_dataset_collisions} ({self.cross_dataset_collision_rate_pct:.2f}%)",
            f"SimHash hits: {self.simhash_hits}",
            f"MinHash hits: {self.minhash_hits}",
            f"Chunk hash hits: {self.chunk_hash_hits}",
            "Top duplicated sources:",
        ]
        for src, cnt in self.top_duplicated_sources:
            lines.append(f"  {src}: {cnt}")
        return "\n".join(lines)
