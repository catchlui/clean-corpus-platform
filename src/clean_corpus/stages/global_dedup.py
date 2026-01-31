"""Global fingerprint deduplication stage.

Uses GlobalFingerprintManager: generate fingerprints -> query GLOBAL store -> decision -> accept / drop / link.
When enabled, deduplication is global, persistent, and queryable across all datasets and time.
"""

from __future__ import annotations
from typing import Optional

from ..pipeline.context import Document, Decision
from ..fingerprints import GlobalFingerprintManager, DedupAction
from .base import Stage


class GlobalDedupStage(Stage):
    """Stage that queries and updates the global fingerprint store (simhash, minhash, chunk_hash)."""

    name = "global_dedup"
    layer = "dedup"

    def __init__(self, manager: GlobalFingerprintManager):
        self.manager = manager

    def apply(self, doc: Document) -> Decision:
        from ..utils.hashing import sha256_bytes
        doc_id = doc.doc_id if doc.doc_id else sha256_bytes(doc.text)
        if not doc_id:
            return Decision(True, self.name)
        source = doc.source or ""
        language = doc.lang or "en"
        decision = self.manager.query_and_decide(
            doc_id=doc_id,
            source=source,
            language=language,
            text=doc.text,
        )
        if decision.action == DedupAction.DROP:
            doc.transform_chain.append("global_dedup_dropped_v1")
            return Decision(
                False,
                self.name,
                "DUP_GLOBAL",
                decision.reason or f"match_type={decision.match_type}",
            )
        if decision.action in (DedupAction.KEEP, DedupAction.KEEP_LINK):
            if decision.duplicate_chunk_ids:
                doc.transform_chain.append(f"global_dedup_keep_link_chunks={len(decision.duplicate_chunk_ids)}_v1")
            else:
                doc.transform_chain.append("global_dedup_kept_v1")
            self.manager.add_fingerprints(
                doc_id=doc_id,
                source=source,
                language=language,
                text=doc.text,
            )
        return Decision(True, self.name)
