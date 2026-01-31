"""Global fingerprint manager: coordinate three stores and decision engine."""

from __future__ import annotations
from typing import Any, Dict, List, Optional

try:
    from datasketch import MinHash
except ImportError:
    MinHash = None

from ..storage.base import StorageBackend, LocalStorageBackend
from .base import FingerprintStore
from .schema import (
    FingerprintRecord,
    FingerprintType,
    HashParams,
    DedupDecision,
    DedupAction,
)
from .simhash_store import SimHashStore
from .minhash_store import MinHashStore, _minhash_signature
from .chunk_hash_store import ChunkHashStore
from .metrics import FingerprintMetrics
from .priority import (
    should_keep_incoming_by_priority,
    should_keep_incoming_by_type_and_source,
)


class GlobalFingerprintManager:
    """
    One authoritative global fingerprint layer.
    Flow: generate fingerprints -> query stores -> decision engine -> accept / drop / link.
    """

    def __init__(
        self,
        storage: Optional[StorageBackend] = None,
        root_path: str = "fingerprints_global",
        simhash_enabled: bool = True,
        minhash_enabled: bool = True,
        chunk_hash_enabled: bool = True,
        simhash_max_hamming: int = 3,
        minhash_threshold: float = 0.9,
        minhash_ngram: int = 5,
        minhash_num_perm: int = 128,
        chunk_size: int = 512,
        chunk_overlap: int = 0,
        simhash_drop_on_match: bool = True,
        minhash_drop_on_match: bool = True,
        chunk_drop_duplicate_chunks_only: bool = True,
        fingerprint_version: str = "v1",
        source_priority: Optional[List[str]] = None,
        document_type_priority: Optional[List[str]] = None,
        source_to_document_type: Optional[Dict[str, str]] = None,
    ):
        self.storage = storage or LocalStorageBackend()
        self.root_path = root_path
        self.simhash_drop_on_match = simhash_drop_on_match
        self.minhash_drop_on_match = minhash_drop_on_match
        self.chunk_drop_duplicate_chunks_only = chunk_drop_duplicate_chunks_only
        self.source_priority = source_priority or []
        self.document_type_priority = document_type_priority or []
        self.source_to_document_type = source_to_document_type or {}
        self.metrics = FingerprintMetrics()

        self._simhash: Optional[SimHashStore] = None
        self._minhash: Optional[MinHashStore] = None
        self._chunk: Optional[ChunkHashStore] = None

        if simhash_enabled:
            self._simhash = SimHashStore(
                self.storage,
                self.storage.join(root_path, "simhash"),
                max_hamming=simhash_max_hamming,
                fingerprint_version=fingerprint_version,
            )
        if minhash_enabled and MinHash is not None:
            self._minhash = MinHashStore(
                self.storage,
                self.storage.join(root_path, "minhash"),
                threshold=minhash_threshold,
                ngram=minhash_ngram,
                num_perm=minhash_num_perm,
                fingerprint_version=fingerprint_version,
            )
        if chunk_hash_enabled:
            self._chunk = ChunkHashStore(
                self.storage,
                self.storage.join(root_path, "chunk_hash"),
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                fingerprint_version=fingerprint_version,
            )

    def _should_keep_incoming(self, incoming_source: str, existing_sources: List[str]) -> bool:
        """Keep incoming iff it has higher priority than existing. Type (family) first; then optional source priority within family."""
        if self.document_type_priority and self.source_to_document_type:
            return should_keep_incoming_by_type_and_source(
                incoming_source,
                existing_sources,
                self.document_type_priority,
                self.source_to_document_type,
                self.source_priority if self.source_priority else None,
            )
        if self.source_priority:
            return should_keep_incoming_by_priority(incoming_source, existing_sources, self.source_priority)
        return False

    def query_and_decide(
        self,
        doc_id: bytes,
        source: str,
        language: str,
        text: str,
        simhash_sig: Optional[int] = None,
        minhash_sig: Optional[Any] = None,
        chunk_hashes: Optional[List[tuple]] = None,
    ) -> DedupDecision:
        """
        Generate fingerprints if not provided, query global stores, return decision.
        """
        existing_doc_ids: List[bytes] = []
        existing_sources: List[str] = []
        duplicate_chunk_ids: List[str] = []
        match_type: Optional[str] = None

        if self._simhash is not None:
            sig = simhash_sig if simhash_sig is not None else SimHashStore.compute_simhash(text)
            found, recs = self._simhash.query(sig, source=source, language=language)
            if found:
                match_type = "simhash"
                for r in recs:
                    existing_doc_ids.append(r.doc_id)
                    existing_sources.append(r.source)
                if self.simhash_drop_on_match:
                    keep = self._should_keep_incoming(source, existing_sources)
                    if keep:
                        self.metrics.record_decision(source, dropped=False, kept_linked=False, match_type="simhash")
                        return DedupDecision(
                            action=DedupAction.KEEP,
                            match_type="simhash",
                            existing_doc_ids=existing_doc_ids,
                            existing_sources=existing_sources,
                            reason="SimHash match but incoming has higher priority; keep",
                        )
                    self.metrics.record_decision(source, dropped=True, kept_linked=False, match_type="simhash")
                    if len(set(existing_sources)) > 1:
                        self.metrics.record_cross_dataset_collision()
                    return DedupDecision(
                        action=DedupAction.DROP,
                        match_type="simhash",
                        existing_doc_ids=existing_doc_ids,
                        existing_sources=existing_sources,
                        reason="SimHash match (coarse duplicate)",
                    )

        if self._minhash is not None:
            mh = minhash_sig
            if mh is None and self._minhash._hash_params:
                p = self._minhash._hash_params
                mh = _minhash_signature(text, p.shingle_size, p.num_hashes)
            if mh is not None:
                found, recs = self._minhash.query(mh, source=source, language=language)
                if found:
                    match_type = "minhash"
                    for r in recs:
                        existing_doc_ids.append(r.doc_id)
                        existing_sources.append(r.source)
                    if self.minhash_drop_on_match:
                        keep = self._should_keep_incoming(source, existing_sources)
                        if keep:
                            self.metrics.record_decision(source, dropped=False, kept_linked=False, match_type="minhash")
                            return DedupDecision(
                                action=DedupAction.KEEP,
                                match_type="minhash",
                                existing_doc_ids=existing_doc_ids,
                                existing_sources=existing_sources,
                                reason="MinHash match but incoming has higher priority; keep",
                            )
                        self.metrics.record_decision(source, dropped=True, kept_linked=False, match_type="minhash")
                        if len(set(existing_sources)) > 1:
                            self.metrics.record_cross_dataset_collision()
                        return DedupDecision(
                            action=DedupAction.DROP,
                            match_type="minhash",
                            existing_doc_ids=existing_doc_ids,
                            existing_sources=existing_sources,
                            reason="MinHash near-duplicate",
                        )

        if self._chunk is not None:
            chunks = chunk_hashes
            if chunks is None:
                chunks = ChunkHashStore.compute_chunk_hashes(
                    text,
                    self._chunk.chunk_size,
                    self._chunk.chunk_overlap,
                )
            for chunk_h, chunk_id in chunks:
                found, recs = self._chunk.query(chunk_h)
                if found:
                    duplicate_chunk_ids.append(chunk_id)
                    for r in recs:
                        existing_sources.append(r.source)
            if duplicate_chunk_ids and self.chunk_drop_duplicate_chunks_only:
                self.metrics.chunk_hash_hits += len(duplicate_chunk_ids)
                return DedupDecision(
                    action=DedupAction.KEEP_LINK,
                    match_type="chunk_hash",
                    existing_doc_ids=existing_doc_ids,
                    existing_sources=existing_sources,
                    duplicate_chunk_ids=duplicate_chunk_ids,
                    reason="Partial chunk overlap; keep with link",
                )

        self.metrics.record_decision(source, dropped=False, kept_linked=bool(duplicate_chunk_ids), match_type=match_type or "")
        return DedupDecision(
            action=DedupAction.KEEP if not duplicate_chunk_ids else DedupAction.KEEP_LINK,
            match_type=match_type,
            existing_doc_ids=existing_doc_ids,
            existing_sources=existing_sources,
            duplicate_chunk_ids=duplicate_chunk_ids,
            reason="",
        )

    def add_fingerprints(
        self,
        doc_id: bytes,
        source: str,
        language: str,
        text: str,
        simhash_sig: Optional[int] = None,
        minhash_sig: Optional[Any] = None,
    ) -> None:
        """After accept: persist fingerprints to global stores."""
        hp_sim = self._simhash.get_version_params() if self._simhash else None
        hp_min = self._minhash.get_version_params() if self._minhash else None
        hp_chunk = self._chunk.get_version_params() if self._chunk else None

        if self._simhash is not None:
            sig = simhash_sig if simhash_sig is not None else SimHashStore.compute_simhash(text)
            rec = FingerprintRecord.create(
                FingerprintType.SIMHASH.value,
                sig,
                doc_id,
                source=source,
                language=language,
                hash_params=hp_sim,
            )
            self._simhash.add(rec)

        if self._minhash is not None:
            mh = minhash_sig
            if mh is None and hp_min:
                mh = _minhash_signature(text, hp_min.shingle_size, hp_min.num_hashes)
            if mh is not None:
                rec = FingerprintRecord.create(
                    FingerprintType.MINHASH.value,
                    mh,
                    doc_id,
                    source=source,
                    language=language,
                    hash_params=hp_min,
                )
                self._minhash.add(rec)

        if self._chunk is not None and hp_chunk:
            for chunk_h, chunk_id in ChunkHashStore.compute_chunk_hashes(
                text,
                hp_chunk.chunk_size,
                hp_chunk.chunk_overlap,
            ):
                rec = FingerprintRecord.create(
                    FingerprintType.CHUNK_HASH.value,
                    chunk_h,
                    doc_id,
                    source=source,
                    language=language,
                    chunk_id=chunk_id,
                    hash_params=hp_chunk,
                )
                self._chunk.add(rec)

        self._simhash and self._simhash.flush()
        self._minhash and self._minhash.flush()
        self._chunk and self._chunk.flush()
