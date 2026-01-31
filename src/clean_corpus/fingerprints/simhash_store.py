"""Global SimHash index (coarse). Fast first-pass filtering."""

from __future__ import annotations
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from ..storage.base import StorageBackend
from .base import FingerprintStore
from .schema import FingerprintRecord, FingerprintType, HashParams

_WORD_RE = re.compile(r"[A-Za-z0-9_]{2,}")


def _simhash64(tokens: List[str]) -> int:
    v = [0] * 64
    for t in tokens:
        h = hash(t)
        for i in range(64):
            bit = (h >> i) & 1
            v[i] += 1 if bit else -1
    out = 0
    for i in range(64):
        if v[i] > 0:
            out |= (1 << i)
    return out & ((1 << 64) - 1)


def hamming_distance(a: int, b: int, bits: int = 64) -> int:
    x = a ^ b
    return bin(x).count("1") if bits == 64 else sum((x >> i) & 1 for i in range(bits))


class SimHashStore(FingerprintStore):
    """Global SimHash store. Key: 64-bit SimHash. Query: Hamming distance <= k."""

    def __init__(
        self,
        storage: StorageBackend,
        root_path: str,
        max_hamming: int = 3,
        max_tokens: int = 2000,
        fingerprint_version: str = "v1",
    ):
        self.storage = storage
        self.root_path = root_path
        self.max_hamming = max_hamming
        self.max_tokens = max_tokens
        self.fingerprint_version = fingerprint_version
        self._hash_params = HashParams(
            fingerprint_version=fingerprint_version,
            max_tokens=max_tokens,
        )
        self._index_path = storage.join(root_path, "simhash_index")
        self._cache: Dict[int, List[FingerprintRecord]] = {}
        self._load()

    @property
    def fingerprint_type(self) -> str:
        return FingerprintType.SIMHASH.value

    def get_version_params(self) -> Optional[HashParams]:
        return self._hash_params

    def _load(self) -> None:
        if not self.storage.exists(self._index_path):
            self.storage.makedirs(self._index_path, exist_ok=True)
            return
        for f in self.storage.list_files(self._index_path, "*.json") or []:
            try:
                data = self.storage.read_file(f)
                obj = json.loads(data.decode("utf-8"))
                sig = int(obj["value"], 16)
                rec = FingerprintRecord(
                    fingerprint_id=obj["fingerprint_id"],
                    fingerprint_type=obj["fingerprint_type"],
                    value=sig,
                    doc_id=bytes.fromhex(obj["doc_id"]),
                    chunk_id=obj.get("chunk_id"),
                    source=obj.get("source", ""),
                    language=obj.get("language", "en"),
                    created_at=obj.get("created_at", 0),
                )
                self._cache.setdefault(sig, []).append(rec)
            except Exception:
                continue

    def _persist_one(self, record: FingerprintRecord) -> None:
        sig = record.value
        key_hex = f"{sig:016x}"
        path = self.storage.join(self._index_path, f"{key_hex}.json")
        payload = {
            "fingerprint_id": record.fingerprint_id,
            "fingerprint_type": record.fingerprint_type,
            "value": key_hex,
            "doc_id": record.doc_id.hex(),
            "chunk_id": record.chunk_id,
            "source": record.source,
            "language": record.language,
            "created_at": record.created_at,
        }
        self.storage.write_file(path, json.dumps(payload).encode("utf-8"))

    def query(
        self,
        value: Any,
        source: Optional[str] = None,
        language: Optional[str] = None,
        hash_params: Optional[HashParams] = None,
    ) -> Tuple[bool, List[FingerprintRecord]]:
        sig = int(value) if not isinstance(value, int) else value
        matches: List[FingerprintRecord] = []
        for cached_sig, recs in self._cache.items():
            if hamming_distance(sig, cached_sig) <= self.max_hamming:
                matches.extend(recs)
        return (len(matches) > 0, matches)

    def add(self, record: FingerprintRecord) -> None:
        sig = record.value
        self._cache.setdefault(sig, []).append(record)
        self._persist_one(record)

    def flush(self) -> None:
        pass

    @staticmethod
    def compute_simhash(text: str, max_tokens: int = 2000) -> int:
        feats = _WORD_RE.findall(text.lower())[:max_tokens]
        return _simhash64(feats)
