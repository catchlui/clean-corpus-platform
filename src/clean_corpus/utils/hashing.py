"""Hashing utilities.

We store `doc_id` as raw 32-byte SHA-256 bytes (not hex strings) for efficiency.

Why SHA-256:
- deterministic across machines
- stable for dedup keys and provenance
- safe for distributed settings
"""

import hashlib

def sha256_bytes(text: str) -> bytes:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).digest()
