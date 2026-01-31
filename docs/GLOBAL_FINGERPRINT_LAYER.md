# Global Fingerprint Layer

Deduplication only works if fingerprints are **global**, **persistent**, and **queryable** across all datasets and time. That means: not per dataset, not per job, not per run.

## Architecture

```
All Ingestion Pipelines
        ↓
Fingerprint Generation
        ↓
GLOBAL FINGERPRINT STORE (simhash | minhash | chunk hashes)
        ↓
Duplicate Decision Engine
        ↓
Accept / Drop / Link
```

## What “global” means

- **One authoritative store** — append-only (mostly), queried by every ingestion job, lives across months/years.
- **Not**: local hash maps per dataset, deduping only within one corpus, recomputing hashes per run, resetting dedupe state.

## One technique vs all

You **do not need all three** stores. Enable only what you need:

| Use case | Recommended | Why |
|----------|-------------|-----|
| Byte-identical duplicates only | **exact_dedup** only (no global_dedup) | Content hash catches same text; no SimHash/MinHash/chunk needed. |
| Rehosted pages, mirrors, OCR vs text (fast, coarse) | **SimHash only** | One 64-bit value per doc; fast; catches near-copies. |
| Paraphrased / semantic near-duplicates | **MinHash only** (or SimHash + MinHash) | Jaccard similarity; catches rephrased content. |
| Same paragraph in many docs (memorization) | **Chunk hash only** (or add to above) | Per-chunk hashes; keep doc but link; drop duplicate chunks. |
| Maximum coverage | **All three** | SimHash (coarse) + MinHash (semantic) + Chunk (substring). |

In config, set only the stores you want: `simhash: true`, `minhash: false`, `chunk_hash: false` for SimHash-only; same idea for MinHash-only or chunk-only. At least one of the three must be enabled when `global_fingerprints.enabled` is true.

## Three global stores

| Store | Purpose | Key / Query |
|-------|---------|-------------|
| **SimHash** | Fast first-pass (rehosted pages, PDF mirrors, OCR vs text) | 64-bit SimHash, Hamming distance ≤ k |
| **MinHash + LSH** | Semantic near-duplicate (cross-dataset) | MinHash signatures, LSH buckets, candidate-based query |
| **Chunk Hash** | LLM-critical chunk dedupe (memorization) | SHA-256 of normalized chunks, exact match |

## Schema (minimal metadata, no full text)

- `fingerprint_id`, `fingerprint_type`, `value`, `doc_id`, `chunk_id` (optional), `source`, `language`, `created_at`
- Versioned via `fingerprint_version` and `hash_params` (shingle_size, num_hashes, etc.) for safe rollback and experiments.

## Configuration

In `global.processing.global_fingerprints`:

```yaml
global_fingerprints:
  enabled: true
  root_path: "fingerprints_global"
  simhash: true
  minhash: true
  chunk_hash: true
  simhash_max_hamming: 3
  minhash_threshold: 0.9
  minhash_ngram: 5
  minhash_num_perm: 128
  chunk_size: 512
  chunk_overlap: 0
  fingerprint_version: "v1"
  # Priority by document type (family) first: books > wiki > commoncrawl
  # document_type_priority: ["books", "wiki", "commoncrawl"]
  # source_to_document_type:
  #   class4_hindi_veena: "books"
  #   wikipedia_stream: "wiki"
  #   dolma_hf: "commoncrawl"
  # Optional: within same type, document-level priority (first = highest). Omit to keep existing (drop incoming) within family.
  # source_priority: ["class4_hindi_veena", "dolma_hf"]
```

Add `global_dedup` to the `stages` list (after `exact_dedup`) when `global_fingerprints.enabled` is true.

## Decision logic (example)

| Match type | Action |
|------------|--------|
| SimHash match | Drop or mark duplicate; if **document_type_priority** (or **source_priority**) set, keep higher-priority, drop lower |
| MinHash ≥ threshold | Same: keep one (by priority), drop rest |
| Chunk hash match | **Never drop whole doc** — keep + link (partial chapter); drop chunk only when truly duplicated |
| Partial overlap | Keep, but mark overlap |

### Priority-order dedup (type first, then optional source within family)

Priority is by **document type (family)** first (e.g. books > wiki > commoncrawl). Within the same type, optionally use **source_priority** for document-level order; otherwise within that family we keep existing (drop incoming = random within family).

- **document_type_priority:** Ordered list of document types, e.g. `["books", "wiki", "commoncrawl"]` (first = highest).
- **source_to_document_type:** Map source name → type, e.g. `class4_hindi_veena: "books"`, `dolma_hf: "commoncrawl"`.
- **source_priority:** (Optional) Within same type, which source wins; e.g. `["class4_hindi_veena", "dolma_hf"]`. If omitted, same-type duplicates: keep existing (drop incoming).
- **If duplicate found:** Keep the document whose **type** ranks higher; if same type, keep by **source_priority** if set, else keep existing.
- **Partial chapter:** Chunk-only overlap never drops the whole document; we keep and link. Only full-doc duplicates (SimHash/MinHash) use priority to decide keep vs drop.

## Metrics (tracked globally)

- Duplication rate (%)
- Cross-dataset collision rate
- Chunk reuse rate
- Source dominance
- Top duplicated sources

Access via `GlobalFingerprintManager(...).metrics.summary()` after a run.

## Storage

Default: local filesystem under `root_path`. Optional: S3 or other backends via `global_fingerprints.storage` (same shape as pipeline storage config). Rule: fingerprint store must be faster than ingestion rate.

## Cloud-native / AWS

To run on AWS with a **distributed** fingerprint store (multiple workers, shared dedup lookup) and optional **"what documents are already processed?"** ledger, see **`docs/CLOUD_NATIVE_FINGERPRINTS.md`**. It covers DynamoDB table design for SimHash, MinHash LSH, and chunk hash; an optional processed-documents table; and a migration path from file-based storage to DynamoDB-backed stores.
