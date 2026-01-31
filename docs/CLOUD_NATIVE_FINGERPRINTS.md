# Cloud-Native Design: Distributed Fingerprints on AWS

This document outlines a plan to run the global fingerprint layer on AWS with a **distributed** store so that:

1. **Deduplication lookup** works across multiple workers, regions, and runs (same global store for all ingestion jobs).
2. **Visibility into processed documents** — query "what documents have already been processed?" by source, run, or doc_id.

---

## 1. Goals

| Goal | Description |
|------|-------------|
| **Distributed dedup lookup** | Any ingestion job (EC2, ECS, Lambda, or another account/region) can query the same fingerprint store and add new fingerprints without local state. |
| **Processed-documents view** | Answer: "Has doc_id X been processed?", "List docs processed from source Y", "List docs in run Z." Optional ledger table alongside fingerprints. |
| **Faster than ingestion** | Store must handle read/write throughput higher than your ingestion rate (DynamoDB, Redis, or similar). |
| **No single point of failure** | Use managed, multi-AZ services (DynamoDB, S3). |

---

## 2. Current State (Single-Node / File-Based)

Today the platform supports:

- **Local filesystem** — `fingerprints_global/` with one JSON file per key (SimHash value, chunk hash, or MinHash doc index). Loaded into memory on startup; fine for one machine.
- **S3** — Same layout via `storage.type: s3` in `global_fingerprints.storage`; still "list + read all" on init, so not suitable for very large or distributed workloads.

Limitations for AWS / multi-worker:

- No shared, low-latency key-value API (SimHash/chunk exact lookup; MinHash LSH buckets).
- No native "query by Hamming distance" (SimHash) in S3; would require scanning.
- "What’s already processed?" is not first-class; you’d infer from fingerprints or run manifests only.

---

## 3. Target: Distributed Store on AWS

### 3.1 Service choices

| Use case | Recommended AWS service | Why |
|----------|--------------------------|-----|
| **Fingerprint key-value + LSH buckets** | **DynamoDB** | Low-latency, scalable, multi-AZ; good for key-value and band→doc_id mapping. |
| **Bulk backup / cold storage** | **S3** | Optional: export DynamoDB to S3 for audit or replay. |
| **Hot path (optional)** | **ElastiCache (Redis)** | Optional cache in front of DynamoDB for highest QPS. |
| **Processed-documents ledger** | **DynamoDB** (separate table or GSI) | Query by doc_id, source, run_id; TTL optional for retention. |

### 3.2 DynamoDB table design

Schema below uses **single-table** or **one table per store**; either is valid. Prefer one table per store for clearer access patterns and cost attribution.

---

#### Table 1: SimHash store

- **Purpose:** Coarse dedup; "have we seen this SimHash (or one within Hamming ≤ k) before?"
- **Access:** Point lookup by SimHash; for Hamming, either exact only in DynamoDB or "prefix + filter" (see note).

| Attribute | Type | Key | Description |
|-----------|------|-----|-------------|
| `pk` | String | Partition | `simhash#{fingerprint_version}#{simhash_hex}` (e.g. 16 hex chars) |
| `sk` | String | Sort | `doc#{doc_id_hex}` |
| `source` | String | | Dataset/source name |
| `language` | String | | Language code |
| `created_at` | Number | | Unix timestamp |
| `fingerprint_id` | String | | UUID of this fingerprint record |

- **Query (exact):** GetItem/BatchGetItem by `pk`; Query by `pk` to list all docs for that SimHash.
- **Query (Hamming ≤ k):** DynamoDB has no native Hamming. Options: (a) **Exact only** in DynamoDB and rely on MinHash for near-dup; (b) **Prefix table**: store a short prefix (e.g. first 16 bits) as pk, then scan items in partition and compute Hamming in app; (c) **Separate index** (e.g. Elasticsearch with a bit field). For many use cases, SimHash exact + MinHash semantic is enough.

---

#### Table 2: MinHash LSH store

- **Purpose:** Semantic near-duplicate; LSH bands → candidate doc_ids.
- **Access:** One partition per band; each band hash maps to a list of doc_ids.

| Attribute | Type | Key | Description |
|-----------|------|-----|-------------|
| `pk` | String | Partition | `minhash#{version}#band#{band_id}#{band_hash}` |
| `sk` | String | Sort | `doc#{doc_id_hex}` |
| `source` | String | | |
| `language` | String | | |
| `created_at` | Number | | |
| `fingerprint_id` | String | | |

- **Query:** For a new doc’s MinHash, compute band hashes; for each band, Query `pk = minhash#v1#band#0#<hash>`. Collect doc_ids; optionally re-check Jaccard in app. **Add:** For each band, PutItem pk+sk.

---

#### Table 3: Chunk hash store

- **Purpose:** Exact chunk dedup; "have we seen this chunk (SHA-256) before?"
- **Access:** Point lookup by chunk_hash.

| Attribute | Type | Key | Description |
|-----------|------|-----|-------------|
| `pk` | String | Partition | `chunk#{version}#{chunk_hash_hex}` |
| `sk` | String | Sort | `doc#{doc_id_hex}#{chunk_id}` |
| `source` | String | | |
| `language` | String | | |
| `created_at` | Number | | |
| `fingerprint_id` | String | | |

- **Query:** GetItem/Query by `pk` to see if any doc already has this chunk. **Add:** PutItem pk+sk.

---

#### Table 4 (optional): Processed documents ledger

- **Purpose:** "What documents have already been processed?" — by doc_id, source, or run_id.
- **Access:** Get by doc_id; list by source or run_id (use GSI).

| Attribute | Type | Key | Description |
|-----------|------|-----|-------------|
| `pk` | String | Partition | `doc#{doc_id_hex}` |
| `sk` | String | Sort | `run#{run_id}` or `source#{source}#{timestamp}` |
| `source` | String | GSI partition (e.g. `source-index`) | Source name |
| `run_id` | String | GSI partition (e.g. `run-index`) | Run identifier |
| `created_at` | Number | GSI sort | Ingestion time |
| `status` | String | | e.g. `accepted`, `rejected` |
| `fingerprint_version` | String | | For audit |

- **Query:** "Was doc X processed?" → GetItem(pk=`doc#{doc_id_hex}`). "List docs from source Y" → GSI on source. "List docs in run Z" → GSI on run_id.
- **Write:** After a doc is accepted (and fingerprints added), put one item into this table. Optional: also write on reject with status=rejected.

---

## 4. Implementation Plan

### Phase 1: Backend interface (no AWS yet)

- Define a **FingerprintStoreBackend** (or extend existing **StorageBackend** usage) that supports:
  - `query_*(...)` and `add(...)` per store type (SimHash, MinHash, Chunk) without assuming file layout.
- Keep current file-based stores as the default; new backends implement the same contract.

### Phase 2: DynamoDB-backed stores

- **DynamoDBSimHashStore** — uses Table 1; `query(simhash)` → Query by pk; `add(record)` → PutItem. Optionally implement "prefix + Hamming filter" for near-match.
- **DynamoDBMinHashStore** — uses Table 2; on `add`, compute LSH bands and insert one item per band; on `query(signature)`, compute bands, Query each pk, merge doc_ids, return records (and optionally re-check similarity).
- **DynamoDBChunkHashStore** — uses Table 3; `query(chunk_hash)` → Query by pk; `add` → PutItem.
- **ProcessedDocsLedger** (optional) — new module; `record_processed(doc_id, source, run_id, ...)` and `was_processed(doc_id)` / `list_by_source(source)` / `list_by_run(run_id)` using Table 4.

### Phase 3: Config-driven backend selection

- In `global_fingerprints` config, support something like:

```yaml
global_fingerprints:
  enabled: true
  root_path: "fingerprints_global"   # used for local/S3 prefix
  backend: "dynamodb"                # local | s3 | dynamodb
  storage:                           # for local/s3
    type: s3
    bucket: my-bucket
    prefix: fingerprints
  dynamodb:                          # when backend: dynamodb
    region: us-east-1
    simhash_table: clean-corpus-simhash
    minhash_table: clean-corpus-minhash
    chunk_table:  clean-corpus-chunk
    processed_table: clean-corpus-processed   # optional
```

- **Checkpoint** and **run state** stay as today (e.g. S3 or shared FS for `checkpoints/{run_id}.json`); they are separate from the fingerprint store and processed-docs ledger.

### Phase 4: Observability and safety

- **Metrics:** Duplication rate, cross-dataset collision rate, chunk reuse rate (already in `FingerprintMetrics`); expose to CloudWatch or your metrics system.
- **Idempotency:** PutItem with conditional expression or doc_id+fingerprint_id as sk so re-runs don’t duplicate entries.
- **TTL (optional):** On DynamoDB tables, add a TTL attribute for automatic expiry of old fingerprints or processed-docs entries if you need retention limits.

---

## 5. Duplication Lookup vs "Already Processed"

| Question | Where to answer |
|----------|-----------------|
| **"Is this content a duplicate of something we’ve seen?"** | **Fingerprint store** (SimHash / MinHash / Chunk). Query by fingerprint; if match, it’s a duplicate. |
| **"Have we already processed document X (by doc_id)?"** | **Processed-docs ledger** (Table 4). GetItem by doc_id. |
| **"List all documents ingested from source Y / run Z"** | **Processed-docs ledger** via GSI on source or run_id. |

Fingerprints answer **content** dedup; the ledger answers **identity** and **audit** (what was processed, when, and in which run). Both can be used together: e.g. skip re-ingestion if doc_id is in the ledger; and drop or link if fingerprint matches.

---

## 6. Summary

- **Distributed fingerprint store on AWS:** Use **DynamoDB** for SimHash (exact or prefix), MinHash LSH bands, and chunk hash; optional **S3** for backup, optional **Redis** for caching.
- **Processed-documents view:** Add an optional **DynamoDB table** (and GSI) keyed by doc_id, source, run_id so you can check "already processed?" and list docs by source or run.
- **Alignment with current code:** Keep existing `FingerprintStore` interface and file-based implementations; add DynamoDB-backed implementations and a config-driven backend so the same pipeline can run locally, on a single EC2, or in a distributed AWS setup with shared dedup and visibility.
