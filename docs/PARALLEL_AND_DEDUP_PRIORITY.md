# Parallel Execution and Dedup Priority

## Goal

- **Most work runs in parallel** where safe (ingestion, normalize, PII, quality, metadata, writing).
- **Only global deduplication is ordered by source priority** so that when two documents are duplicates, the higher-priority source wins (e.g. NCERT over Dolma).

## Design

### 1. Source processing order = priority order (for dedup correctness)

Global dedup uses a **single, persistent fingerprint store**. For priority to work:

- Documents from **higher-priority sources must be seen first** by the global_dedup stage.
- When a lower-priority document arrives and matches an existing fingerprint, it is **dropped**; the one already in the store (higher priority) is kept.

So **sources are processed in priority order**: highest-priority source runs through the full pipeline first (including global_dedup), then the next, and so on. That way the fingerprint store is filled with high-priority content before lower-priority streams are checked.

- **Config (type-based):** `document_type_priority` (e.g. `["books", "wiki", "commoncrawl"]`) and `source_to_document_type` (source name → type). Sources are sorted by type first (highest first), then by optional `source_priority` within same type.
- **Config (legacy):** `source_priority` only (e.g. `["class4_hindi_veena", "dolma_hf"]`) — sources sorted by this list (highest first).
- **Implementation:** Before the pipeline iterates over sources, it **sorts** the source list by type rank (then source rank if set). Sources not listed get the lowest priority (processed last).
- **Optional:** `execution.source_order: "priority" | "config"` — use `"priority"` when global dedup is enabled (default), `"config"` to keep YAML order.

### 2. What can run in parallel

| Component | Parallel? | Notes |
|-----------|-----------|--------|
| **Across sources** | No (when global_dedup is used) | Sources are processed in **priority order** so dedup sees high-priority docs first. |
| **Within a source** | Yes (future) | Per-doc stages (normalize, PII, quality) can be batched or parallelized (e.g. Ray, multiprocessing). |
| **Stages before global_dedup** | Yes within source | Ingest → normalize → PII → exact_dedup: no cross-source state. |
| **global_dedup** | Single-threaded per run | One fingerprint store; inputs must be in priority order. |
| **Stages after global_dedup** | Yes within source | quality_gate → metadata → write. |
| **Writing shards** | Yes | Per-shard writes are independent. |

So today: **sources run one after another in priority order**. Within each source, the pipeline is sequential per document; future work can add within-source parallelism (e.g. Ray Data, batch processing).

### 3. Two-phase option (future)

For maximum parallelism while keeping dedup priority:

- **Phase 1 (parallel):** All sources run in parallel through **pre-dedup** stages (ingest → normalize → PII → exact_dedup). Output: intermediate buffer or stream per source.
- **Phase 2 (single, ordered):** Merge streams in **source-priority order** (all docs from source A, then source B, …). Run **global_dedup** only. Output: accepted doc stream.
- **Phase 3 (parallel):** Run quality, metadata, and write in parallel on the accepted stream.

This requires intermediate buffering/streaming and a merge step; it is a larger change than priority-ordered source iteration.

### 4. Summary

- **Dedup priority:** Set `document_type_priority` and `source_to_document_type` (and optionally `source_priority` within same type); enable `global_fingerprints`; add `global_dedup` to stages. Sources are **processed in type order** (then source order within type). Within same type without `source_priority`, keep existing (drop incoming).
- **Parallelism today:** Source order is the only constraint for correctness. Within-source parallelism (e.g. Ray) can be added later without changing dedup semantics.
- **Config:** Use `source_order: "priority"` (default when global dedup enabled) so the pipeline sorts sources by type/source priority instead of YAML order.
