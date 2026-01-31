# Canonical Pipeline Order

This document defines the **required order** of processing stages for legal, forensic, and quality reasons. Stage order in config **must** follow this sequence.

---

## 1. Ingest (RAW, read-only)

**Goal:** Capture data exactly as received.

**What you do:**
- Read NCERT / Dolma / sources as-is
- Assign `source_id`, `doc_id`
- Store raw text separately (optional: immutable raw copy for audits)

**Why first?**
- Legal & forensic copy
- Enables rollback
- Required for audits

**Rules:**
- ❌ Never do PII or dedup on raw storage
- ✔ Raw is immutable

*In this platform: sources produce `RawDocument`; we assign `doc_id` (e.g. hash of prefix) and build `Document`. Optional raw snapshot can be written before any stage.*

---

## 2. Normalize (minimal & safe)

**Goal:** Make text machine-consistent without changing meaning.

**Allowed:**
- Unicode NFC
- Whitespace cleanup
- OCR artifact fixes
- Control character removal

**Why BEFORE PII?**
- PII detectors rely on patterns
- Normalization improves recall
- Prevents missing masked characters

**Rules:**
- ❌ Do NOT rewrite text (semantic change)
- ❌ Do NOT tokenize

*Stages: `sanitize`, `unicode_normalize`.*

---

## 3. PII detection & handling (HARD GATE)

**Goal:** Ensure no sensitive data goes forward.

**Actions:**
- Detect PII
- Decide: DROP / MASK / KEEP
- Tag `pii_status`

**Why PII comes BEFORE dedup:**
- ❌ If you dedup first: PII may propagate across copies; one bad copy “blesses” others
- ✔ If you PII-scan first: all downstream data is safe; dedup won’t spread sensitive info

PII is a legal concern — it must be handled at the earliest safe point (after normalize).

*Stage: `pii_policy_gate`.*

---

## 4. Deduplication (GLOBAL & SOURCE-AWARE)

**Goal:** Remove repeated content without losing quality.

**What you do:**
- Exact hash
- MinHash / near-dup
- Global fingerprint store (optional)
- Priority-based resolution

**Why AFTER PII?**
- Masked/dropped text must not affect hashes
- Dedup should operate on safe content
- Avoid fingerprinting sensitive info

**Why BEFORE quality filtering?**
- Duplicates artificially inflate “quality”
- Quality signals are distorted by repetition

*Stages: `exact_dedup`, `global_dedup` (optional), `near_dup_minhash`, `semantic_simhash`.*

---

## 5. Quality filtering (light-touch only)

**Goal:** Remove objectively broken data, not “low value” data.

**Drop only if:**
- Empty / near-empty
- OCR garbage
- Boilerplate spam
- Encoding corruption

**Why here?**
- No PII, no duplicates → you can judge structural validity safely

**Rules:**
- ❌ No semantic judgment
- ❌ No “this seems unimportant”

*Stage: `quality_gate`.*

---

## 6. Metadata enrichment

**Goal:** Make the data usable by any downstream team.

**Add metadata:**
- Source, language, script
- PII status, dedup status
- Hashes, processing version

**Why AFTER cleaning?**
- Metadata must reflect final truth
- Avoid mismatches between content & tags

*Stages: `curriculum_eligibility`, `domain_tagging`, `tokenize` (optional).*

---

## 7. Freeze & handoff (STRICT BOUNDARY)

**Goal:** Deliver a stable, reproducible dataset.

**What freeze means:**
- No further modification
- Immutable checksums
- Versioned manifest

*In this platform: writing shards + manifest is the freeze boundary; no stages run after write.*

---

## Config order (recommended)

```yaml
stages:
  - license_gate          # Ingest gate (allowed to process)
  - sanitize              # Normalize: whitespace, HTML, etc.
  - unicode_normalize     # Normalize: Unicode NFC
  - pii_policy_gate       # 3. PII (HARD GATE) — before dedup
  - exact_dedup            # 4. Dedup
  # - global_dedup         # 4. Global fingerprint dedup (if enabled)
  # - near_dup_minhash     # 4. Near-duplicate
  # - semantic_simhash     # 4. Semantic dedup
  - quality_gate          # 5. Quality (light-touch)
  - curriculum_eligibility # 6. Metadata enrichment
  # - domain_tagging       # 6. Metadata
  # - tokenize             # 6. Optional
```

**Do not** put `quality_gate` or any dedup stage **before** `pii_policy_gate`.

---

## Source order and dedup priority

When using **global_dedup**, sources are processed in **priority order** (type first, then optional source within type) so that duplicates are resolved in favor of the higher-priority type/source. Set `document_type_priority` (e.g. `["books", "wiki", "commoncrawl"]`) and `source_to_document_type`; optionally `source_priority` within same type. The pipeline sorts sources by type (then source) before running. See [PARALLEL_AND_DEDUP_PRIORITY.md](PARALLEL_AND_DEDUP_PRIORITY.md) and [GLOBAL_FINGERPRINT_LAYER.md](GLOBAL_FINGERPRINT_LAYER.md) for design.
