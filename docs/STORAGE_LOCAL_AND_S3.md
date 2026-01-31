# Storage: Local and S3 (Input, Output, Analytics)

**Local is the default and always supported.** We expand from there: input data, output data, and analytics can all optionally use S3 so the same pipeline can run on a laptop (local) or in the cloud (S3).

---

## 1. Default: local everywhere

| What | Default | Location |
|------|---------|----------|
| **Input data** | Local | `data/`, paths in `sources[].dataset` (e.g. `data/class4_hindi_veena/dhve101.pdf`) |
| **Output data** | Local | `run.out_dir` (e.g. `runs/<run_id>/`) — docs, rejections, manifests, reports |
| **Analytics** | Local | `{out_dir}/analytics/` — events and aggregates Parquet |
| **Checkpoints** | Local | `global.checkpoint_dir` (e.g. `checkpoints/`) |
| **Logs** | Local | `global.log_dir` (e.g. `logs/`) |
| **Fingerprints** | Local | `global_fingerprints.root_path` (e.g. `fingerprints_global/`) or `global_fingerprints.storage` (S3) |

No config change needed for local; everything works with paths under the project (or absolute local paths).

---

## 2. S3 for input data

**Goal:** Read sources from S3 (e.g. `s3://bucket/input/ncert/`, `s3://bucket/raw/*.jsonl`).

**Today:** Sources (PDF, local_jsonl) expect local paths or URLs. Dataset paths are passed as strings (e.g. `data/...`).

**Expand:**

- **Option A — Path convention:** If `sources[].dataset` starts with `s3://`, resolve it via a shared storage backend: list objects under the prefix, download to a temp dir or stream. PDF source would need to "open" from S3 (download to temp or use a backend that yields bytes). Local_jsonl would list S3 keys and read each object.
- **Option B — Explicit storage per source:** Add optional `sources[].storage: { type: s3, bucket: ..., prefix: ... }` and use it for that source’s dataset path (interpret path as key prefix).

**Config (future):**

```yaml
# Input from S3 (expansion)
sources:
  - name: ncert_s3
    kind: pdf
    dataset: "ncert/class4_hindi_veena/"   # prefix under bucket
    storage:
      type: s3
      bucket: my-corpus-bucket
      prefix: input/
```

Local remains default: if `storage` is omitted, `dataset` is a local path (or file URL).

---

## 3. S3 for output data

**Goal:** Write run output (documents, rejections, manifests, reports) to S3 so `out_dir` is an S3 prefix (e.g. `s3://bucket/runs/<run_id>/`).

**Today:**

- **Writers:** Most writers (JSONL, Parquet, Dolma) write with `open()` and `os.path.join(out_dir, ...)`. Only **S3 Parquet** exists: `writers/s3_parquet.py` uses a storage backend and treats `out_dir` as a prefix.
- **Rejections and manifest:** `storage/writer.py` has `append_jsonl()` and `write_manifest()` — local only (open/write).

**Expand:**

- **Unified output backend:** Introduce a config-driven "output storage" backend (local vs S3). When S3:
  - `out_dir` = S3 prefix (e.g. `runs/Dolma_HF_NCERT_2026-01-30` with bucket from config).
  - All writers receive the same backend (or a path abstraction) and write via `backend.write_file(key, data)`.
  - `append_jsonl` and `write_manifest` use the same backend (append = read existing object, append lines, write back; or use multipart / append pattern).
- **Config:** e.g. `run.out_dir: "runs/<run_id>"` plus `run.storage: { type: s3, bucket: my-bucket }` so output goes to `s3://my-bucket/runs/<run_id>/`.

**Config (future):**

```yaml
run:
  run_id: "Dolma_HF_NCERT_2026-01-30"
  out_dir: "runs/Dolma_HF_NCERT_2026-01-30"
  # Optional: send output to S3
  storage:
    type: s3
    bucket: my-corpus-bucket
    prefix: ""   # optional; out_dir is then prefix under bucket
```

If `run.storage` is omitted, `out_dir` is a local path (current behavior).

---

## 4. S3 for analytics data

**Goal:** Write analytics events and aggregates to S3 (e.g. `s3://bucket/runs/<run_id>/analytics/` or a dedicated analytics prefix).

**Today:** `AnalyticsSink` uses `out_dir` and writes Parquet under `{out_dir}/analytics/events/` and `{out_dir}/analytics/aggregates/` with local `open()` / `pq.write_table()`.

**Expand:**

- **Analytics backend:** Pass an optional storage backend into `AnalyticsSink`. When S3:
  - Build keys like `{out_dir}/analytics/events/stage=.../date=.../events.parquet`.
  - Write Parquet to a buffer, then `backend.write_file(key, buffer.getvalue())`.
- **Config:** Reuse the same output storage (so if run output is S3, analytics go under the same prefix). Or a separate `analytics.storage` for a dedicated analytics bucket/prefix.

**Config (future):**

```yaml
# If run.storage is S3, analytics go to the same bucket/prefix
# run.storage: { type: s3, bucket: my-bucket }
# => analytics at s3://my-bucket/runs/<run_id>/analytics/...

# Or separate analytics location
# analytics:
#   storage: { type: s3, bucket: my-analytics-bucket, prefix: "aggregates/" }
```

Local remains default: if no S3 storage is set, analytics stay under local `out_dir`.

---

## 5. Single config shape (local + optional S3)

Keep one place for "where things live": local by default, optional S3 override.

```yaml
# ---------- Local (default) ----------
run:
  out_dir: "runs/Dolma_HF_NCERT_2026-01-30"
# No storage key => local paths

global:
  checkpoint_dir: "checkpoints"
  log_dir: "logs"

# ---------- Optional S3 for output and analytics ----------
run:
  out_dir: "runs/Dolma_HF_NCERT_2026-01-30"
  storage:
    type: s3
    bucket: my-corpus-bucket
    prefix: ""                    # optional
    region: us-east-1             # optional

# Input from S3 (per-source or global)
sources:
  - name: ncert
    dataset: "data/class4_hindi_veena/dhve101.pdf"   # local
  # - name: dolma_s3
  #   dataset: "dolma/v1_7/"
  #   storage: { type: s3, bucket: my-bucket, prefix: "input/" }

# Fingerprints already support S3
global:
  processing:
    global_fingerprints:
      enabled: true
      root_path: "fingerprints_global"
      storage: { type: s3, bucket: my-corpus-bucket, prefix: "fingerprints/" }
```

- **Input:** Local paths by default; optional `storage` per source (or global) for S3.
- **Output:** Local `out_dir` by default; optional `run.storage` (S3) so docs, rejections, manifests, reports go to S3.
- **Analytics:** Same as output — if `run.storage` is S3, analytics go under the same prefix; otherwise local under `out_dir`.
- **Fingerprints:** Already support `global_fingerprints.storage` (S3) today.
- **Checkpoints / logs:** Can stay local (or later add optional S3 for durability); often kept local or on a shared FS in the cloud.

---

## 6. Summary

| Data | Local (default) | S3 (expand) |
|------|-----------------|-------------|
| **Input** | `data/...`, `sources[].dataset` | Per-source or global `storage: { type: s3, ... }`; dataset = prefix or key |
| **Output** | `run.out_dir` (e.g. `runs/<run_id>/`) | `run.storage: { type: s3, bucket, prefix }`; out_dir = key prefix |
| **Analytics** | `{out_dir}/analytics/` | Same backend as output (S3 prefix) or dedicated `analytics.storage` |
| **Fingerprints** | `root_path` on local FS | Already: `global_fingerprints.storage: { type: s3, ... }` |
| **Checkpoints / logs** | `checkpoints/`, `logs/` | Keep local or add optional S3 later |

**Local stays the default;** S3 is an expansion via config so that both input and output (and analytics) can live on S3 when you need it.

---

## 7. Streaming and Ray with S3

For how streaming sources (e.g. Hugging Face Dolma), data copy, and Ray interact with local or S3 output, see **[STREAMING_DOLMA_RAY.md](STREAMING_DOLMA_RAY.md)**.
