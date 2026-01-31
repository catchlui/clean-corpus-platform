# Runs Directory Layout and Checkpoints

## 1. One directory for all runs and data

**Recommended layout:** Either at project root or under a single **Input-output-exec** folder:

**Option A — Under Input-output-exec (all input/output/exec in one place):**

```
<project_root>/
├── Input-output-exec/
│   ├── data/                 # Input data (read-only)
│   │   └── class4_hindi_veena/
│   ├── runs/                  # All pipeline runs (one subdir per run)
│   │   └── Dolma_HF_NCERT_2026-01-30/
│   │       ├── docs/          # or documents/
│   │       ├── rejections/
│   │       ├── manifests/
│   │       ├── reports/
│   │       └── analytics/
│   ├── checkpoints/           # Global: one file per run_id
│   ├── logs/                  # Global: one log per run_id
│   └── fingerprints_global/   # Global fingerprint store (dedup)
├── configs/
├── scripts/
└── ...
```

**Option B — At project root:**

```
<project_root>/
├── data/                    # Input data only (read-only for pipeline)
│   └── ...
├── runs/                     # All pipeline runs (one subdir per run)
│   └── ...
├── checkpoints/              # Global: one file per run_id
├── logs/                     # Global: one log file per run_id
└── fingerprints_global/      # Global fingerprint store (dedup)
```

**Config convention:** Put every run under a single base directory. For **Input-output-exec** layout:

```yaml
run:
  run_id: "Dolma_HF_NCERT_2026-01-30"
  out_dir: "Input-output-exec/runs/Dolma_HF_NCERT_2026-01-30"

global:
  checkpoint_dir: "Input-output-exec/checkpoints"
  log_dir: "Input-output-exec/logs"
  processing:
    global_fingerprints:
      root_path: "Input-output-exec/fingerprints_global"

# Source dataset paths under the same base
sources:
  - name: class4_hindi_veena
    dataset: "Input-output-exec/data/class4_hindi_veena/dhve101.pdf"
```

For project-root layout, use `runs/<run_id>`, `checkpoints`, `logs`, `data/...` without the `Input-output-exec/` prefix.

- **data/** — Input only. Paths in `sources[].dataset` point here (e.g. `data/class4_hindi_veena/dhve101.pdf`). Can be expanded to S3 (see **`docs/STORAGE_LOCAL_AND_S3.md`**).
- **runs/<run_id>/** — Output for that run only: documents, rejections, manifests, stats, reports. Can be S3 (same doc).
- **checkpoints/** — Global. One JSON file per `run_id`; shared across runs.
- **logs/** — Global. One `.log` file per `run_id`.

---

## 2. What lives in the run directory vs global

| Location | Contents | Can move? |
|----------|----------|-----------|
| **Global (project root)** | | |
| `checkpoints/` | One `{run_id}.json` per run. Resume state (processed_docs, shard_idx per source). | Already global. |
| `logs/` | One `{run_id}.log` per run. | Already global. |
| `fingerprints_global/` | SimHash/MinHash/chunk stores (dedup). | Already global. |
| **Run dir (`out_dir`)** | | |
| `documents/` or `docs/` | Written shards (JSONL/Parquet). | Must stay in run dir. |
| `rejected/` or `rejections/` | Rejection records. | Must stay in run dir. |
| `manifests/` | Run manifest (counts, config snapshot). | Must stay in run dir. |
| `stats/` | pii_report, dedup_report, quality_report (structured layout). | Can stay in run dir. |
| `reports/` | Summary report (e.g. `*_summary.txt`). | Can stay in run dir. |
| `analytics/` | Events and aggregates (if enabled). | Optional; could move to global/analytics/{run_id}/ later. |
| `metadata/` | Parquet metadata shards (if metadata_format set). | Run-specific; keep in run dir. |

**Summary:** Logs and checkpoints are already global. The run directory should contain only output that belongs to that run: documents, rejections, manifests, stats, reports. Analytics and metadata are run-specific and stay under the run dir unless you add a separate global analytics layout.

---

## 3. What can be removed or simplified

- **Logs** — Already in global `logs/`; no log files inside the run dir.
- **Checkpoints** — Already in global `checkpoints/`; no checkpoint files inside the run dir.
- **Analytics** — If you don’t use them, you can avoid writing analytics (or add a config flag to disable). Otherwise keep under run dir.
- **Metadata dir** — Only created if `output.metadata_format` is set (e.g. `parquet_v1`). Omit or set to `null` to skip.
- **Run dir name** — Use `out_dir: "runs/{{run_id}}"` (or literally `runs/Dolma_HF_NCERT_2026-01-30`) so all runs sit under one parent.

Nothing else needs to be removed; the run dir stays focused on documents, rejections, manifests, and reports.

---

## 4. How checkpoints work

- **File:** `{global_checkpoint_dir}/{run_id}.json` (e.g. `checkpoints/Dolma_HF_NCERT_2026-01-30.json`).
- **Contents:** Run id, timestamps, and per-source progress:
  - `processed_docs` — number of documents already consumed from that source.
  - `shard_idx` — next shard index to write.
  - `file_stats` — per-file processed/written/rejected (optional).
- **When it’s written:** After each shard flush (and optionally every `checkpoint_every_docs`).
- **Resume:** On the next run with the same config and same `out_dir`, the pipeline loads this file and:
  - Skips the first `processed_docs` records from each source.
  - Writes new shards starting at `shard_idx`.
  - Leaves already-written shards in the run dir untouched.

Streaming sources (e.g. Hugging Face) are best-effort resume (skip N records); batch sources (e.g. local JSONL, PDF) are deterministic.

---

## 5. How to restart from a checkpoint

**Automatic resume (default):**

1. Use the **same config** and the **same `out_dir`** (and same `run_id`, which is typically in the config).
2. Run the pipeline again:  
   `python scripts/run_pipeline.py configs/build.yaml`
3. The pipeline loads `checkpoints/{run_id}.json` and continues from the last saved state.

**Start from the beginning (ignore checkpoint):**

1. In config set:
   ```yaml
   checkpoint:
     resume_mode: "beginning"
   ```
2. Or delete the checkpoint file and run:
   ```bash
   # Windows
   del checkpoints\Dolma_HF_NCERT_2026-01-30.json
   python scripts/run_pipeline.py configs/build.yaml
   ```
3. Optionally clear the run dir (e.g. `runs/Dolma_HF_NCERT_2026-01-30/`) if you want a completely fresh output.

**Resume modes (in config):**

| resume_mode | Behavior |
|------------|----------|
| `auto` | Use existing checkpoint if present; otherwise start from beginning. |
| `beginning` | Ignore checkpoint; start from beginning. |
| `checkpoint` | Load a specific checkpoint by `checkpoint_id` (e.g. an older backup). |
| `ignore` | Ignore checkpoint and clear source state; start fresh. |

**Rerun from scratch (clean run + clean output):**

1. Delete the checkpoint:  
   `checkpoints/{run_id}.json`
2. Delete or clear the run output dir:  
   `runs/{run_id}/` (or whatever `out_dir` you use).
3. Run the pipeline again with the same config.

---

## 6. Quick reference

- **All runs in one place:** Set `out_dir: "runs/<run_id>"` (or `runs/<your_run_name>`) in config.
- **Data in one place:** Put inputs under `data/` and reference them in `sources[].dataset`.
- **Logs:** Global `logs/{run_id}.log` (set `global.log_dir: "logs"`).
- **Checkpoints:** Global `checkpoints/{run_id}.json` (set `global.checkpoint_dir: "checkpoints"`).
- **Resume:** Same config + same `out_dir` → automatic resume from `checkpoints/{run_id}.json`.
- **Fresh run:** `checkpoint.resume_mode: "beginning"` or delete `checkpoints/{run_id}.json` (and optionally the run dir).

---

## See also

- **`docs/STORAGE_LOCAL_AND_S3.md`** — Local is default; input, output, and analytics can optionally use S3 (expansion path and config shape).
