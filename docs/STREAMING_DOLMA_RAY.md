# Streaming, Dolma, and Ray Data Flow

How data moves from streaming sources (e.g. Hugging Face Dolma), through the pipeline, and into output (Dolma format or Parquet) with optional Ray.

---

## 1. Streaming: no full copy

### Hugging Face streaming source

When you use a source with `kind: hf_stream` and a dataset like `allenai/dolma`:

- **No full download.** `load_dataset(..., streaming=True)` returns an **iterator** over the dataset on the Hugging Face Hub (or cache).
- Records are **fetched on demand** as you consume the iterator (e.g. `for ex in ds: yield RawDocument(...)`).
- **Data copy:** Only the current batch of records is in memory; the rest stays on the hub or in HF cache. So “data copy from Dolma” is **streaming**: records are read one-by-one (or in small chunks by the HF client) and passed into the pipeline. There is no single full copy of the dataset in memory.

### Local runner (no Ray)

1. **Source:** `src.stream()` yields `RawDocument` one at a time from the HF iterator.
2. **Pipeline:** Each raw doc is converted to `Document`, then run through stages (license, sanitize, PII, dedup, quality, …). Accepted docs are buffered into shards (e.g. 5000 docs per shard).
3. **Write:** When a shard is full (or the stream ends), the corpus writer (e.g. Dolma) writes one shard file (e.g. `docs/source=dolma_hf/shard_000000.jsonl`). So **streaming in, sharded files out**; no need to hold the whole dataset in memory.

### Ray Data runner

1. **Source:** Same `src.stream()` iterator on the **driver**. The driver pulls from the stream in **chunks** (e.g. 20,000 records).
2. **Ray:** Each chunk is turned into a Ray Dataset: `ray.data.from_items(buf)`.
3. **Stages:** Each stage runs as `map_batches` on the dataset (e.g. `ds.map_batches(stage_fn, batch_size=512)`). So work is distributed across Ray workers; only the current batch of rows is on each worker.
4. **Write:** After all stages, the pipeline either:
   - **Parquet:** `ds.write_parquet(out_path)` (local or S3 if path is `s3://...`), or
   - **Dolma / JSONL:** Collects the dataset to the driver (`ds.take_all()` → list of rows), converts rows to `Document`, then calls `corpus_writer.write_shard(docs, out_dir=..., ...)`. So for Dolma with Ray, the **last stage’s output** is brought back to the driver and written by the same Dolma writer as in the local runner.

So:

- **Streaming** = records come from HF (or other source) via an iterator; no full dataset copy.
- **Data copy from Dolma** = reading that stream (HF Dolma or any HF dataset) in a streaming fashion; optional copy is only into HF cache on disk, not a second full in-memory copy.
- **Ray** = batches of that stream are processed in parallel; writing is either distributed (Parquet) or driver-side (Dolma/JSONL via current writer).

---

## 2. End-to-end flow (local vs Ray)

```
[HF Hub / Dolma]  streaming=True
        │
        ▼
  src.stream()  ──►  RawDocument, RawDocument, ...
        │
        ├── LOCAL: for each raw → _raw_to_doc → stages → buffer → when shard full → writer.write_shard()
        │
        └── RAY:   chunk (e.g. 20k) → ray.data.from_items(chunk)
                         │
                         ▼
                   map_batches(stage1) → map_batches(stage2) → ...
                         │
                         ├── corpus_format parquet  → ds.write_parquet(path)
                         └── corpus_format dolma    → _collect_docs(ds) → writer.write_shard(docs)
```

- **Rejections:** Stages that reject docs (PII, dedup, quality) drop them; only accepted docs go into the shard. Rejected docs are written to rejection logs (today: local paths).
- **Global dedup:** When `global_dedup` is in the stages, each doc is checked against the global fingerprint store (local or S3); duplicates are dropped before writing.

---

## 3. S3 and “outside the VM”

- **Output to S3:** Use `run.storage: { type: s3, bucket, prefix }` and `corpus_format: s3_parquet`. Corpus Parquet shards are written to S3; manifest/rejections/reports still go to local `out_dir` unless the pipeline is extended with an output storage backend (see [STORAGE_LOCAL_AND_S3.md](STORAGE_LOCAL_AND_S3.md)).
- **Fingerprints outside the VM:** Set `global_fingerprints.storage: { type: s3, bucket, prefix }`. The global SimHash/MinHash/chunk_hash stores then live on S3 so all workers/VMs share the same dedup state. See [CLOUD_NATIVE_FINGERPRINTS.md](CLOUD_NATIVE_FINGERPRINTS.md) for a DynamoDB-based design for very large scale.

---

## 4. Config summary

| Goal                         | Config |
|-----------------------------|--------|
| Stream from Dolma (HF)      | Source `kind: hf_stream`, `dataset: allenai/dolma`, `streaming: true` (implicit with `load_dataset(..., streaming=True)`). |
| Output in Dolma format      | `output.corpus_format: dolma`. |
| Output to S3 (Parquet)      | `run.storage: { type: s3, bucket, prefix }`, `output.corpus_format: s3_parquet`. |
| Fingerprints on S3          | `global_fingerprints.storage: { type: s3, bucket, prefix }`. |
| Use Ray for processing      | `execution.mode: ray_data` and Ray cluster; streaming is still chunked on the driver, then processed with `map_batches`. |

Use **configs/build_s3_template.yaml** as a starting point for S3 output and fingerprints outside the VM.
