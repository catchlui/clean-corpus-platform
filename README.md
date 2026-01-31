# Clean Corpus Platform

A **production-grade, scalable data preprocessing and governance platform** for large-scale language model training. Designed for high-volume ingestion, persistent deduplication, and verifiable data quality.

## Key Features

- ✅ **Global Fingerprint Layer** - Persistent SimHash, MinHash LSH, and Chunk Hash stores for cross-dataset deduplication.
- ✅ **S3-Native Support** - Input, output, and global fingerprints can live on S3; perfect for stateless cloud workers.
- ✅ **Priority-Aware Dedup** - Resolve duplicates by document family (Books > Wiki > Web) or specific source priority.
- ✅ **Policy-Driven Processing** - YAML-based gates for licenses, PII detection, and quality (length, entropy).
- ✅ **Ray & Ray Data Integration** - Scalable processing from local dev to large-scale distributed clusters.
- ✅ **Unified Data Layout** - Standardized `Input-output-exec/` structure for data, runs, logs, and fingerprints.
- ✅ **Rich Monitoring** - Real-time terminal dashboard with per-stage analytics and per-file statistics.
- ✅ **Data Tagging** - Tag data as `training`, `sft`, or `alignment` for downstream filtering.

---

## Installation

### Prerequisites
- Python 3.9+
- AWS credentials (if using S3)

### Install Package
```bash
# Core installation
pip install -e .

# With optional cloud/scaling features
pip install -e ".[s3,ray,pdf,web_pdf]"
```

### Bootstrap
Required to register PII detectors and default policies:
```bash
python scripts/bootstrap_pii.py
```

---

## Quick Start

### 1. Configure your run
Copy the template and add your sources:
```bash
cp configs/standard_template.yaml my_run.yaml
```

### 2. Run the pipeline
```bash
# Local execution
python scripts/run_pipeline.py configs/build.yaml

# Distributed execution (requires Ray)
# Set execution.mode: ray_data in your YAML
python scripts/run_pipeline.py configs/build.yaml
```

### 3. Monitor live
In a separate terminal:
```bash
python -m clean_corpus.cli monitor Input-output-exec/runs/YOUR_RUN_ID --unified
```

---

## How It Works

### 1. Ingestion (Sources)
The platform streams data from diverse sources without full downloads:
- **HuggingFace**: Streams directly from the Hub via `load_dataset(..., streaming=True)`.
- **PDFs**: Local or S3; supports page-level, document-level, or fixed-size chunking.
- **JSONL**: Local files, directories, or glob patterns.
- **Web PDF**: Scrapes and downloads PDFs from URLs (e.g., NCERT) with auto-language detection.

### 2. Canonical Pipeline Order
Stages follow a hard-coded governance order to ensure safety and quality:
1. **Ingest Gate** (License checks)
2. **Normalization** (Unicode NFC, sanitization)
3. **PII Gate** (HARD GATE: must run before deduplication to prevent sensitive data leakage)
4. **Deduplication** (Exact SHA256 + Global Fingerprint stores)
5. **Quality Gate** (Length, character entropy, language filters)
6. **Metadata Enrichment** (Domain tagging, data use tagging)
7. **Freeze & Handoff** (Writing final shards)

### 3. Global Fingerprint Layer
Unlike traditional local-map dedup, this layer is **persistent and cross-dataset**:
- **SimHash**: Fast coarse filtering (rehosted pages, mirrors).
- **MinHash LSH**: Semantic near-duplicate detection.
- **Chunk Hash**: LLM-critical sub-document deduplication (prevents memorization).

### 4. Storage Architecture (`Input-output-exec/`)
All artifacts are consolidated under one root for easy management:
- `data/`: Raw input datasets.
- `runs/`: Output shards, rejections, and summary reports.
- `checkpoints/`: Resume state files.
- `logs/`: System and run logs.
- `fingerprints_global/`: The authoritative global fingerprint stores.

---

## Configuration Reference

### `run`: Execution Identity
```yaml
run:
  # Auto-generate unique ID: {source}_{year}_{time}
  run_id_auto:
    enabled: true
    prefix_digits: 4
    suffix_digits: 6
  out_dir: "Input-output-exec/runs/{run_id}"
```

### `global_fingerprints`: Global Dedup
```yaml
global_fingerprints:
  enabled: true
  root_path: "Input-output-exec/fingerprints_global"
  # Optional: Move store outside the VM to S3
  storage: { type: s3, bucket: my-bucket, prefix: "dedup" }
  
  # Family-level priority (Books beat CommonCrawl)
  document_type_priority: ["books", "wiki", "commoncrawl"]
  source_to_document_type:
    class4_hindi_veena: "books"
    dolma_hf: "commoncrawl"
    
  simhash: { enabled: true, max_hamming: 3 }
  minhash: { enabled: true, threshold: 0.9 }
  chunk_hash: { enabled: true, chunk_size: 512 }
```

### `output`: Format and Tagging
```yaml
output:
  corpus_format: "dolma"  # dolma | parquet | jsonl
  data_tag: "training"    # training | sft | alignment (filterable metadata)
  layout: "structured"   # /processed/v1/documents/{lang}/{domain}/...
```

---

## Distributed Execution (Ray)
For 100M+ documents, switch to `ray_data` mode. The driver handles streaming ingestion and chunks data into Ray blocks, which are processed in parallel across your cluster.

```yaml
execution:
  mode: "ray_data"
  ray_config: "configs/ray.yaml"
```

---

## Documentation

- 📄 **[Global Fingerprint Layer](docs/GLOBAL_FINGERPRINT_LAYER.md)** - Hashing and priority logic.
- 📄 **[S3 & Storage](docs/STORAGE_LOCAL_AND_S3.md)** - Running in the cloud.
- 📄 **[Pipeline Order](docs/PIPELINE_ORDER.md)** - Rationale for stage ordering.
- 📄 **[Streaming & Ray](docs/STREAMING_DOLMA_RAY.md)** - How data moves through the cluster.
- 📄 **[Output Structure](docs/OUTPUT_STRUCTURE.md)** - Organizing processed data.
- 📄 **[Cloud-Native Design](docs/CLOUD_NATIVE_FINGERPRINTS.md)** - Scaling with DynamoDB.

---

## License
MIT
