# Unified Configuration System

The Clean Corpus Platform now supports a unified YAML configuration system that consolidates dataset configuration, processing functions, storage settings, and execution modes into a single, easy-to-manage file.

## Overview

The unified configuration system provides:

1. **Global Processing Functions**: Configure processing once, apply to all sources
2. **Entry-Level Overrides**: Override global settings per source
3. **Enhanced Checkpoint System**: Start from beginning, specific checkpoint, or ignore data
4. **Unicode NFC Normalization**: Automatic normalization for Indic scripts
5. **High-Scale Deduplication**: Integration with duplodocus for large corpora
6. **Automated Domain Tagging**: FastText + datamap-rs integration
7. **Organized Storage Structure**: Metadata/docs in storage, logs/checkpoints at global level

## Configuration Schema

### Basic Structure

```yaml
run:
  run_id: "ExampleRun_2026-01-30"
  out_dir: "storage_example"  # Storage for metadata and processed output
  shard_docs: 5000
  log_every_docs: 1000
  checkpoint_every_docs: 5000
  policy_version: "policy_v1"

# Global directories (checkpoints and logs)
global:
  checkpoint_dir: "checkpoints"  # Global checkpoint directory
  log_dir: "logs"  # Global log directory
  
  # Global processing functions
  processing:
    enabled: true
    unicode_normalize: true
    deduplication:
      enabled: true
      method: "exact"  # exact | minhash | duplodocus
      duplodocus:
        enabled: false
        exact_match: true
        minhash: true
        disk_based: true
        threshold: 0.9
    
    domain_tagging:
      enabled: false
      method: "fasttext_datamap"
      fasttext_model: null
      datamap_config: null

execution:
  mode: "local"  # local | ray | ray_data

sources:
  - name: "source1"
    kind: "local_jsonl"
    dataset: "data/file.jsonl"
    
    # Entry-level processing overrides (optional)
    processing:
      deduplication:
        method: "minhash"  # Override global setting

policies:
  licenses: "src/clean_corpus/policies/defaults/licenses.yaml"
  quality: "src/clean_corpus/policies/defaults/quality.yaml"
  pii: "src/clean_corpus/policies/defaults/pii.yaml"
  curriculum: "src/clean_corpus/policies/defaults/curriculum.yaml"

stages:
  - license_gate
  - sanitize
  - unicode_normalize
  - exact_dedup
  - quality_gate
  - pii_policy_gate

output:
  corpus_format: "parquet"  # parquet | jsonl | dolma | doml
  metadata_format: "parquet_v1"
  
  # Format-specific options (optional)
  format_options:
    dolma:  # or doml (DOML is alias for DOLMA)
      include_all_metadata: true
      custom_metadata_fields:
        # dataset_version: "v1.0"

checkpoint:
  resume_mode: "auto"  # auto | beginning | checkpoint | ignore
  checkpoint_id: null
```

## Key Features

### 1. Global Processing Functions

Global processing functions apply to all sources unless overridden:

```yaml
global:
  processing:
    unicode_normalize: true  # Apply Unicode NFC normalization
    deduplication:
      enabled: true
      method: "exact"
    domain_tagging:
      enabled: false
```

### 2. Entry-Level Overrides

Each source can override global processing settings:

```yaml
sources:
  - name: "pdf_source"
    kind: "pdf"
    dataset: "data/papers/"
    
    # Override global deduplication method
    processing:
      deduplication:
        method: "minhash"  # Use MinHash instead of exact match
```

### 3. Enhanced Checkpoint System

The checkpoint system supports multiple resume modes:

- **`auto`**: Use existing checkpoint if available, otherwise start from beginning
- **`beginning`**: Ignore existing checkpoint, start from beginning
- **`checkpoint`**: Load specific checkpoint by ID
- **`ignore`**: Ignore all checkpoints and data, start fresh

```yaml
checkpoint:
  resume_mode: "checkpoint"
  checkpoint_id: "ExampleRun_2026-01-30_partial"
```

### 4. Unicode NFC Normalization

Automatic Unicode NFC normalization ensures Indic script consistency:

```yaml
stages:
  - unicode_normalize  # Applies NFC normalization to all text
```

The normalization stage:
- Normalizes text to Unicode NFC (Canonical Composition) form
- Fixes broken encodings
- Ensures consistent representation of Indic scripts
- Runs automatically if enabled in global processing

### 5. High-Scale Deduplication (duplodocus)

For large corpora (3T+ tokens), use duplodocus for disk-based deduplication:

```yaml
global:
  processing:
    deduplication:
      method: "duplodocus"
      duplodocus:
        enabled: true
        exact_match: true
        minhash: true
        disk_based: true  # Use disk-based processing to save RAM
        threshold: 0.9
```

**Note**: Install duplodocus with `pip install duplodocus` before using.

### 6. Automated Domain Tagging

Inject metadata tags using FastText + datamap-rs:

```yaml
global:
  processing:
    domain_tagging:
      enabled: true
      method: "fasttext_datamap"
      fasttext_model: "path/to/model.bin"
      datamap_config: "path/to/datamap_config.yaml"
```

**Note**: Install dependencies with:
- `pip install fasttext`
- `pip install datamap-rs` (if available)

### 7. Storage Structure

The unified system organizes files as follows:

```
storage_example/              # Storage directory (out_dir)
├── docs/                     # Processed output files
│   └── source=example/
│       └── shard_000000.parquet
├── metadata/                 # Metadata files
│   └── schema=meta_v1/
│       └── source=example/
│           └── shard_000000.parquet
├── manifests/                # Run manifests
│   └── ExampleRun_2026-01-30.json
├── rejections/              # Rejected documents
│   └── rejections.jsonl
└── analytics/               # Analytics data
    ├── events/
    └── aggregates/

checkpoints/                 # Global checkpoint directory
└── ExampleRun_2026-01-30.json

logs/                        # Global log directory
└── ExampleRun_2026-01-30.log
```

## Usage Examples

### Basic Configuration

```yaml
run:
  run_id: "BasicExample"
  out_dir: "storage_basic"

global:
  checkpoint_dir: "checkpoints"
  log_dir: "logs"
  processing:
    unicode_normalize: true

execution:
  mode: "local"

sources:
  - name: "data"
    kind: "local_jsonl"
    dataset: "data.jsonl"

policies:
  licenses: "src/clean_corpus/policies/defaults/licenses.yaml"
  quality: "src/clean_corpus/policies/defaults/quality.yaml"
  pii: "src/clean_corpus/policies/defaults/pii.yaml"
  curriculum: "src/clean_corpus/policies/defaults/curriculum.yaml"

stages:
  - license_gate
  - sanitize
  - unicode_normalize
  - exact_dedup
  - quality_gate
  - pii_policy_gate

output:
  corpus_format: "parquet"  # parquet | jsonl | dolma | doml
  metadata_format: "parquet_v1"
  
  # Format-specific options (optional)
  format_options:
    dolma:  # or doml (DOML is alias for DOLMA)
      include_all_metadata: true
      custom_metadata_fields:
        # dataset_version: "v1.0"

checkpoint:
  resume_mode: "auto"
```

### High-Scale Deduplication

```yaml
global:
  processing:
    deduplication:
      method: "duplodocus"
      duplodocus:
        enabled: true
        exact_match: true
        minhash: true
        disk_based: true

stages:
  - license_gate
  - sanitize
  - unicode_normalize
  - duplodocus_dedup  # Use duplodocus instead of exact_dedup
  - quality_gate
```

### Domain Tagging

```yaml
global:
  processing:
    domain_tagging:
      enabled: true
      fasttext_model: "models/lid.176.bin"
      datamap_config: "configs/domain_mapping.yaml"

stages:
  - license_gate
  - sanitize
  - unicode_normalize
  - exact_dedup
  - quality_gate
  - domain_tagging  # Add domain tags
  - pii_policy_gate
```

### Resume from Specific Checkpoint

```yaml
checkpoint:
  resume_mode: "checkpoint"
  checkpoint_id: "ExampleRun_2026-01-30_partial"
```

## Migration from Old Configuration

Old configurations continue to work. The system automatically:
- Uses `out_dir/logs` for logs if `global.log_dir` is not specified
- Uses `out_dir/checkpoints` for checkpoints if `global.checkpoint_dir` is not specified
- Applies default processing settings if `global.processing` is not specified

To migrate:
1. Add `global` section with `checkpoint_dir` and `log_dir`
2. Add `global.processing` section if you want global processing functions
3. Add `checkpoint` section for enhanced checkpoint control
4. Optionally add entry-level `processing` overrides to sources

## Canonical pipeline order

Stage order must follow: **Ingest gate → Normalize → PII (before dedup) → Dedup → Quality → Metadata → Freeze.** PII must run before dedup so sensitive data does not propagate; quality runs after dedup so duplicates do not distort quality signals. See **`docs/PIPELINE_ORDER.md`** for the full rationale and rules.

## See Also

- `docs/RUNS_AND_CHECKPOINTS.md` - One directory for all runs, data layout, global vs run dirs, checkpoint and how to restart
- `docs/STORAGE_LOCAL_AND_S3.md` - Local default; input, output, and analytics can optionally use S3
- `docs/PIPELINE_ORDER.md` - Canonical pipeline order (Ingest → Normalize → PII → Dedup → Quality → Metadata → Freeze)
- `examples/build_unified.yaml` - Complete example configuration
- `docs/HOW_TO_ADD_FILES.md` - Adding new sources
- `docs/WEB_PDF_DOWNLOADER.md` - Web PDF configuration
