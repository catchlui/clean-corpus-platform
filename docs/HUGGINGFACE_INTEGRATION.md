# HuggingFace Dataset Integration Guide

This guide explains how to integrate the Clean Corpus Platform with HuggingFace datasets, including locally downloaded datasets.

## Overview

The platform supports HuggingFace datasets in two ways:
1. **Streaming** (`hf_stream`) - Downloads and processes on-the-fly
2. **Local** - Process datasets downloaded via `huggingface-cli download`

## Method 1: Automatic Download (Recommended)

### Step 1: Download Dataset (Automatic or Manual)

**Option A: Automatic Download (Recommended)**
The pipeline can automatically download datasets for you:

```bash
# Use --auto-download flag - dataset will be downloaded automatically
python scripts/run_pipeline.py build_pg19.yaml --auto-download
```

**Option B: Manual Download via Python Script**
```bash
# Download using Python script
python scripts/download_hf_dataset.py datasets/pg19 --repo-type dataset

# Or download to custom directory
python scripts/download_hf_dataset.py datasets/pg19 --repo-type dataset --local-dir pg19_raw
```

**Option C: Manual Download via CLI**
```bash
# Download pg19 dataset to local directory
huggingface-cli download datasets/pg19 --repo-type dataset --local-dir pg19_raw
```

The automatic download option is recommended as it handles everything for you.

**Important**: The `datasets` library will automatically use your local cache. If you download to a custom directory, you have two options:

**Option A: Use HuggingFace Cache (Recommended)**
```bash
# Download to default cache location (automatic)
huggingface-cli download datasets/pg19 --repo-type dataset
```

**Option B: Set Cache Directory**
```bash
# Set environment variable to use custom cache
export HF_HOME=/path/to/cache  # Linux/Mac
set HF_HOME=C:\path\to\cache    # Windows

# Then download
huggingface-cli download datasets/pg19 --repo-type dataset
```

### Step 2: Verify Dataset Schema

Before configuring, check the dataset structure:

```bash
python scripts/verify_hf_dataset.py datasets/pg19 train
```

Or manually inspect:
```python
from datasets import load_dataset
ds = load_dataset("datasets/pg19", split="train", streaming=True)
sample = next(iter(ds))
print(sample.keys())  # Shows available fields
```

### Step 3: Configure Platform

The platform automatically uses your local cache. Create `build_pg19.yaml`:

```yaml
run:
  run_id: "PG19_2026-01-29"
  out_dir: "storage_pg19"
  shard_docs: 1000
  log_every_docs: 100
  checkpoint_every_docs: 1000
  policy_version: "policy_v1"

execution:
  mode: local

sources:
  - name: "pg19"
    type: "streaming"
    kind: "hf_stream"
    dataset: "datasets/pg19"  # HuggingFace dataset name
    split: "train"  # Check available splits: train, test, validation
    text_field: "text"  # Adjust based on actual schema
    license_field: "license"  # If available
    url_field: "url"  # If available

policies:
  licenses: "src/clean_corpus/policies/defaults/licenses.yaml"
  quality: "src/clean_corpus/policies/defaults/quality.yaml"
  pii: "src/clean_corpus/policies/defaults/pii.yaml"
  curriculum: "src/clean_corpus/policies/defaults/curriculum.yaml"

stages:
  - license_gate
  - sanitize
  - exact_dedup
  - quality_gate
  - pii_policy_gate
  - semantic_simhash
  - curriculum_eligibility
```

### Step 4: Set HuggingFace Cache Directory (Optional)

If you want to use a specific local directory:

```bash
# Set environment variable
export HF_HOME=/path/to/your/cache
# Or on Windows:
set HF_HOME=C:\path\to\your\cache

# Then run pipeline
python -m clean_corpus.cli build --config build_pg19.yaml
```

The `datasets` library will automatically use your local cache if available.

## Method 2: Convert to JSONL (Alternative - Best for Custom Directories)

If you downloaded to a custom directory with `--local-dir`, converting to JSONL is the most reliable approach:

### Step 1: Download Dataset

```bash
huggingface-cli download datasets/pg19 --repo-type dataset --local-dir pg19_raw
```

### Step 2: Convert to JSONL

Use the provided conversion script:

```bash
# Convert from HuggingFace Hub (uses cache automatically)
python scripts/convert_hf_to_jsonl.py datasets/pg19 train pg19_train.jsonl

# OR convert from local directory
python scripts/convert_hf_to_jsonl.py datasets/pg19 train pg19_train.jsonl --local-dir pg19_raw
```

The script will:
- Load the dataset (from cache or local directory)
- Show the schema
- Convert to JSONL format
- Preserve all fields

### Step 3: Process JSONL

Then use `local_jsonl` source:

```yaml
sources:
  - name: "pg19"
    kind: "local_jsonl"
    dataset: "pg19_train.jsonl"
    text_field: "text"
    license_field: "license"
    url_field: "url"
```

**Benefits of JSONL approach:**
- Works with any download location
- Faster processing (no streaming overhead)
- Better error handling
- Can process multiple files together

## Method 3: Direct Streaming (No Local Download)

For smaller datasets or when you want to process on-the-fly:

```yaml
sources:
  - name: "pg19"
    type: "streaming"
    kind: "hf_stream"
    dataset: "datasets/pg19"
    split: "train"
    text_field: "text"
```

The platform will download and process automatically.

## PG19 Dataset Specific Configuration

### Understanding PG19 Schema

PG19 dataset typically has:
- `text`: The book text content
- `short_book_title`: Book title
- `publication_date`: Publication date
- `url`: Project Gutenberg URL

### Recommended Configuration

```yaml
run:
  run_id: "PG19_2026-01-29"
  out_dir: "storage_pg19"
  shard_docs: 5000  # Larger shards for books
  log_every_docs: 500
  checkpoint_every_docs: 5000  # Frequent checkpoints for long processing
  policy_version: "policy_v1"

execution:
  mode: local  # Use 'ray_data' for faster processing

sources:
  - name: "pg19"
    type: "streaming"
    kind: "hf_stream"
    dataset: "datasets/pg19"
    split: "train"
    text_field: "text"  # Main text content
    license_field: "license"  # Usually "Public Domain" for PG19
    url_field: "url"  # Project Gutenberg URL

policies:
  licenses: "src/clean_corpus/policies/defaults/licenses.yaml"
  quality: "src/clean_corpus/policies/defaults/quality.yaml"
  pii: "src/clean_corpus/policies/defaults/pii.yaml"
  curriculum: "src/clean_corpus/policies/defaults/curriculum.yaml"

stages:
  - license_gate
  - sanitize
  - exact_dedup
  - quality_gate
  - pii_policy_gate
  - semantic_simhash
  - curriculum_eligibility
```

## Complete Workflow Example

### 1. Download Dataset

```bash
# Download pg19 dataset
huggingface-cli download datasets/pg19 --repo-type dataset --local-dir pg19_raw

# Verify download
ls -lh pg19_raw/
```

### 2. Verify Schema

```bash
python scripts/verify_hf_dataset.py datasets/pg19 train
```

This will show:
- Available fields
- Sample data
- Suggested configuration

### 3. Create Configuration

Create `build_pg19.yaml` (see example above).

### 4. Run Pipeline

```bash
# Basic run
python -m clean_corpus.cli build --config build_pg19.yaml

# With monitoring
python scripts/run_pipeline.py build_pg19.yaml --monitor
```

### 5. Monitor Progress

In another terminal:
```bash
python -m clean_corpus.cli monitor storage_pg19 --unified
```

## Troubleshooting

### Issue: Dataset Not Found Locally

**Problem**: Platform tries to download even though you have local copy.

**Solution**: 
1. Check HuggingFace cache location:
   ```python
   from datasets import config
   print(config.HF_DATASETS_CACHE)
   ```
2. Ensure your download directory matches the cache structure
3. Or set `HF_HOME` environment variable

### Issue: Wrong Field Names

**Problem**: `text_field` or other fields don't match dataset schema.

**Solution**:
1. Run verification script:
   ```bash
   python scripts/verify_hf_dataset.py datasets/pg19 train
   ```
2. Check actual field names in output
3. Update `text_field`, `license_field`, `url_field` in config

### Issue: Out of Memory

**Problem**: Large dataset causes memory issues.

**Solution**:
1. Use streaming mode (already default for `hf_stream`)
2. Reduce `shard_docs` size
3. Use Ray Data mode:
   ```yaml
   execution:
     mode: ray_data
   ```

### Issue: Slow Processing

**Problem**: Processing is too slow.

**Solution**:
1. Use Ray Data execution mode
2. Increase `shard_docs` for fewer writes
3. Process in batches with checkpoints

## Advanced: Custom Field Mapping

If your dataset has different field names:

```yaml
sources:
  - name: "pg19"
    type: "streaming"
    kind: "hf_stream"
    dataset: "datasets/pg19"
    split: "train"
    text_field: "book_text"  # Custom field name
    license_field: "copyright_status"  # Custom field name
    url_field: "gutenberg_url"  # Custom field name
```

## Best Practices

1. **Download First**: For large datasets, download locally first
2. **Verify Schema**: Always verify dataset schema before processing
3. **Use Checkpoints**: Set `checkpoint_every_docs` for resumability
4. **Monitor Progress**: Use the dashboard to track processing
5. **Test Small**: Test with a small split first (e.g., `split="train[:1000]"`)
6. **Check Rejections**: Review `rejections.jsonl` to understand filtering

## Example: Processing Multiple HuggingFace Datasets

```yaml
sources:
  - name: "pg19"
    type: "streaming"
    kind: "hf_stream"
    dataset: "datasets/pg19"
    split: "train"
    text_field: "text"
  
  - name: "wikipedia"
    type: "streaming"
    kind: "hf_stream"
    dataset: "wikipedia"
    split: "20220301.en"
    text_field: "text"
  
  - name: "common_crawl"
    type: "streaming"
    kind: "hf_stream"
    dataset: "allenai/c4"
    split: "en"
    text_field: "text"
```

Each source processes independently with its own checkpoint.

## See Also

- `examples/build_gutenberg.yaml` - Gutenberg dataset example
- `examples/build_wikipedia.yaml` - Wikipedia dataset example
- `examples/build_common_pile.yaml` - Common Pile dataset example
- `scripts/verify_hf_dataset.py` - Dataset verification tool
