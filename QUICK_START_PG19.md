# Quick Start: Processing PG19 Dataset

This guide shows you how to process the PG19 dataset downloaded via HuggingFace CLI.

## Step-by-Step Guide

### 1. Download PG19 Dataset (Optional - Can Be Automated)

**Option A: Manual Download (Traditional)**
```bash
# Download pg19 dataset manually
huggingface-cli download datasets/pg19 --repo-type dataset --local-dir pg19_raw
```

**Option B: Automatic Download (Recommended)**
```bash
# Use Python script to download
python scripts/download_hf_dataset.py datasets/pg19 --repo-type dataset

# OR let the pipeline download automatically (see Step 4)
```

### 2. Verify Dataset Schema

```bash
python scripts/verify_hf_dataset.py datasets/pg19 train
```

This will show you:
- Available fields in the dataset
- Sample data
- Suggested configuration

### 3. Choose Processing Method

#### Option A: Direct Processing (Recommended)

If the dataset is in HuggingFace cache, use `hf_stream`:

```yaml
# build_pg19.yaml
run:
  run_id: "PG19_2026-01-29"
  out_dir: "storage_pg19"
  shard_docs: 1000
  checkpoint_every_docs: 1000

execution:
  mode: local

sources:
  - name: "pg19"
    type: "streaming"
    kind: "hf_stream"
    dataset: "datasets/pg19"
    split: "train"
    text_field: "text"  # Adjust based on actual schema
    license_field: "license"
    url_field: "url"

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

#### Option B: Convert to JSONL First (More Reliable)

If you downloaded to a custom directory, convert to JSONL:

```bash
# Convert to JSONL
python scripts/convert_hf_to_jsonl.py datasets/pg19 train pg19_train.jsonl --local-dir pg19_raw
```

Then use `local_jsonl`:

```yaml
sources:
  - name: "pg19"
    kind: "local_jsonl"
    dataset: "pg19_train.jsonl"
    text_field: "text"
    license_field: "license"
    url_field: "url"
```

### 4. Run Pipeline

**With Automatic Download (Recommended):**
```bash
# Pipeline will automatically download missing datasets
python scripts/run_pipeline.py build_pg19.yaml --auto-download --monitor
```

**Without Auto-Download:**
```bash
# Run with monitoring (assumes dataset already downloaded)
python scripts/run_pipeline.py build_pg19.yaml --monitor

# Or directly
python -m clean_corpus.cli build --config build_pg19.yaml
```

**Note**: The `--auto-download` flag will automatically download any missing HuggingFace datasets before processing.

### 5. Monitor Progress

In another terminal:

```bash
python -m clean_corpus.cli monitor storage_pg19 --unified
```

Press `'m'` for Monitor, `'a'` for Analytics.

## Troubleshooting

### Dataset Not Found

**Problem**: Platform can't find locally downloaded dataset.

**Solution**: 
1. Check if dataset is in HuggingFace cache:
   ```python
   from datasets import config
   print(config.HF_DATASETS_CACHE)
   ```
2. If downloaded to custom directory, convert to JSONL (Option B above)
3. Or set `HF_HOME` environment variable to your cache location

### Wrong Field Names

**Problem**: `text_field` doesn't match dataset schema.

**Solution**: Run verification script first:
```bash
python scripts/verify_hf_dataset.py datasets/pg19 train
```

Then update field names in config based on output.

## Example Configuration

See `examples/build_pg19_local.yaml` for a complete example.

## Next Steps

- Check `docs/HUGGINGFACE_INTEGRATION.md` for detailed documentation
- Review `examples/` for more dataset examples
- Use the analytics dashboard to review processing results
