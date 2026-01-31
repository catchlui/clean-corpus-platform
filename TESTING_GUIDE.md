# Testing Guide

## Quick Test Commands

### 1. Reset Checkpoint (if needed)

If you see "No documents were yielded" or pipeline seems stuck:

```bash
python scripts/reset_checkpoint.py storage_example ExampleJSONL_2026-01-28
```

### 2. Run Simple Test

```bash
# Minimal test (avoids scipy/torch dependencies)
python -m clean_corpus.cli build --config examples/build_test_minimal.yaml
```

### 3. Run Full Test

```bash
# Full pipeline with all stages
python -m clean_corpus.cli build --config examples/build_local_jsonl.yaml
```

### 4. Check Output

```bash
# View logs
Get-Content storage_test\logs\*.log -Tail 50

# Check output files
dir storage_test\docs
dir storage_test\metadata
dir storage_test\rejections

# View manifest
Get-Content storage_test\manifests\*.json | ConvertFrom-Json | ConvertTo-Json -Depth 5
```

### 5. Monitor Progress

```bash
# In another terminal
python -m clean_corpus.cli monitor storage_test --unified
```

## Troubleshooting

### Issue: No Output Files

**Symptoms:**
- Pipeline runs but no files in `docs/` or `metadata/`
- Logs show "No documents were yielded"

**Solutions:**

1. **Reset checkpoint:**
   ```bash
   python scripts/reset_checkpoint.py <out_dir> [run_id]
   ```

2. **Check if files exist:**
   ```bash
   dir examples\sample_*.jsonl
   ```

3. **Verify config:**
   ```bash
   python scripts/test_pipeline_simple.py
   ```

### Issue: Dependency Errors

**Symptoms:**
- `ImportError: cannot import name '_promote' from 'scipy'`
- `OSError: Error loading torch DLL`

**Solutions:**

1. **Use minimal config** (avoids problematic stages):
   ```bash
   python -m clean_corpus.cli build --config examples/build_test_minimal.yaml
   ```

2. **Fix dependencies:**
   ```bash
   pip install --upgrade scipy
   # or
   pip install --force-reinstall scipy torch
   ```

3. **Use clean environment:**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   pip install -e .
   ```

### Issue: No Logs Visible

**Check log location:**
```bash
dir <out_dir>\logs
Get-Content <out_dir>\logs\*.log -Tail 50
```

Logs are written to: `<out_dir>/logs/<run_id>.log`

### Issue: Checkpoint Resuming Issues

**Reset checkpoint:**
```bash
python scripts/reset_checkpoint.py storage_example
```

**Or delete checkpoint manually:**
```bash
del storage_example\checkpoints\*.json
```

## Expected Output Structure

After successful run:

```
storage_test/
├── docs/
│   └── source=<source_name>/
│       └── shard_*.parquet
├── metadata/
│   └── source=<source_name>/
│       └── shard_*.parquet
├── rejections/
│   └── rejections.jsonl
├── analytics/
│   ├── events/
│   └── aggregates/
├── checkpoints/
│   └── <run_id>.json
├── manifests/
│   └── <run_id>.json
└── logs/
    └── <run_id>.log
```

## Verification Commands

```bash
# Count output files
(dir storage_test\docs -Recurse -File).Count

# Check file sizes
dir storage_test\docs -Recurse -File | Select-Object Name, Length

# View rejection count
(Get-Content storage_test\rejections\rejections.jsonl | Measure-Object -Line).Lines

# Check manifest stats
Get-Content storage_test\manifests\*.json | ConvertFrom-Json | Select-Object run_id, total_written_docs, total_rejected_docs
```
