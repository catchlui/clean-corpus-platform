# How to Add and Process Files

This guide explains how to add files to the Clean Corpus Platform and process them through the pipeline.

## Quick Start

### Step 1: Prepare Your Files

**For JSONL files:**
- Each line should be a JSON object with at least a `text` field
- Optional fields: `license`, `url`, `metadata`, etc.

Example `data/my_file.jsonl`:
```jsonl
{"text": "This is sample text.", "license": "CC-BY", "url": "https://example.com"}
{"text": "Another document.", "license": "MIT", "url": "https://example.com/doc2"}
```

**For PDF files:**
- Place PDF files in a directory
- The platform will extract text automatically

### Step 2: Create a Configuration File

Create a YAML configuration file (e.g., `my_config.yaml`):

```yaml
run:
  run_id: "MyRun_2026-01-29"
  out_dir: "output"
  shard_docs: 1000
  log_every_docs: 100
  checkpoint_every_docs: 1000
  policy_version: "policy_v1"

execution:
  mode: local

sources:
  - name: "my_source"
    type: "batch"
    kind: "local_jsonl"
    dataset: "data/my_file.jsonl"  # Your file path
    text_field: "text"
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

### Step 3: Run the Pipeline

```bash
python -m clean_corpus.cli build --config my_config.yaml
```

Or use the helper script:
```bash
python scripts/run_pipeline.py my_config.yaml
```

## Adding Files - Different Methods

### Method 1: Single File

```yaml
sources:
  - name: "single_file"
    kind: "local_jsonl"
    dataset: "data/my_file.jsonl"
    text_field: "text"
    license_field: "license"
```

### Method 2: Multiple Files (List)

Process multiple files together as one source:

```yaml
sources:
  - name: "multiple_files"
    kind: "local_jsonl"
    dataset:
      - "data/file1.jsonl"
      - "data/file2.jsonl"
      - "data/file3.jsonl"
    text_field: "text"
    license_field: "license"
```

**Benefits:**
- All files processed together
- Per-file statistics tracked
- Single source in output

### Method 3: Directory

Process all `.jsonl` files in a directory:

```yaml
sources:
  - name: "directory_source"
    kind: "local_jsonl"
    dataset: "data/"  # Processes all .jsonl files in data/
    text_field: "text"
    license_field: "license"
```

### Method 4: Glob Pattern

Process files matching a pattern:

```yaml
sources:
  - name: "pattern_source"
    kind: "local_jsonl"
    dataset: "data/sample_*.jsonl"  # Matches sample_*.jsonl
    text_field: "text"
    license_field: "license"
```

### Method 5: Multiple Sources

Process different file sets separately:

```yaml
sources:
  - name: "source1"
    kind: "local_jsonl"
    dataset: "data/file1.jsonl"
    text_field: "text"
  
  - name: "source2"
    kind: "local_jsonl"
    dataset: "data/file2.jsonl"
    text_field: "text"
  
  - name: "source3"
    kind: "local_jsonl"
    dataset:
      - "data/file3.jsonl"
      - "data/file4.jsonl"
    text_field: "text"
```

## Processing PDF Files

### Basic PDF Processing

```yaml
pdf:
  chunk_mode: "page"  # page | document | fixed_size
  extractor: "pymupdf"  # pymupdf | pdfplumber | pypdf2
  min_text_length: 200

sources:
  - name: "pdf_source"
    kind: "pdf"
    dataset: "data/pdfs/"  # Directory of PDF files
```

### PDF Chunking Strategies

**1. Page-by-page (recommended for long documents):**
```yaml
sources:
  - name: "papers"
    kind: "pdf"
    dataset: "data/papers/"
    chunk_mode: "page"  # Each page becomes a document
```

**2. Entire document:**
```yaml
sources:
  - name: "reports"
    kind: "pdf"
    dataset: "data/reports/"
    chunk_mode: "document"  # Entire PDF = one document
```

**3. Fixed-size chunks:**
```yaml
sources:
  - name: "long_docs"
    kind: "pdf"
    dataset: "data/long/"
    chunk_mode: "fixed_size"
    chunk_size: 1000      # Characters per chunk
    chunk_overlap: 200    # Overlap between chunks
```

## Complete Examples

### Example 1: Process Multiple JSONL Files

```yaml
run:
  run_id: "MultiFile_2026-01-29"
  out_dir: "output"
  shard_docs: 1000
  log_every_docs: 100
  checkpoint_every_docs: 1000
  policy_version: "policy_v1"

execution:
  mode: local

sources:
  - name: "all_data"
    kind: "local_jsonl"
    dataset:
      - "data/file1.jsonl"
      - "data/file2.jsonl"
      - "data/file3.jsonl"
    text_field: "text"
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

### Example 2: Process PDFs with Custom Chunking

```yaml
run:
  run_id: "PDF_2026-01-29"
  out_dir: "output_pdf"
  shard_docs: 1000
  log_every_docs: 100
  checkpoint_every_docs: 1000
  policy_version: "policy_v1"

execution:
  mode: local

pdf:
  chunk_mode: "page"
  extractor: "pymupdf"
  min_text_length: 200
  chunk_size: 1000
  chunk_overlap: 200

sources:
  - name: "research_papers"
    kind: "pdf"
    dataset: "data/papers/"
    chunk_mode: "page"

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

## Monitoring Progress

### Real-time Monitoring

In another terminal, run:
```bash
python -m clean_corpus.cli monitor output --unified
```

Press:
- `'m'` for Monitor screen (real-time progress)
- `'a'` for Analytics screen (detailed statistics)
- `'q'` to quit

### View Results

After processing, check:
- **Documents**: `output/docs/` - Processed documents in Parquet format
- **Metadata**: `output/metadata/` - Document metadata
- **Rejections**: `output/rejections/rejections.jsonl` - Rejected documents with reasons
- **Analytics**: `output/analytics/` - Stage-by-stage analytics
- **Checkpoint**: `output/checkpoints/` - Resume checkpoints

## Field Mapping

### JSONL Field Mapping

Map your JSONL fields to platform fields:

```yaml
sources:
  - name: "my_source"
    kind: "local_jsonl"
    dataset: "data/file.jsonl"
    text_field: "content"        # Your text field name
    license_field: "license"     # Your license field name
    url_field: "source_url"      # Your URL field name
    id_field: "doc_id"           # Optional: custom ID field
```

### PDF Metadata Mapping

```yaml
pdf:
  schema:
    metadata_mapping:
      document_title: "title"      # Map PDF title to "title"
      document_author: "author"    # Map PDF author to "author"
      page_number: "page"           # Map page number to "page"
```

## Troubleshooting

### File Not Found

**Error**: `FileNotFoundError: [Errno 2] No such file or directory`

**Solution**: Use absolute paths or paths relative to where you run the command:
```yaml
dataset: "C:/full/path/to/file.jsonl"  # Absolute path
# OR
dataset: "data/file.jsonl"  # Relative to current directory
```

### No Files Processed

**Check**:
1. File paths are correct
2. Files exist and are readable
3. File format matches source type (`.jsonl` for `local_jsonl`, `.pdf` for `pdf`)

### Empty Output

**Possible causes**:
1. All documents rejected by stages (check `rejections.jsonl`)
2. Files are empty
3. Field names don't match (`text_field`, `license_field`, etc.)

## Best Practices

1. **Start Small**: Test with a few files first
2. **Use Checkpoints**: Set `checkpoint_every_docs` to resume if interrupted
3. **Monitor Progress**: Use the dashboard to track processing
4. **Check Rejections**: Review `rejections.jsonl` to understand why documents were rejected
5. **Use Multi-file Sources**: Group related files together for better statistics
6. **Absolute Paths**: Use absolute paths for reliability

## Next Steps

- See `examples/` directory for more configuration examples
- Check `README.md` for advanced features
- Review `docs/` for detailed documentation
