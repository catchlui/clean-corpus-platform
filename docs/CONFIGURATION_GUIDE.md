# Configuration Guide

This guide explains how to configure the Clean Corpus Platform for different data source types.

## Quick Start

1. **Copy the standard template:**
   ```bash
   cp configs/standard_template.yaml my_config.yaml
   ```

2. **Edit `my_config.yaml`** and configure your data sources

3. **Run the pipeline:**
   ```bash
   python scripts/run_pipeline.py my_config.yaml
   ```

## Data Source Types

### 1. Local JSONL Files

Process JSONL files from your local filesystem.

**Single File:**
```yaml
sources:
  - name: "my_data"
    type: "batch"
    kind: "local_jsonl"
    dataset: "data/myfile.jsonl"
    text_field: "text"
    license_field: "license"
    url_field: "url"
```

**Multiple Files:**
```yaml
sources:
  - name: "my_data"
    type: "batch"
    kind: "local_jsonl"
    dataset:
      - "data/file1.jsonl"
      - "data/file2.jsonl"
    text_field: "text"
    license_field: "license"
    url_field: "url"
```

**Directory (all .jsonl files):**
```yaml
sources:
  - name: "my_data"
    type: "batch"
    kind: "local_jsonl"
    dataset: "data/jsonl_files/"  # Processes all .jsonl files recursively
    text_field: "text"
    license_field: "license"
    url_field: "url"
```

**Glob Pattern:**
```yaml
sources:
  - name: "my_data"
    type: "batch"
    kind: "local_jsonl"
    dataset: "data/*.jsonl"  # Matches all .jsonl files
    text_field: "text"
    license_field: "license"
    url_field: "url"
```

### 2. PDF Files and Folders

Process PDF files from your local filesystem.

**Single PDF:**
```yaml
sources:
  - name: "my_pdf"
    type: "batch"
    kind: "pdf"
    dataset: "data/document.pdf"
    chunk_mode: "document"  # Entire PDF as one document
```

**PDF Folder:**
```yaml
sources:
  - name: "my_pdfs"
    type: "batch"
    kind: "pdf"
    dataset: "data/papers/"  # Directory containing PDFs
    chunk_mode: "page"       # Each page becomes a document
    
    # Folder-level metadata (applied to all PDFs in folder)
    metadata:
      book_name: "Research Papers Collection"
      author: "Various Authors"
      certificate_type: "Academic"
      category: "Research"
      license: "CC-BY"
```

**Note:** Folder-level metadata overrides PDF metadata if there's a conflict. This is useful when processing a folder of PDFs that all share common metadata (e.g., same book, author, certificate type).

**Chunking Modes:**
- `page` - Each page becomes a separate document (default)
- `document` - Entire PDF becomes one document
- `fixed_size` - Split into fixed-size chunks with overlap

**Example with Fixed-Size Chunking:**
```yaml
sources:
  - name: "long_docs"
    type: "batch"
    kind: "pdf"
    dataset: "data/long_docs/"
    chunk_mode: "fixed_size"
    chunk_size: 2000      # Characters per chunk
    chunk_overlap: 400   # Overlap between chunks
```

**Folder-Level Metadata:**
When processing a folder of PDFs, you can specify metadata that applies to all PDFs in that folder:

```yaml
sources:
  # Example: Textbook chapters (all from same book)
  - name: "textbook_chapters"
    type: "batch"
    kind: "pdf"
    dataset: "data/textbook_chapters/"
    chunk_mode: "document"
    
    # Metadata applied to all PDFs in folder
    metadata:
      book_name: "Mathematics Grade 10"
      author: "John Smith"
      publisher: "Education Press"
      isbn: "978-1234567890"
      license: "CC-BY-NC"
      subject: "Mathematics"
      grade_level: "10"
  
  # Example: Certificates (all same type)
  - name: "certificates"
    type: "batch"
    kind: "pdf"
    dataset: "data/certificates/"
    chunk_mode: "document"
    
    metadata:
      certificate_type: "Academic Achievement"
      issuing_authority: "University Name"
      category: "Education"
      license: "CC-BY"
  
  # Example: Research papers (same collection)
  - name: "research_collection"
    type: "batch"
    kind: "pdf"
    dataset: "data/research/"
    chunk_mode: "page"
    
    metadata:
      book_name: "AI Research Papers 2024"
      author: "Various Authors"
      category: "Research"
      field: "Artificial Intelligence"
      license: "CC-BY"
```

**Note:** Folder-level metadata:
- Overrides PDF metadata if there's a conflict
- Is applied to all PDFs processed from that folder
- Can include any custom fields (book_name, author, certificate_type, etc.)
- Is stored in the document's `extra` field and can be used in metadata mappings

### 3. Web PDFs (Download from URLs)

Download PDFs from websites and process them.

**Specific URLs:**
```yaml
sources:
  - name: "web_pdfs"
    type: "batch"
    kind: "web_pdf"
    urls:
      - "https://example.com/doc1.pdf"
      - "https://example.com/doc2.pdf"
    download_dir: "downloads/web_pdfs"
    language: "en"                # ISO 639-1 code
    auto_detect_language: true
    timeout: 30
    max_retries: 3
    metadata:
      source: "Example"
      license: "CC-BY"
```

**URL Pattern (Scrape):**
```yaml
sources:
  - name: "scraped_pdfs"
    type: "batch"
    kind: "web_pdf"
    url_pattern: "https://example.com/pdf/*.pdf"
    base_url: "https://example.com"
    download_dir: "downloads/scraped"
    auto_detect_language: true
    resume_download: true  # Skip already downloaded files
```

### 4. HuggingFace Streaming Datasets

Stream data from HuggingFace Hub.

```yaml
sources:
  - name: "hf_dataset"
    type: "streaming"
    kind: "hf_stream"
    dataset: "common-pile/comma_v0.1_training_dataset"
    split: "train"
    text_field: "text"
    license_field: "license"
    url_field: "url"
```

**Note:** The pipeline can automatically download missing datasets if you use `--auto-download` flag.

### 5. Multiple Sources (Mixed Types)

You can combine different source types in one configuration:

```yaml
sources:
  # Local JSONL files
  - name: "local_data"
    type: "batch"
    kind: "local_jsonl"
    dataset: "data/local.jsonl"
    text_field: "text"
    license_field: "license"
    url_field: "url"
  
  # PDF folder
  - name: "pdf_documents"
    type: "batch"
    kind: "pdf"
    dataset: "data/pdfs/"
    chunk_mode: "document"
  
  # HuggingFace stream
  - name: "hf_data"
    type: "streaming"
    kind: "hf_stream"
    dataset: "dataset/name"
    split: "train"
    text_field: "text"
```

Each source processes independently with its own checkpoint.

## Configuration Examples

See `configs/standard_template.yaml` for a complete template with all options.

See `examples/` directory for specific use cases:
- `build_local_jsonl.yaml` - Local JSONL processing
- `build_pdf.yaml` - PDF processing
- `build_web_pdf_ncert.yaml` - Web PDF downloader
- `build_multi_source.yaml` - Multiple sources
- `build_common_pile.yaml` - HuggingFace streaming
- `build_unified.yaml` - Unified configuration system

## Advanced Configuration

### Global Processing Functions

Configure processing once, apply to all sources:

```yaml
global:
  processing:
    unicode_normalize: true
    deduplication:
      enabled: true
      method: "exact"  # exact | minhash | duplodocus
    domain_tagging:
      enabled: false
```

### Entry-Level Overrides

Override global settings per source:

```yaml
sources:
  - name: "my_source"
    kind: "local_jsonl"
    dataset: "data.jsonl"
    processing:
      deduplication:
        method: "minhash"  # Override global setting
```

### Enhanced Checkpoint System

Control how to resume processing:

```yaml
checkpoint:
  resume_mode: "auto"  # auto | beginning | checkpoint | ignore
  checkpoint_id: null  # Specific checkpoint ID (if resume_mode="checkpoint")
```

### S3 Storage

Store outputs in S3:

```yaml
storage:
  type: s3
  bucket: my-bucket
  prefix: clean-corpus/runs
  region: us-east-1
```

## Output Formats

The platform supports multiple output formats for storing processed documents:

### Available Formats

1. **Parquet** (Default) - Apache Parquet columnar storage
   - Best for: Large datasets, efficient storage, analytics
   - File extension: `.parquet`
   - Storage efficiency: ⭐⭐⭐⭐⭐

2. **JSONL** - JSON Lines format (one JSON object per line)
   - Best for: Human-readable output, streaming, debugging
   - File extension: `.jsonl`
   - Storage efficiency: ⭐⭐⭐

3. **DOLMA/DOML** - DOLMA format (AI2 corpus format)
   - Best for: Language model training, AI2 pipelines
   - File extension: `.jsonl` (with DOLMA structure)
   - Storage efficiency: ⭐⭐⭐

### Configuration Examples

**Parquet (Recommended for large datasets):**
```yaml
output:
  corpus_format: "parquet"
  metadata_format: "parquet_v1"
```

**JSONL (Good for debugging and streaming):**
```yaml
output:
  corpus_format: "jsonl"
  metadata_format: "parquet_v1"
```

**DOLMA/DOML (For language model training):**
```yaml
output:
  corpus_format: "dolma"  # or "doml"
  metadata_format: "parquet_v1"
  format_options:
    dolma:
      include_all_metadata: true
      custom_metadata_fields:
        dataset_version: "v1.0"
```

See `docs/OUTPUT_FORMATS.md` for detailed format documentation and how to add custom formats.
See `docs/OUTPUT_STRUCTURE.md` for complete output structure documentation.

## See Also

- `docs/UNIFIED_CONFIGURATION.md` - Unified configuration system documentation
- `docs/OUTPUT_FORMATS.md` - Output formats guide
- `configs/standard_template.yaml` - Complete configuration template
- `examples/` - Example configurations for different use cases
