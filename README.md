# Clean Corpus Platform

A **production-grade, scalable data preprocessing and governance platform** for large-scale language model training.

## Features

- ✅ **Policy-driven processing** - YAML-based policies for licenses, quality, PII, curriculum
- ✅ **Modular pipeline stages** - License gate, sanitization, deduplication, quality filtering, PII detection
- ✅ **Checkpoint/resume** - Resume long-running jobs from checkpoints
- ✅ **Per-stage analytics** - Detailed analytics for each pipeline stage
- ✅ **Multiple execution modes** - Local, Ray, Ray Data
- ✅ **Extensible** - Add sources, stages, detectors, writers without code changes
- ✅ **Storage flexibility** - Local filesystem or S3 storage
- ✅ **Real-time monitoring** - Terminal dashboard for live monitoring
- ✅ **Multi-file processing** - Process multiple files together with per-file statistics
- ✅ **PDF processing** - Configurable chunking strategies and schema transformations
- ✅ **Web PDF downloader** - Download PDFs from websites (like NCERT) with automatic language detection
- ✅ **Multi-language support** - Automatic language detection and metadata for different languages
- ✅ **Per-file tracking** - Track statistics and analytics per source file

## Installation

### Prerequisites

- Python 3.8+
- pip or uv

### Install Package

```bash
# Using pip
pip install -e .

# Using uv (recommended)
uv pip install -e .

# With optional dependencies
pip install -e ".[s3]"      # S3 storage support
pip install -e ".[ray]"     # Ray execution support
pip install -e ".[pdf]"     # PDF processing support
pip install -e ".[web_pdf]" # Web PDF downloader (requests, beautifulsoup4, langdetect)
pip install -e ".[web_pdf_playwright]" # Playwright for JavaScript-heavy sites (better than Selenium)
pip install -e ".[all]"     # All optional dependencies
```

### Windows Installation Issues

If you see "The system cannot find the file specified: [executable].exe":

```powershell
# Close Python processes
taskkill /F /IM python.exe /T

# Install with ignore flag
pip install -e . --ignore-installed datasets numpy

# Or use virtual environment
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```

### CLI Access

If `clean-corpus` command is not found, use Python module:

```bash
# Instead of: clean-corpus build --config ...
python -m clean_corpus.cli build --config examples/build_local_jsonl.yaml

# Instead of: clean-corpus monitor ...
python -m clean_corpus.cli monitor storage_example
```

## Quick Start

### 1. Bootstrap PII Detectors

```bash
python scripts/bootstrap_pii.py
```

### 2. Run Pipeline

**Using all-in-one script (recommended):**

```bash
# Basic usage
python scripts/run_pipeline.py examples/build_local_jsonl.yaml

# With real-time monitoring
python scripts/run_pipeline.py examples/build_local_jsonl.yaml --monitor
```

**Using CLI directly:**

```bash
python -m clean_corpus.cli build --config examples/build_local_jsonl.yaml
```

### 3. Monitor Progress

**Unified Monitor + Analytics (recommended):**
```bash
# In another terminal - unified app with two screens
python -m clean_corpus.cli monitor storage_example --unified
# Press 'm' for Monitor screen, 'a' for Analytics screen, 'q' to quit
```

**Legacy Dashboard:**
```bash
# In another terminal
python -m clean_corpus.cli monitor storage_example
```

### 4. View Results

```bash
# View analytics
python scripts/view_analytics.py storage_example

# View checkpoint report
python scripts/checkpoint_report.py storage_example

# View run info
python scripts/show_run_info.py storage_example
```

## Configuration

### Basic Configuration

```yaml
run:
  run_id: "Example_2026-01-28"
  out_dir: "storage_example"
  shard_docs: 5000
  checkpoint_every_docs: 5000
  policy_version: "policy_v1"

execution:
  mode: local  # local | ray | ray_data

sources:
  - name: "internal_jsonl"
    type: "batch"
    kind: "local_jsonl"
    dataset: "examples/sample_internal.jsonl"
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

output:
  corpus_format: parquet
  metadata_format: parquet_v1
```

### Multi-File Processing

Process multiple files together in a single source:

```yaml
sources:
  # Option 1: List of files
  - name: "all_samples"
    type: "batch"
    kind: "local_jsonl"
    dataset:
      - "examples/sample_internal.jsonl"
      - "examples/sample_additional.jsonl"
      - "examples/sample_tech.jsonl"
    text_field: "text"
    license_field: "license"
    url_field: "url"
  
  # Option 2: Directory (processes all .jsonl files)
  - name: "directory_source"
    type: "batch"
    kind: "local_jsonl"
    dataset: "examples/"  # Processes all .jsonl files recursively
    text_field: "text"
    license_field: "license"
    url_field: "url"
  
  # Option 3: Glob pattern
  - name: "pattern_source"
    type: "batch"
    kind: "local_jsonl"
    dataset: "examples/sample_*.jsonl"  # Matches all matching files
    text_field: "text"
    license_field: "license"
    url_field: "url"
```

**Benefits:**
- All files processed together as one source
- Per-file statistics tracked automatically
- Dashboard shows breakdown by file
- Each document includes `source_file` field in output

### PDF Processing

Process PDF files with configurable chunking and schema transformations:

```yaml
# Global PDF configuration (applies to all PDF sources)
pdf:
  chunk_mode: "page"  # page | document | fixed_size
  extractor: "pymupdf"  # pymupdf | pdfplumber | pypdf2
  min_text_length: 200
  metadata_fields: ["title", "author", "page_number"]
  
  # Fixed-size chunking (when chunk_mode="fixed_size")
  chunk_size: 1000  # Characters per chunk
  chunk_overlap: 200  # Overlap between chunks
  
  # Global schema configuration
  schema:
    default_license: "CC-BY"
    text_prefix: ""
    text_suffix: ""
    metadata_mapping:
      document_title: "title"
      document_author: "author"

sources:
  # Use global PDF config
  - name: "research_papers"
    type: "batch"
    kind: "pdf"
    dataset: "data/papers/"  # Directory of PDFs
  
  # Override chunking strategy
  - name: "reports"
    type: "batch"
    kind: "pdf"
    dataset: "data/reports/"
    chunk_mode: "document"  # Override: entire PDF as one document
    min_text_length: 500
  
  # Fixed-size chunking
  - name: "long_documents"
    type: "batch"
    kind: "pdf"
    dataset: "data/long_docs/"
    chunk_mode: "fixed_size"
    chunk_size: 2000
    chunk_overlap: 400
  
  # Directory-specific schema override
  - name: "academic_papers"
    type: "batch"
    kind: "pdf"
    dataset: "data/academic/"
    schema:
      directory_pattern: ".*academic.*"
      default_license: "CC-BY-SA"
      text_prefix: "[ACADEMIC PAPER]\n"
      metadata_mapping:
        paper_title: "title"
        paper_authors: "author"
```

**Chunking Modes:**
- `page` - Each page becomes a separate document (default)
- `document` - Entire PDF becomes one document
- `fixed_size` - Split into fixed-size chunks with overlap

**Schema Features:**
- Global schema applies to all PDFs
- Directory-specific schemas override global settings
- Supports text prefixes/suffixes
- Metadata field mapping
- Default license assignment

### Storage Configuration

**Local Storage (Default):**
```yaml
# Omit storage section or use:
storage:
  type: local
```

**S3 Storage:**
```yaml
storage:
  type: s3
  bucket: my-bucket
  prefix: clean-corpus/runs
  region: us-east-1

run:
  out_dir: "s3://my-bucket/clean-corpus/runs"
```

**Per-Output Storage:**
```yaml
storage:
  type: local  # Default

output_storage:
  docs:
    type: s3
    bucket: corpus-bucket
  metadata:
    type: s3
    bucket: metadata-bucket
  analytics:
    type: local  # Keep local for faster access
```

## Pipeline Stages

| Stage | Purpose | Layer |
|-------|---------|------|
| `license_gate` | Filter by license policy | governance |
| `sanitize` | Text normalization | preprocessing |
| `exact_dedup` | Exact duplicate detection (SHA256) | preprocessing |
| `near_dup_minhash` | Near-duplicate detection (MinHash LSH) | dedup |
| `semantic_simhash` | Semantic similarity hashing | dedup |
| `quality_gate` | Length + entropy filtering | quality |
| `pii_policy_gate` | PII detection + policy enforcement | governance |
| `tokenize` | Tokenization (via adapter) | preprocessing |
| `curriculum_eligibility` | Token window eligibility tagging | curriculum |

## Execution Modes

### Local Mode

```yaml
execution:
  mode: local
```

- Single-threaded processing
- Best for development and small datasets
- Full checkpoint/resume support

### Ray Mode

```yaml
execution:
  mode: ray
```

- Distributed processing with Ray
- Requires Ray cluster

### Ray Data Mode

```yaml
execution:
  mode: ray_data
```

- Ray Data pipeline for large-scale processing
- Best for production workloads
- Requires Ray cluster

## Monitoring Dashboard

### Launch Dashboard

```bash
# Monitor default directory
python -m clean_corpus.cli monitor

# Monitor specific directory
python -m clean_corpus.cli monitor storage_example

# Custom refresh rate
python -m clean_corpus.cli monitor storage_example --refresh 2.0
```

### Dashboard Features

- ✅ Live processing statistics (written/rejected counts)
- 📊 Stage-by-stage breakdown
- 📈 Rejection rates
- 📂 Source information with per-file statistics
- 📝 Recent log activity
- ⏱️ Elapsed time and completion estimates
- 📁 Per-file breakdown showing processed/written/rejected per file

**Important:** Dashboard is **read-only** and safe to start/stop anytime without affecting pipeline.

## Extensibility

### Adding New Sources

**Step 1:** Implement source class

```python
# src/clean_corpus/sources/my_source.py
from .base import DataSource, RawDocument, SourceSpec
from typing import Iterable

class MyCustomSource(DataSource):
    def __init__(self, spec: SourceSpec):
        self.spec = spec
        self.name = spec.name
    
    def stream(self) -> Iterable[RawDocument]:
        for item in your_data_iterator():
            yield RawDocument(
                raw_id=generate_id(item),
                text=item['text'],
                source=self.spec.name,
                url=item.get('url'),
                license=item.get('license')
            )
```

**Step 2:** Register in registry

```python
# src/clean_corpus/sources/registry.py
from .my_source import MyCustomSource

def make_source(spec: SourceSpec):
    if spec.kind == "my_custom":
        return MyCustomSource(spec)
    # ... existing sources
```

**Step 3:** Use in config

```yaml
sources:
  - name: "my_data"
    kind: "my_custom"
    # ... your config
```

### Adding New Metadata Fields

**Step 1:** Create new metadata writer

```python
# src/clean_corpus/writers/meta_parquet_v2.py
class ParquetMetadataWriterV2(MetadataWriter):
    schema_version = "meta_v2"  # NEW version
    
    def _schema_arrow(self) -> pa.Schema:
        return pa.schema([
            # All fields from V1
            ("doc_id", pa.binary(32)),
            # ... existing fields ...
            
            # NEW fields
            ("domain", pa.string()),      # NEW
            ("word_count", pa.int32()),   # NEW
        ])
```

**Step 2:** Register writer

```python
# src/clean_corpus/writers/registry.py
register_metadata_writer("parquet_v2", ParquetMetadataWriterV2())
```

**Step 3:** Use in config

```yaml
output:
  metadata_format: parquet_v2
```

**Note:** Old runs continue using old schema - no conflicts!

### Adding PII Detectors

**Step 1:** Implement detector

```python
# src/clean_corpus/pii/detectors/custom.py
class CustomDetector(PIIDetector):
    name = "custom"
    
    def detect(self, text: str) -> List[PIISignal]:
        # Your detection logic
        return signals
```

**Step 2:** Register detector

```python
from clean_corpus.pii.registry import register_detector
register_detector(CustomDetector())
```

**Step 3:** Update policy

```yaml
# policies/defaults/pii.yaml
drop_kinds: ["custom"]  # Or redact_kinds
```

## Storage

### Local Storage

Default storage - no configuration needed:

```yaml
# Omit storage section
run:
  out_dir: "storage"
```

### S3 Storage

**Install S3 support:**
```bash
pip install boto3
# or
pip install -e ".[s3]"
```

**Configure:**
```yaml
storage:
  type: s3
  bucket: my-bucket
  prefix: clean-corpus/runs
  region: us-east-1
```

**AWS Credentials:**
- Use AWS credentials file: `~/.aws/credentials`
- Or environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- Or IAM roles (recommended for production)

**S3-Compatible Services:**
```yaml
storage:
  type: s3
  bucket: my-bucket
  endpoint_url: https://s3-compatible.example.com
  region: us-east-1
```

## Checkpoint Reports

### Automatic Reports

After each run, a summary report is automatically generated:

```
{out_dir}/reports/{run_id}_summary.txt
```

### Detailed Reports

```bash
python scripts/checkpoint_report.py storage_example [run_id] [--config config.yaml]
```

Reports include:
- Source metadata (file names, sizes, configuration)
- Processed document counts
- **Per-file statistics** (processed/written/rejected per file)
- Shard information
- Resume/rerun instructions

## Troubleshooting

### No Documents Extracted

**Common causes:**
1. Quality gate too strict - documents too short
2. License gate - invalid licenses
3. PII policy - documents dropped
4. Multi-file source - check if files exist and are readable

**Check rejections:**
```bash
python scripts/view_analytics.py storage_example
cat storage_example/rejections/rejections.jsonl
```

**Check per-file statistics:**
```bash
# View checkpoint which includes per-file stats
python scripts/checkpoint_report.py storage_example

# Or check logs for per-file breakdown
# Logs show: "Source X per-file statistics:"
```

### Dashboard Not Showing

**Check:**
1. Pipeline has run successfully
2. Output directory exists: `storage_example/manifests/`
3. Manifest file exists

**Run pipeline first:**
```bash
python scripts/bootstrap_pii.py
python -m clean_corpus.cli build --config examples/build_local_jsonl.yaml
```

### Installation Errors

**Windows file locking:**
```powershell
taskkill /F /IM python.exe /T
pip install -e . --ignore-installed datasets numpy
```

**CLI not found:**
```bash
# Use Python module instead
python -m clean_corpus.cli build --config examples/build_local_jsonl.yaml
```

## Scripts

### Core Scripts

- `scripts/bootstrap_pii.py` - Register PII detectors (required)
- `scripts/run_pipeline.py` - All-in-one pipeline runner
- `scripts/run_pipeline.sh` - Shell wrapper
- `scripts/run_pipeline.bat` - Windows wrapper

### Utility Scripts

- `scripts/checkpoint_report.py` - Detailed checkpoint reports (includes per-file stats)
- `scripts/show_sources.py` - Inspect configured sources
- `scripts/show_run_info.py` - Display run summary
- `scripts/view_analytics.py` - View analytics data
- `scripts/verify_hf_dataset.py` - Verify HuggingFace datasets
- `scripts/diagnose_source.py` - Diagnose source configuration issues

## Examples

### Local JSONL Processing (Single File)

```bash
python scripts/run_pipeline.py examples/build_local_jsonl.yaml
```

### Multi-File Processing

Process multiple JSONL files together:

```yaml
sources:
  - name: "all_samples"
    type: "batch"
    kind: "local_jsonl"
    dataset:
      - "examples/sample_internal.jsonl"
      - "examples/sample_additional.jsonl"
      - "examples/sample_tech.jsonl"
    text_field: "text"
    license_field: "license"
    url_field: "url"
```

```bash
python scripts/run_pipeline.py examples/build_local_jsonl.yaml
```

**Output:**
- Per-file statistics in logs
- Dashboard shows per-file breakdown
- Each document tagged with source file

### PDF Processing

```yaml
pdf:
  chunk_mode: "page"
  extractor: "pymupdf"
  min_text_length: 200

sources:
  - name: "research_papers"
    type: "batch"
    kind: "pdf"
    dataset: "data/papers/"
```

```bash
python scripts/run_pipeline.py examples/build_pdf.yaml
```

### HuggingFace Dataset

```yaml
sources:
  - name: "common_pile"
    kind: "hf_stream"
    dataset: "common-pile/comma_v0.1_training_dataset"
    split: "train"
    text_field: "text"
```

```bash
python scripts/run_pipeline.py examples/build_common_pile.yaml
```

### Multiple Sources

```yaml
sources:
  - name: "source1"
    kind: "local_jsonl"
    dataset: "data1.jsonl"
  - name: "source2"
    kind: "hf_stream"
    dataset: "dataset/name"
```

Each source processes independently with its own checkpoint.

### Multi-File Source Example

Process multiple files together with per-file tracking:

```yaml
sources:
  - name: "all_samples"
    type: "batch"
    kind: "local_jsonl"
    dataset:
      - "examples/sample_internal.jsonl"
      - "examples/sample_additional.jsonl"
      - "examples/sample_tech.jsonl"
    text_field: "text"
    license_field: "license"
    url_field: "url"
```

**Output includes:**
- Per-file statistics in logs and dashboard
- `source_file` field in each document
- Per-file processed/written/rejected counts

## Architecture

### Pipeline Flow

```
Sources → Stages → Writers
   ↓        ↓        ↓
Raw Docs → Processed → Outputs
```

### Key Components

- **Sources** - Data ingestion (local JSONL, HuggingFace streaming, PDF)
  - Supports single files, multiple files, directories, and glob patterns
  - Per-file statistics tracking
- **Stages** - Processing steps (license, quality, dedup, PII)
- **Writers** - Output formats (Parquet, JSONL)
  - Includes `source_file` field for multi-file sources
- **Analytics** - Per-stage metrics and events
  - Per-file breakdowns available
- **Checkpoints** - Resume state per source
  - Includes per-file statistics

### Extensibility Points

- **Sources** - Add via `sources/registry.py`
- **Stages** - Add via `stages/registry.py`
- **PII Detectors** - Add via `pii/registry.py`
- **Writers** - Add via `writers/registry.py`
- **Tokenizers** - Add via `plugins/registry.py`

## License

MIT

## Data Tracking

### Per-File Statistics

When processing multiple files in a single source, the platform automatically tracks:

- **Per-file processed counts** - How many documents from each file
- **Per-file written counts** - How many documents passed all stages
- **Per-file rejected counts** - How many documents were rejected
- **Source file tracking** - Each document includes `source_file` field

**View per-file stats:**

```bash
# In logs during processing
# Look for: "Source X per-file statistics:"

# In dashboard
python -m clean_corpus.cli monitor storage_example --unified
# See "Per-File Statistics" table

# In checkpoint report
python scripts/checkpoint_report.py storage_example
```

**Output data includes:**
- `source_file` field in Parquet documents
- `source_file` field in metadata files
- `source_file` field in rejection logs

### PDF Processing Features

**Chunking Strategies:**
- `page` - Each PDF page becomes a separate document (best for long documents)
- `document` - Entire PDF becomes one document (best for short PDFs)
- `fixed_size` - Split into fixed-size chunks with overlap (best for very long documents)

**Schema Configuration:**
- Global schema applies to all PDFs by default
- Directory-specific schemas can override global settings
- Supports text transformations (prefix/suffix)
- Metadata field mapping
- Default license assignment

### Web PDF Downloader

Download PDFs from websites and process them with automatic language detection:

```yaml
sources:
  - name: "ncert_hindi"
    kind: "web_pdf"
    urls:
      - "https://ncert.nic.in/textbook/pdf/iehi101.pdf"
    download_dir: "downloads/ncert/hindi"
    language: "hi"  # Hindi (ISO 639-1)
    auto_detect_language: true
    metadata:
      source: "NCERT"
      license: "CC-BY-NC"
      category: "textbook"
```

**Features:**
- Download from single URL, list of URLs, or URL patterns
- Automatic language detection from PDF content
- Multi-language support (Hindi, Tamil, English, etc.)
- Resume support (skip already downloaded files)
- Metadata extraction (title, author, language, source URL)

See `docs/WEB_PDF_DOWNLOADER.md` for complete guide.

## Contributing

See examples in `examples/` directory for adding custom sources, stages, and detectors.

**New Features:**
- Multi-file processing examples: `examples/build_local_jsonl.yaml`
- PDF processing examples: `examples/build_pdf.yaml`
- Web PDF downloader examples: `examples/build_web_pdf_ncert.yaml`
- Multi-source examples: `examples/build_multi_source.yaml`
