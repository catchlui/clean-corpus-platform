# Output Structure Documentation

This document describes the structure of output files for each format supported by the Clean Corpus Platform.

## Output Layouts

### Flat layout (default: `output.layout: "flat"`)

- **Documents:** `{out_dir}/docs/source={source_name}/shard_{idx:06d}.jsonl` (or `.parquet`)
- **Rejections:** `{out_dir}/rejections/rejections.jsonl` (single file)
- **Manifests, metadata, analytics:** `{out_dir}/manifests/`, `metadata/`, `analytics/`

### Structured layout (`output.layout: "structured"`)

Target structure aligned with `/processed/v1/`:

```
{out_dir}/
├── documents/
│   ├── ncert/
│   │   ├── en/
│   │   │   ├── physics/
│   │   │   │   └── class11/
│   │   │   │       └── shard_000001.jsonl
│   │   └── hi/
│   │       └── hindi/
│   │           └── class10/
│   └── dolma/
│       ├── wikipedia/
│       ├── books/
│       └── web/
├── rejected/
│   ├── pii/
│   │   └── rejections.jsonl
│   ├── duplicates/
│   │   └── rejections.jsonl
│   ├── corrupt/
│   │   └── rejections.jsonl
│   └── low_quality/
│       └── rejections.jsonl
└── stats/
    ├── pii_report.json
    ├── dedup_report.json
    └── quality_report.json
```

- **Documents:** `{out_dir}/documents/{namespace}/{lang}/{domain?}/{grade?}/shard_*.jsonl`  
  Path is built from `output.source_to_namespace` (source → namespace) and doc `lang` and `extra` (subject, grade).
- **Rejections:** Split by reason: `rejected/pii/`, `rejected/duplicates/`, `rejected/corrupt/`, `rejected/low_quality/`.
- **Stats:** `stats/pii_report.json`, `stats/dedup_report.json`, `stats/quality_report.json`.

To use structured layout, set in config:

```yaml
output:
  layout: "structured"
  source_to_namespace:
    class4_hindi_veena: "ncert"
    wikipedia_stream: "dolma"
```

## Document Structure

All output formats include a well-defined structure with:

### Core Fields (Always Present)

- **doc_id** - Unique document identifier (hex string or binary)
- **text** - Document text content
- **source** - Source name
- **lang** - Language code (ISO 639-1)
- **url** - Source URL (if available)
- **license** - License type
- **license_version** - License version (if available)
- **source_file** - Original file path (for multi-file sources)

### Statistics Fields

- **tokens** - Token count (if tokenized)
- **chars** - Character count
- **bytes_utf8** - UTF-8 byte count
- **entropy** - Character entropy
- **ppl** - Perplexity (if calculated)
- **quality_score** - Quality score (if calculated)

### Processing Fields

- **dup_group_id** - Duplicate group identifier
- **pii_flag** - Whether PII was detected
- **pii_types** - List of PII types detected
- **policy_version** - Policy version used
- **transform_chain** - List of transformations applied
- **created_at_ms** - Creation timestamp (milliseconds)

### Custom Metadata Fields

- **extra_metadata** - Custom metadata (folder-level metadata, PDF metadata, etc.)
  - Includes: `book_name`, `author`, `certificate_type`, `pdf_metadata`, etc.
  - Format varies by output type (see below)

## Format-Specific Structures

### 1. Parquet Format

**File:** `{out_dir}/docs/source={source_name}/shard_{idx:06d}.parquet`

**Schema:**
```python
{
    "doc_id": binary(32),
    "source": string,
    "lang": string,
    "text": string,
    "url": string,
    "license": string,
    "license_version": string,
    "source_file": string,
    "tokens": int64,
    "chars": int64,
    "bytes_utf8": int64,
    "entropy": float32,
    "ppl": float32,
    "quality_score": float32,
    "dup_group_id": int64,
    "pii_flag": bool,
    "pii_types": list<string>,
    "policy_version": string,
    "transform_chain": list<string>,
    "created_at_ms": int64,
    "extra_metadata": string  # JSON string with custom metadata
}
```

**Example extra_metadata (JSON string):**
```json
{
  "book_name": "Mathematics Grade 10",
  "author": "John Smith",
  "certificate_type": "Textbook",
  "publisher": "Education Press",
  "pdf_file": "data/chapters/chapter1.pdf",
  "pdf_metadata": {
    "title": "Chapter 1",
    "author": "John Smith",
    "page_number": 1
  }
}
```

### 2. JSONL Format

**File:** `{out_dir}/docs/source={source_name}/shard_{idx:06d}.jsonl`

**Structure:**
```json
{
  "doc_id": "abc123...",
  "text": "document text content",
  "source": "source_name",
  "lang": "en",
  "url": "https://example.com/doc",
  "license": "CC-BY",
  "license_version": "4.0",
  "source_file": "data/file.jsonl",
  "tokens": 1234,
  "chars": 5678,
  "bytes_utf8": 5678,
  "entropy": 4.5,
  "ppl": null,
  "quality_score": 0.95,
  "dup_group_id": 123456789,
  "pii_flag": false,
  "pii_types": [],
  "policy_version": "policy_v1",
  "transform_chain": ["license_gate_v1", "sanitize_v1", "unicode_nfc_normalized_v1"],
  "created_at_ms": 1704067200000,
  "book_name": "Mathematics Grade 10",
  "author": "John Smith",
  "certificate_type": "Textbook",
  "publisher": "Education Press",
  "pdf_file": "data/chapters/chapter1.pdf",
  "pdf_metadata": {
    "title": "Chapter 1",
    "author": "John Smith",
    "page_number": 1
  }
}
```

**Note:** In JSONL format, custom metadata fields (from `extra`) are flattened into the root object.

### 3. DOLMA/DOML Format

**File:** `{out_dir}/docs/source={source_name}/shard_{idx:06d}.jsonl`

**Structure:**
```json
{
  "id": "abc123...",
  "text": "document text content",
  "metadata": {
    "source": "source_name",
    "license": "CC-BY",
    "license_version": "4.0",
    "url": "https://example.com/doc",
    "language": "en",
    "source_file": "data/file.jsonl",
    "tokens": 1234,
    "chars": 5678,
    "bytes_utf8": 5678,
    "entropy": 4.5,
    "ppl": null,
    "quality_score": 0.95,
    "dup_group_id": 123456789,
    "pii_flag": false,
    "pii_types": [],
    "policy_version": "policy_v1",
    "transform_chain": ["license_gate_v1", "sanitize_v1"],
    "created_at_ms": 1704067200000,
    "book_name": "Mathematics Grade 10",
    "author": "John Smith",
    "certificate_type": "Textbook",
    "publisher": "Education Press",
    "pdf_file": "data/chapters/chapter1.pdf",
    "pdf_metadata": {
      "title": "Chapter 1",
      "author": "John Smith",
      "page_number": 1
    }
  }
}
```

**Note:** In DOLMA/DOML format, all metadata (including custom fields) is nested under the `metadata` object.

## Custom Metadata Fields

Custom metadata fields are added from:

1. **Folder-level metadata** (for PDF folders):
   ```yaml
   sources:
     - name: "textbook"
       kind: "pdf"
       dataset: "data/chapters/"
       metadata:
         book_name: "Mathematics Grade 10"
         author: "John Smith"
         certificate_type: "Textbook"
   ```

2. **PDF metadata** (extracted from PDF files):
   - `pdf_file` - Path to PDF file
   - `pdf_metadata` - PDF metadata object (title, author, etc.)
   - `page_number` - Page number (for page-level chunking)
   - `chunk_number` - Chunk number (for fixed-size chunking)

3. **Web PDF metadata**:
   - `source_url` - Original URL
   - `download_timestamp` - Download time
   - Custom metadata from configuration

## Structure Validation

The output structure is validated by:

1. **Parquet Schema** - Enforced by PyArrow schema
2. **JSONL Structure** - Validated by JSON serialization
3. **DOLMA Structure** - Validated by format specification

## Extending the Structure

To add new fields to the output structure:

1. **Add to Document model** (`src/clean_corpus/pipeline/context.py`)
2. **Update writers** to include the new field
3. **Update schema** (for Parquet format)
4. **Update documentation** (this file)

## Examples

### Example: PDF Folder with Metadata

**Configuration:**
```yaml
sources:
  - name: "textbook"
    kind: "pdf"
    dataset: "data/chapters/"
    metadata:
      book_name: "Mathematics Grade 10"
      author: "John Smith"
      certificate_type: "Textbook"
```

**Output (JSONL):**
```json
{
  "doc_id": "...",
  "text": "...",
  "source": "textbook",
  "book_name": "Mathematics Grade 10",
  "author": "John Smith",
  "certificate_type": "Textbook",
  "pdf_file": "data/chapters/chapter1.pdf",
  "pdf_metadata": {...}
}
```

### Example: Multiple Custom Fields

**Configuration:**
```yaml
sources:
  - name: "certificates"
    kind: "pdf"
    dataset: "data/certs/"
    metadata:
      certificate_type: "Academic Achievement"
      issuing_authority: "University Name"
      category: "Education"
      year: "2024"
```

**Output includes all custom fields** in the document structure.

## See Also

- `docs/OUTPUT_FORMATS.md` - Format comparison and selection guide
- `src/clean_corpus/pipeline/context.py` - Document model definition
- `src/clean_corpus/storage/writer.py` - Parquet schema definition
- `src/clean_corpus/writers/` - Writer implementations
