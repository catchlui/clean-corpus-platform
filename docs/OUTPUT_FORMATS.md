# Output Formats Guide

The Clean Corpus Platform supports multiple output formats for processed documents. You can choose the format that best fits your use case, and the system is designed to be extensible for adding custom formats.

## Supported Formats

### 1. Parquet (Default)

**Format:** Apache Parquet (columnar storage)

**Best for:**
- Large-scale datasets
- Efficient storage and querying
- Integration with data processing frameworks (Spark, Pandas, etc.)

**Configuration:**
```yaml
output:
  corpus_format: "parquet"
  metadata_format: "parquet_v1"
```

**Output Structure:**
- `{out_dir}/docs/source={source_name}/shard_{idx:06d}.parquet`
- `{out_dir}/metadata/schema=meta_v1/source={source_name}/shard_{idx:06d}.parquet`

### 2. JSONL

**Format:** JSON Lines (one JSON object per line)

**Best for:**
- Simple text processing
- Human-readable output
- Streaming processing

**Configuration:**
```yaml
output:
  corpus_format: "jsonl"
  metadata_format: "parquet_v1"
```

**Output Structure:**
- `{out_dir}/docs/source={source_name}/shard_{idx:06d}.jsonl`
- `{out_dir}/metadata/schema=meta_v1/source={source_name}/shard_{idx:06d}.parquet`

### 3. DOLMA / DOML

**Format:** DOLMA format (AI2 corpus format)

**Best for:**
- Language model training
- AI2 Dolma-compatible pipelines
- Structured metadata with nested objects

**Configuration:**
```yaml
output:
  corpus_format: "dolma"  # or "doml" (alias)
  metadata_format: "parquet_v1"
```

**Format Structure:**
```json
{
  "id": "doc_id_hex",
  "text": "document text content",
  "metadata": {
    "source": "source_name",
    "license": "CC-BY",
    "url": "source_url",
    "language": "en",
    "tokens": 1234,
    "chars": 5678,
    ...
  }
}
```

**Output Structure:**
- `{out_dir}/docs/source={source_name}/shard_{idx:06d}.jsonl` (DOLMA format)
- `{out_dir}/metadata/schema=meta_v1/source={source_name}/shard_{idx:06d}.parquet`

**Note:** `doml` is an alias for `dolma` - both use the same format.

## Format-Specific Options

Some formats support additional configuration options:

### DOLMA/DOML Options

```yaml
output:
  corpus_format: "dolma"
  metadata_format: "parquet_v1"
  format_options:
    dolma:
      include_all_metadata: true  # Include all available metadata fields
      custom_metadata_fields:
        dataset_version: "v1.0"
        processing_date: "2026-01-30"
```

## Adding Custom Output Formats

The output format system is extensible. You can add custom formats by:

### 1. Create a Custom Writer

Create a new writer class that extends `CorpusWriter`:

```python
# src/clean_corpus/writers/my_format.py
from __future__ import annotations
from typing import Iterable
from .base import CorpusWriter
from ..pipeline.context import Document

class MyFormatWriter(CorpusWriter):
    """Writer for My Custom Format."""
    name = "my_format"
    
    def write_shard(
        self, 
        docs: Iterable[Document], 
        *, 
        out_dir: str, 
        source: str, 
        shard_idx: int
    ) -> str:
        """Write documents in custom format."""
        import os
        path = os.path.join(
            out_dir, 
            "docs", 
            f"source={source}", 
            f"shard_{shard_idx:06d}.myformat"
        )
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        with open(path, "w", encoding="utf-8") as f:
            for doc in docs:
                # Write document in your custom format
                f.write(f"{doc.doc_id.hex()}\t{doc.text}\n")
        
        return path
```

### 2. Register the Writer

Register your custom writer in the registry:

```python
# In your initialization code or plugin
from clean_corpus.writers.registry import register_corpus_writer
from clean_corpus.writers.my_format import MyFormatWriter

register_corpus_writer("my_format", MyFormatWriter())
```

### 3. Use in Configuration

Use your custom format in the configuration:

```yaml
output:
  corpus_format: "my_format"
  metadata_format: "parquet_v1"
```

## Format Comparison

| Format | File Extension | Storage Efficiency | Human Readable | Best For |
|--------|---------------|-------------------|----------------|----------|
| **Parquet** | `.parquet` | ⭐⭐⭐⭐⭐ | ⭐⭐ | Large datasets, analytics, efficient storage |
| **JSONL** | `.jsonl` | ⭐⭐⭐ | ⭐⭐⭐⭐ | Simple processing, streaming, human-readable |
| **DOLMA/DOML** | `.jsonl` | ⭐⭐⭐ | ⭐⭐⭐⭐ | LM training, AI2 pipelines, structured metadata |

## Quick Format Selection Guide

**Choose Parquet if:**
- Processing large-scale datasets (millions+ documents)
- Need efficient storage and fast querying
- Using data processing frameworks (Spark, Pandas, Dask)
- Running analytics on the corpus
- Storage space is a concern

**Choose JSONL if:**
- Need human-readable output for debugging
- Processing documents in a streaming fashion
- Simple text processing pipelines
- Want to easily inspect individual documents
- Working with smaller datasets

**Choose DOLMA/DOML if:**
- Training language models
- Need compatibility with AI2 Dolma pipelines
- Want nested metadata structure
- Require structured metadata format for training

## Recommendations

- **For large-scale production:** Use `parquet` for efficient storage and querying
- **For language model training:** Use `dolma` or `doml` for compatibility with training pipelines
- **For debugging/testing:** Use `jsonl` for human-readable output
- **For custom pipelines:** Create a custom writer for your specific format needs

## See Also

- `configs/standard_template.yaml` - Configuration template with format examples
- `src/clean_corpus/writers/` - Writer implementations
- `src/clean_corpus/writers/registry.py` - Writer registration system
