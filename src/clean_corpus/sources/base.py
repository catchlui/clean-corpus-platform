"""Data source plugin interface.

Goal: allow multiple teams to add sources without changing pipeline code.

A source can be:
- streaming (HF streaming, APIs, Kafka)
- batch (local dumps, S3 files)
- incremental (daily drops, CDC)

All sources expose a unified `stream()` generator yielding RawDocument.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, Optional, List, Union

class DataSourceType(str, Enum):
    STREAMING = "streaming"
    BATCH = "batch"
    INCREMENTAL = "incremental"
    SNAPSHOT = "snapshot"

@dataclass
class RawDocument:
    raw_id: str
    text: str
    source: str
    url: Optional[str] = None
    license: Optional[str] = None
    created_at: Optional[str] = None
    extra: Dict[str, Any] = None

@dataclass
class SourceSpec:
    name: str
    type: str               # streaming|batch|incremental|snapshot
    kind: str               # implementation key, e.g., hf_stream, pdf
    dataset: Union[str, List[str]]  # path to dataset/file/directory, or list of files, or glob pattern
    split: str = "train"
    text_field: str = "text"
    license_field: str = "license"
    url_field: str = "url"
    # PDF-specific options (optional, only used when kind="pdf")
    chunk_mode: str = "page"  # page | document | fixed_size
    extractor: str = "pymupdf"  # pymupdf | pdfplumber | pypdf2
    min_text_length: int = 100
    metadata_fields: List[str] = field(default_factory=list)
    # Fixed-size chunking options (when chunk_mode="fixed_size")
    chunk_size: int = 1000  # Characters per chunk
    chunk_overlap: int = 200  # Overlap between chunks
    # Schema configuration (for PDF sources)
    schema: Optional[Dict[str, Any]] = None  # Directory-specific schema override
    # Web PDF download options (only used when kind="web_pdf")
    urls: Union[str, List[str]] = field(default_factory=list)  # List of PDF URLs to download
    url_pattern: Optional[str] = None  # URL pattern to scrape (e.g., "https://site.com/pdf/*.pdf")
    base_url: Optional[str] = None  # Base URL for relative URLs
    download_dir: Optional[str] = None  # Directory to download PDFs to
    resume_download: bool = True  # Skip already downloaded files
    timeout: int = 30  # Download timeout in seconds
    max_retries: int = 3  # Maximum download retries
    language: Optional[str] = None  # ISO 639-1 language code (en, hi, ta, etc.)
    auto_detect_language: bool = True  # Automatically detect language from PDF content
    metadata: Optional[Dict[str, Any]] = None  # Additional metadata to add to documents

class DataSource:
    """Base interface for all sources."""
    name: str
    source_type: DataSourceType

    def schema(self) -> Dict[str, str]:
        return {"text": "string", "license": "string", "url": "string"}

    def metadata(self) -> Dict[str, Any]:
        return {}

    def stream(self) -> Iterable[RawDocument]:
        raise NotImplementedError


from clean_corpus.utils.fingerprint import stable_fingerprint

def _source_fingerprint(spec_dict: dict) -> str:
    return stable_fingerprint(spec_dict)
