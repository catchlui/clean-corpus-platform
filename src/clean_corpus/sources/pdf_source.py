"""PDF batch source.

Supports processing PDF files to extract text for training datasets.

Features:
- Single PDF files or directories of PDFs
- Multiple extraction backends (PyPDF2, pdfplumber, pymupdf)
- Configurable chunking (by page, by document, custom)
- Metadata extraction (title, author, page numbers)
- Progress tracking for large PDF collections

Configuration:
```yaml
sources:
  - name: "research_papers"
    type: "batch"
    kind: "pdf"
    dataset: "data/papers/"  # Directory or single file
    chunk_mode: "page"       # page | document | custom
    extractor: "pymupdf"     # pymupdf | pdfplumber | pypdf2
    min_text_length: 100     # Minimum characters per chunk
    metadata_fields: ["title", "author", "page"]  # Optional metadata to extract
```
"""

from __future__ import annotations
import os
import glob
from pathlib import Path
from typing import Iterable, Optional, List, Dict, Any
from .base import DataSource, DataSourceType, RawDocument, SourceSpec

class PDFSource(DataSource):
    """PDF batch source for processing PDF files."""
    
    source_type = DataSourceType.BATCH
    
    def __init__(self, spec: SourceSpec, global_pdf_config: Optional[Dict[str, Any]] = None):
        self.spec = spec
        self.name = spec.name
        self.dataset = spec.dataset  # Path to PDF file or directory
        
        # Get global PDF config (if provided)
        global_config = global_pdf_config or {}
        
        # Get PDF-specific options from spec (with defaults)
        # Directory-specific schema overrides global config
        spec_chunk_mode = getattr(spec, 'chunk_mode', None)
        self.chunk_mode = spec_chunk_mode if spec_chunk_mode is not None else global_config.get("chunk_mode", "page")  # page | document | fixed_size
        spec_extractor = getattr(spec, 'extractor', None)
        self.extractor_name = spec_extractor if spec_extractor is not None else global_config.get("extractor", "pymupdf")  # pymupdf | pdfplumber | pypdf2
        self.min_text_length = getattr(spec, 'min_text_length', None)
        if self.min_text_length is None:
            self.min_text_length = global_config.get("min_text_length", 100)
        self.metadata_fields = getattr(spec, 'metadata_fields', None) or global_config.get("metadata_fields", [])
        
        # Fixed-size chunking options
        self.chunk_size = getattr(spec, 'chunk_size', None)
        if self.chunk_size is None:
            self.chunk_size = global_config.get("chunk_size", 1000)
        self.chunk_overlap = getattr(spec, 'chunk_overlap', None)
        if self.chunk_overlap is None:
            self.chunk_overlap = global_config.get("chunk_overlap", 200)
        
        # Schema configuration: directory-specific overrides global
        self.schema = getattr(spec, 'schema', None)
        if not self.schema:
            self.schema = global_config.get("schema")
        
        # Initialize extractor
        self.extractor = self._get_extractor()
    
    def _get_extractor(self):
        """Get PDF extractor based on configuration."""
        if self.extractor_name == 'pymupdf':
            try:
                import fitz  # PyMuPDF
                return PyMuPDFExtractor()
            except ImportError:
                raise ImportError(
                    "PyMuPDF not installed. Install with: pip install pymupdf\n"
                    "Or use pdfplumber: pip install pdfplumber"
                )
        elif self.extractor_name == 'pdfplumber':
            try:
                import pdfplumber
                return PDFPlumberExtractor()
            except ImportError:
                raise ImportError(
                    "pdfplumber not installed. Install with: pip install pdfplumber\n"
                    "Or use pymupdf: pip install pymupdf"
                )
        elif self.extractor_name == 'pypdf2':
            try:
                import PyPDF2
                return PyPDF2Extractor()
            except ImportError:
                raise ImportError(
                    "PyPDF2 not installed. Install with: pip install PyPDF2\n"
                    "Or use pymupdf: pip install pymupdf"
                )
        else:
            raise ValueError(f"Unknown extractor: {self.extractor_name}. Use: pymupdf, pdfplumber, or pypdf2")
    
    def metadata(self) -> Dict[str, Any]:
        """Return source metadata."""
        pdf_path = Path(self.dataset)
        if pdf_path.is_file():
            file_count = 1
            total_size = pdf_path.stat().st_size
        elif pdf_path.is_dir():
            pdf_files = list(pdf_path.glob("*.pdf"))
            file_count = len(pdf_files)
            total_size = sum(f.stat().st_size for f in pdf_files)
        else:
            file_count = 0
            total_size = 0
        
        return {
            "kind": "pdf",
            "path": str(self.dataset),
            "file_count": file_count,
            "total_size_bytes": total_size,
            "chunk_mode": self.chunk_mode,
            "extractor": self.extractor_name
        }
    
    def _get_pdf_files(self) -> List[Path]:
        """Get list of PDF files to process."""
        pdf_path = Path(self.dataset)
        if pdf_path.is_file():
            return [pdf_path]
        elif pdf_path.is_dir():
            # Recursively find all PDF files
            return list(pdf_path.rglob("*.pdf"))
        else:
            raise FileNotFoundError(f"PDF path not found: {self.dataset}")
    
    def _chunk_text_fixed_size(self, text: str, chunk_id_prefix: str) -> Iterable[tuple[str, int]]:
        """Chunk text into fixed-size chunks with overlap."""
        if len(text) <= self.chunk_size:
            yield (text, 0)
            return
        
        start = 0
        chunk_idx = 0
        while start < len(text):
            end = start + self.chunk_size
            chunk = text[start:end]
            
            # Try to break at word boundary if not at end
            if end < len(text):
                # Look for last space/newline in last 10% of chunk
                boundary_search_start = max(start + int(self.chunk_size * 0.9), start)
                last_space = text.rfind(' ', boundary_search_start, end)
                last_newline = text.rfind('\n', boundary_search_start, end)
                boundary = max(last_space, last_newline)
                
                if boundary > boundary_search_start:
                    end = boundary + 1
                    chunk = text[start:end]
            
            yield (chunk, chunk_idx)
            
            # Move start forward with overlap
            start = end - self.chunk_overlap
            chunk_idx += 1
    
    def stream(self) -> Iterable[RawDocument]:
        """Stream documents from PDF files."""
        pdf_files = self._get_pdf_files()
        
        for pdf_file in pdf_files:
            try:
                # Apply directory-specific schema if this PDF is in a matching directory
                pdf_dir = pdf_file.parent
                apply_schema = self._should_apply_schema(pdf_dir)
                
                if self.chunk_mode == 'document':
                    # Extract entire document as one chunk
                    text, metadata = self.extractor.extract_full(pdf_file)
                    if len(text) >= self.min_text_length:
                        yield self._create_document(
                            pdf_file, text, metadata, chunk_id=f"{pdf_file.stem}",
                            url=str(pdf_file), apply_schema=apply_schema
                        )
                elif self.chunk_mode == 'page':
                    # Extract page by page
                    pages = self.extractor.extract_pages(pdf_file)
                    for page_num, (text, page_metadata) in enumerate(pages, start=1):
                        if len(text) >= self.min_text_length:
                            yield self._create_document(
                                pdf_file, text, page_metadata,
                                chunk_id=f"{pdf_file.stem}_page_{page_num}",
                                url=f"{pdf_file}#page={page_num}",
                                page_number=page_num,
                                apply_schema=apply_schema
                            )
                elif self.chunk_mode == 'fixed_size':
                    # Extract full text and chunk into fixed-size pieces
                    text, metadata = self.extractor.extract_full(pdf_file)
                    for chunk_idx, (chunk_text, chunk_num) in enumerate(self._chunk_text_fixed_size(text, pdf_file.stem)):
                        if len(chunk_text) >= self.min_text_length:
                            yield self._create_document(
                                pdf_file, chunk_text, metadata,
                                chunk_id=f"{pdf_file.stem}_chunk_{chunk_num}",
                                url=f"{pdf_file}#chunk={chunk_num}",
                                chunk_number=chunk_num,
                                apply_schema=apply_schema
                            )
                else:
                    raise ValueError(f"Unknown chunk_mode: {self.chunk_mode}. Use 'page', 'document', or 'fixed_size'")
            except Exception as e:
                # Log error but continue processing other PDFs
                import logging
                logging.getLogger("clean_corpus.sources.pdf").warning(
                    f"Error processing PDF {pdf_file}: {e}"
                )
                continue
    
    def _should_apply_schema(self, pdf_dir: Path) -> bool:
        """Check if schema should be applied based on directory matching."""
        if not self.schema:
            return False
        
        # If schema has a 'directory_pattern' field, check if it matches
        if isinstance(self.schema, dict) and 'directory_pattern' in self.schema:
            import re
            pattern = self.schema['directory_pattern']
            return bool(re.search(pattern, str(pdf_dir)))
        
        # Default: apply schema if it exists
        return True
    
    def _create_document(self, pdf_file: Path, text: str, metadata: Dict[str, Any],
                        chunk_id: str, url: str, page_number: Optional[int] = None,
                        chunk_number: Optional[int] = None, apply_schema: bool = False) -> RawDocument:
        """Create a RawDocument with optional schema transformation."""
        extra = {
            "pdf_file": str(pdf_file),
            "pdf_metadata": metadata,
            **{k: metadata.get(k) for k in self.metadata_fields if k in metadata}
        }
        
        if page_number is not None:
            extra["page_number"] = page_number
        if chunk_number is not None:
            extra["chunk_number"] = chunk_number
        
        # Apply schema transformation if configured
        if apply_schema and self.schema:
            text, extra = self._apply_schema(text, extra, metadata)
        
        # Ensure raw_id is string
        raw_id_str = str(chunk_id) if not isinstance(chunk_id, str) else chunk_id
        
        return RawDocument(
            raw_id=raw_id_str,
            text=text,
            source=self.spec.name,
            url=url,
            license=self.schema.get("default_license") if apply_schema and isinstance(self.schema, dict) else None,
            created_at=None,
            extra=extra
        )
    
    def _apply_schema(self, text: str, extra: Dict[str, Any], metadata: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        """Apply schema transformations to text and metadata."""
        if not isinstance(self.schema, dict):
            return text, extra
        
        # Apply text transformations
        transformed_text = text
        
        # Add prefix/suffix if specified
        if 'text_prefix' in self.schema:
            transformed_text = self.schema['text_prefix'] + transformed_text
        if 'text_suffix' in self.schema:
            transformed_text = transformed_text + self.schema['text_suffix']
        
        # Apply metadata mappings
        if 'metadata_mapping' in self.schema:
            mapping = self.schema['metadata_mapping']
            for target_key, source_key in mapping.items():
                if source_key in metadata:
                    extra[target_key] = metadata[source_key]
        
        return transformed_text, extra


class PDFExtractor:
    """Base class for PDF extractors."""
    
    def extract_full(self, pdf_path: Path) -> tuple[str, Dict[str, Any]]:
        """Extract full document text and metadata."""
        raise NotImplementedError
    
    def extract_pages(self, pdf_path: Path) -> Iterable[tuple[str, Dict[str, Any]]]:
        """Extract text page by page."""
        raise NotImplementedError


class PyMuPDFExtractor(PDFExtractor):
    """PyMuPDF (fitz) extractor - fast and accurate."""
    
    def extract_full(self, pdf_path: Path) -> tuple[str, Dict[str, Any]]:
        import fitz
        doc = fitz.open(str(pdf_path))
        text_parts = []
        metadata = {
            "title": doc.metadata.get("title", ""),
            "author": doc.metadata.get("author", ""),
            "subject": doc.metadata.get("subject", ""),
            "total_pages": len(doc)
        }
        
        for page in doc:
            text_parts.append(page.get_text())
        
        doc.close()
        return "\n\n".join(text_parts), metadata
    
    def extract_pages(self, pdf_path: Path) -> Iterable[tuple[str, Dict[str, Any]]]:
        import fitz
        doc = fitz.open(str(pdf_path))
        doc_metadata = {
            "title": doc.metadata.get("title", ""),
            "author": doc.metadata.get("author", ""),
            "subject": doc.metadata.get("subject", ""),
        }
        
        for page_num, page in enumerate(doc, start=1):
            text = page.get_text()
            page_metadata = {**doc_metadata, "page_number": page_num}
            yield text, page_metadata
        
        doc.close()


class PDFPlumberExtractor(PDFExtractor):
    """pdfplumber extractor - good for tables and structured content."""
    
    def extract_full(self, pdf_path: Path) -> tuple[str, Dict[str, Any]]:
        import pdfplumber
        text_parts = []
        metadata = {}
        
        with pdfplumber.open(str(pdf_path)) as pdf:
            metadata = {
                "title": pdf.metadata.get("Title", ""),
                "author": pdf.metadata.get("Author", ""),
                "subject": pdf.metadata.get("Subject", ""),
                "total_pages": len(pdf.pages)
            }
            
            for page in pdf.pages:
                text_parts.append(page.extract_text() or "")
        
        return "\n\n".join(text_parts), metadata
    
    def extract_pages(self, pdf_path: Path) -> Iterable[tuple[str, Dict[str, Any]]]:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            doc_metadata = {
                "title": pdf.metadata.get("Title", ""),
                "author": pdf.metadata.get("Author", ""),
                "subject": pdf.metadata.get("Subject", ""),
            }
            
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                page_metadata = {**doc_metadata, "page_number": page_num}
                yield text, page_metadata


class PyPDF2Extractor(PDFExtractor):
    """PyPDF2 extractor - lightweight but less accurate."""
    
    def extract_full(self, pdf_path: Path) -> tuple[str, Dict[str, Any]]:
        import PyPDF2
        text_parts = []
        
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            metadata = {
                "title": pdf_reader.metadata.get("/Title", "") if pdf_reader.metadata else "",
                "author": pdf_reader.metadata.get("/Author", "") if pdf_reader.metadata else "",
                "total_pages": len(pdf_reader.pages)
            }
            
            for page in pdf_reader.pages:
                text_parts.append(page.extract_text())
        
        return "\n\n".join(text_parts), metadata
    
    def extract_pages(self, pdf_path: Path) -> Iterable[tuple[str, Dict[str, Any]]]:
        import PyPDF2
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            doc_metadata = {
                "title": pdf_reader.metadata.get("/Title", "") if pdf_reader.metadata else "",
                "author": pdf_reader.metadata.get("/Author", "") if pdf_reader.metadata else "",
            }
            
            for page_num, page in enumerate(pdf_reader.pages, start=1):
                text = page.extract_text()
                page_metadata = {**doc_metadata, "page_number": page_num}
                yield text, page_metadata
