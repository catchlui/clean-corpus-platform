"""Web PDF downloader source.

Downloads PDFs from websites and processes them into dataset format.

Features:
- Download PDFs from URLs (single URL, list of URLs, or URL patterns)
- Automatic language detection
- Metadata extraction (title, author, language, source URL)
- Configurable download directory
- Resume support (skip already downloaded files)
- Support for multiple languages

Configuration:
```yaml
sources:
  - name: "ncert_books"
    type: "batch"
    kind: "web_pdf"
    urls:
      - "https://ncert.nic.in/textbook/pdf/..."
      - "https://ncert.nic.in/textbook/pdf/..."
    download_dir: "downloads/ncert"
    language: "hi"  # Optional: ISO 639-1 code (hi, en, ta, etc.)
    auto_detect_language: true  # Detect language from PDF content
    metadata:
      source: "NCERT"
      license: "CC-BY-NC"
      category: "textbook"
```

For URL patterns/scraping:
```yaml
sources:
  - name: "ncert_all"
    type: "batch"
    kind: "web_pdf"
    url_pattern: "https://ncert.nic.in/textbook/pdf/*.pdf"  # Pattern
    download_dir: "downloads/ncert"
    auto_detect_language: true
```

"""

from __future__ import annotations
import os
import re
import json
import time
from pathlib import Path
from typing import Iterable, Optional, List, Dict, Any, Union
from urllib.parse import urlparse, urljoin
from dataclasses import field

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False
    requests = None

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    BeautifulSoup = None

try:
    from langdetect import detect, LangDetectException
    HAS_LANGDETECT = True
except ImportError:
    HAS_LANGDETECT = False
    langdetect = None

from .base import DataSource, DataSourceType, RawDocument, SourceSpec


class WebPDFSource(DataSource):
    """Web PDF downloader source - downloads PDFs from URLs and processes them."""
    
    source_type = DataSourceType.BATCH
    
    def __init__(self, spec: SourceSpec, global_pdf_config: Optional[Dict[str, Any]] = None):
        self.spec = spec
        self.name = spec.name
        
        # Get URLs from spec
        self.urls = getattr(spec, 'urls', None) or []
        self.url_pattern = getattr(spec, 'url_pattern', None)
        self.base_url = getattr(spec, 'base_url', None)  # For relative URLs
        
        # Download configuration
        self.download_dir = getattr(spec, 'download_dir', None) or f"downloads/{spec.name}"
        self.resume_download = getattr(spec, 'resume_download', True)
        self.timeout = getattr(spec, 'timeout', 30)
        self.max_retries = getattr(spec, 'max_retries', 3)
        
        # Language configuration
        self.language = getattr(spec, 'language', None)  # ISO 639-1 code (en, hi, ta, etc.)
        self.auto_detect_language = getattr(spec, 'auto_detect_language', True)
        
        # Metadata configuration
        self.metadata_config = getattr(spec, 'metadata', None) or {}
        
        # PDF processing configuration (reuse PDFSource)
        self.global_pdf_config = global_pdf_config or {}
        self.chunk_mode = getattr(spec, 'chunk_mode', None) or self.global_pdf_config.get("chunk_mode", "page")
        self.extractor_name = getattr(spec, 'extractor', None) or self.global_pdf_config.get("extractor", "pymupdf")
        
        # Ensure download directory exists
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Resolve URLs
        self.pdf_urls = self._resolve_urls()
        
        # Initialize PDF source for processing downloaded files
        self.pdf_source = None  # Will be created after download
    
    def _resolve_urls(self) -> List[str]:
        """Resolve URLs from configuration."""
        urls = []
        
        # Direct URL list
        if self.urls:
            if isinstance(self.urls, str):
                urls.append(self.urls)
            elif isinstance(self.urls, list):
                urls.extend(self.urls)
        
        # URL pattern (scrape from webpage)
        if self.url_pattern:
            pattern_urls = self._scrape_urls_from_pattern(self.url_pattern)
            urls.extend(pattern_urls)
        
        # Base URL + relative paths
        if self.base_url and self.urls:
            for url in self.urls:
                if not url.startswith('http'):
                    urls.append(urljoin(self.base_url, url))
        
        return list(set(urls))  # Remove duplicates
    
    def _scrape_urls_from_pattern(self, pattern: str) -> List[str]:
        """Scrape PDF URLs from a webpage matching a pattern."""
        if not HAS_REQUESTS:
            raise ImportError(
                "requests library required for URL scraping. "
                "Install with: pip install requests"
            )
        if not HAS_BS4:
            raise ImportError(
                "beautifulsoup4 required for URL scraping. "
                "Install with: pip install beautifulsoup4"
            )
        
        # Extract base URL from pattern
        # Pattern like "https://ncert.nic.in/textbook/pdf/*.pdf"
        base_match = re.match(r'^(https?://[^/]+/[^*]+)', pattern)
        if not base_match:
            return []
        
        base_url = base_match.group(1)
        url_pattern_regex = pattern.replace('*', '.*').replace('.pdf', r'\.pdf')
        
        try:
            response = requests.get(base_url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            pdf_urls = []
            
            # Find all links ending in .pdf
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(base_url, href)
                
                if re.match(url_pattern_regex, full_url) or href.endswith('.pdf'):
                    pdf_urls.append(full_url)
            
            return list(set(pdf_urls))
        except Exception as e:
            import logging
            logging.getLogger("clean_corpus.sources.web_pdf").warning(
                f"Could not scrape URLs from {base_url}: {e}"
            )
            return []
    
    def _download_pdf(self, url: str) -> Optional[Path]:
        """Download a PDF from URL."""
        if not HAS_REQUESTS:
            raise ImportError(
                "requests library required for downloading PDFs. "
                "Install with: pip install requests"
            )
        
        # Generate filename from URL
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path) or f"document_{hash(url) % 10000}.pdf"
        if not filename.endswith('.pdf'):
            filename += '.pdf'
        
        file_path = Path(self.download_dir) / filename
        
        # Skip if already downloaded and resume is enabled
        if self.resume_download and file_path.exists():
            return file_path
        
        # Download with retries
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, timeout=self.timeout, stream=True)
                response.raise_for_status()
                
                # Verify it's a PDF
                content_type = response.headers.get('content-type', '').lower()
                if 'pdf' not in content_type and not url.endswith('.pdf'):
                    import logging
                    logging.getLogger("clean_corpus.sources.web_pdf").warning(
                        f"URL {url} does not appear to be a PDF (content-type: {content_type})"
                    )
                    return None
                
                # Save file
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                return file_path
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    import logging
                    logging.getLogger("clean_corpus.sources.web_pdf").error(
                        f"Failed to download {url} after {self.max_retries} attempts: {e}"
                    )
                    return None
        
        return None
    
    def _detect_language(self, text: str) -> Optional[str]:
        """Detect language from text."""
        if not self.auto_detect_language:
            return self.language
        
        if not HAS_LANGDETECT:
            return self.language  # Fallback to configured language
        
        try:
            from langdetect import detect, LangDetectException
            # Use first 1000 chars for detection (faster)
            sample = text[:1000] if len(text) > 1000 else text
            if len(sample.strip()) < 50:
                return self.language  # Too short to detect
            
            detected = detect(sample)
            # Ensure ISO 639-1 format (2 characters)
            if detected and len(detected) >= 2:
                return detected[:2].lower()
            return self.language
        except (LangDetectException, Exception):
            return self.language  # Fallback to configured language
    
    def _extract_metadata(self, pdf_path: Path, url: str, language: Optional[str]) -> Dict[str, Any]:
        """Extract metadata from PDF and URL."""
        metadata = {
            "source_url": url,
            "source_file": str(pdf_path),
            "download_timestamp": int(time.time()),
            **self.metadata_config  # User-provided metadata
        }
        
        # Try to extract PDF metadata
        try:
            if self.extractor_name == 'pymupdf':
                import fitz
                doc = fitz.open(str(pdf_path))
                pdf_meta = doc.metadata
                if pdf_meta.get('title'):
                    metadata['title'] = pdf_meta['title']
                if pdf_meta.get('author'):
                    metadata['author'] = pdf_meta['author']
                if pdf_meta.get('subject'):
                    metadata['subject'] = pdf_meta['subject']
                doc.close()
        except Exception:
            pass
        
        # Add language
        if language:
            metadata['language'] = language
        
        # Extract from filename/URL
        filename = pdf_path.stem
        metadata['filename'] = filename
        
        return metadata
    
    def metadata(self) -> Dict[str, Any]:
        """Return source metadata."""
        return {
            "kind": "web_pdf",
            "urls": self.pdf_urls,
            "url_count": len(self.pdf_urls),
            "download_dir": self.download_dir,
            "language": self.language,
            "auto_detect_language": self.auto_detect_language,
        }
    
    def stream(self) -> Iterable[RawDocument]:
        """Download PDFs and stream them as documents."""
        import logging
        log = logging.getLogger("clean_corpus.sources.web_pdf")
        
        log.info(f"Source {self.name}: Downloading {len(self.pdf_urls)} PDF(s) from web")
        
        downloaded_files = []
        
        # Download all PDFs
        for i, url in enumerate(self.pdf_urls, 1):
            log.info(f"Source {self.name}: Downloading PDF {i}/{len(self.pdf_urls)}: {url}")
            pdf_path = self._download_pdf(url)
            
            if pdf_path and pdf_path.exists():
                downloaded_files.append(pdf_path)
                log.info(f"Source {self.name}: Downloaded to {pdf_path}")
            else:
                log.warning(f"Source {self.name}: Failed to download {url}")
        
        if not downloaded_files:
            log.warning(f"Source {self.name}: No PDFs were downloaded")
            return
        
        log.info(f"Source {self.name}: Successfully downloaded {len(downloaded_files)} PDF(s)")
        
        # Process each downloaded PDF with language detection and metadata
        for pdf_path in downloaded_files:
            # Extract text sample for language detection
            language = self.language
            if self.auto_detect_language:
                try:
                    # Quick text extraction for language detection
                    if self.extractor_name == 'pymupdf':
                        import fitz
                        doc = fitz.open(str(pdf_path))
                        sample_text = ""
                        for page_num in range(min(3, len(doc))):  # First 3 pages
                            sample_text += doc[page_num].get_text()
                        doc.close()
                        language = self._detect_language(sample_text)
                except Exception:
                    pass
            
            # Find matching URL for this PDF
            url = ""
            for u in self.pdf_urls:
                parsed_url = urlparse(u)
                if pdf_path.name == os.path.basename(parsed_url.path) or pdf_path.name in u:
                    url = u
                    break
            
            pdf_metadata = self._extract_metadata(pdf_path, url, language)
            
            # Process PDF directly using extractor
            from .base import SourceSpec
            
            pdf_spec_single = SourceSpec(
                name=self.name,
                type="batch",
                kind="pdf",
                dataset=str(pdf_path),  # Single file
                chunk_mode=self.chunk_mode,
                extractor=self.extractor_name,
                min_text_length=self.global_pdf_config.get("min_text_length", 100),
                metadata_fields=self.global_pdf_config.get("metadata_fields", []),
                chunk_size=self.global_pdf_config.get("chunk_size", 1000),
                chunk_overlap=self.global_pdf_config.get("chunk_overlap", 200),
                schema=self.global_pdf_config.get("schema"),
            )
            
            # Import PDFSource here to avoid circular import
            from .pdf_source import PDFSource
            pdf_source_single = PDFSource(pdf_spec_single, global_pdf_config=self.global_pdf_config)
            
            # Stream documents from this PDF
            for raw_doc in pdf_source_single.stream():
                # Enhance metadata with web-specific info
                if raw_doc.extra:
                    raw_doc.extra.update(pdf_metadata)
                else:
                    raw_doc.extra = pdf_metadata
                
                # Add language to document
                if language:
                    raw_doc.extra['language'] = language
                    # Also add to main document fields if needed
                    if not raw_doc.license and self.metadata_config.get('license'):
                        raw_doc.license = self.metadata_config['license']
                
                # Set source_file to original URL for tracking
                raw_doc.source_file = url or str(pdf_path)
                
                # Set URL field
                if not raw_doc.url:
                    raw_doc.url = url
                
                yield raw_doc
