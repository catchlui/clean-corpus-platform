# Web PDF Downloader Feature Summary

## Overview

Added comprehensive web PDF downloader functionality with automatic language detection and metadata extraction. This allows downloading PDFs from websites (like NCERT) and processing them into dataset format with full language support.

## What Was Added

### 1. Web PDF Source (`src/clean_corpus/sources/web_pdf.py`)

New source type `web_pdf` that:
- Downloads PDFs from URLs (single, list, or patterns)
- Automatically detects language from PDF content
- Extracts metadata (title, author, language, source URL)
- Supports multiple languages
- Resumes downloads (skips already downloaded files)

### 2. Language Support

- **Automatic Detection**: Uses `langdetect` library to detect language from PDF content
- **Manual Override**: Can specify language explicitly via config
- **ISO 639-1 Codes**: Supports standard language codes (en, hi, ta, etc.)
- **Metadata Integration**: Language stored in both `lang` field and document metadata

### 3. Enhanced Document Processing

- Updated `_raw_to_doc` to extract language from `raw.extra`
- Language properly propagated to Document `lang` field
- Metadata includes language information

### 4. Configuration Schema

Added to `SourceSpec`:
- `urls`: List of PDF URLs
- `url_pattern`: Pattern to scrape PDFs from webpage
- `base_url`: Base URL for relative URLs
- `download_dir`: Where to save downloaded PDFs
- `language`: ISO 639-1 language code
- `auto_detect_language`: Enable/disable language detection
- `metadata`: Custom metadata dictionary

### 5. Example Configurations

- `examples/build_web_pdf_ncert.yaml` - Complete NCERT example with multiple languages

### 6. Helper Scripts

- `scripts/download_web_pdfs.py` - Standalone PDF downloader

### 7. Documentation

- `docs/WEB_PDF_DOWNLOADER.md` - Complete guide with examples

## Usage Example

### Basic Configuration

```yaml
sources:
  - name: "ncert_hindi"
    kind: "web_pdf"
    urls:
      - "https://ncert.nic.in/textbook/pdf/iehi101.pdf"
    download_dir: "downloads/ncert/hindi"
    language: "hi"
    auto_detect_language: true
    metadata:
      source: "NCERT"
      license: "CC-BY-NC"
      category: "textbook"
```

### Multi-language Example

```yaml
sources:
  - name: "ncert_hindi"
    kind: "web_pdf"
    urls: [...]
    language: "hi"
    metadata:
      source: "NCERT"
      language: "hi"
  
  - name: "ncert_english"
    kind: "web_pdf"
    urls: [...]
    language: "en"
    metadata:
      source: "NCERT"
      language: "en"
  
  - name: "ncert_tamil"
    kind: "web_pdf"
    urls: [...]
    language: "ta"
    metadata:
      source: "NCERT"
      language: "ta"
```

## Installation

Install required dependencies:

```bash
pip install requests beautifulsoup4 langdetect
# Or
pip install -e ".[web_pdf]"
```

## Features

✅ **Download from URLs** - Single URL, list, or patterns  
✅ **Language Detection** - Automatic detection from PDF content  
✅ **Multi-language** - Support for Hindi, Tamil, English, and more  
✅ **Metadata Extraction** - Title, author, language, source URL  
✅ **Resume Support** - Skip already downloaded files  
✅ **Configurable** - Full control via YAML  

## Output

Each document includes:
- **Text**: Extracted PDF content
- **Language**: Detected/configured language (ISO 639-1)
- **Metadata**:
  - `source_url`: Original PDF URL
  - `language`: Detected language
  - `title`, `author`: PDF metadata
  - Custom metadata from config

## Files Created/Modified

### New Files
- `src/clean_corpus/sources/web_pdf.py` - Web PDF source implementation
- `examples/build_web_pdf_ncert.yaml` - NCERT example config
- `docs/WEB_PDF_DOWNLOADER.md` - Complete documentation
- `scripts/download_web_pdfs.py` - Standalone downloader script
- `WEB_PDF_FEATURE_SUMMARY.md` - This file

### Modified Files
- `src/clean_corpus/sources/base.py` - Added web_pdf fields to SourceSpec
- `src/clean_corpus/sources/registry.py` - Registered web_pdf source
- `src/clean_corpus/pipeline/build.py` - Enhanced language extraction
- `pyproject.toml` - Added web_pdf optional dependencies
- `README.md` - Updated with web PDF features

## Next Steps

1. **Install dependencies:**
   ```bash
   pip install requests beautifulsoup4 langdetect
   ```

2. **Test with example:**
   ```bash
   python -m clean_corpus.cli build --config examples/build_web_pdf_ncert.yaml
   ```

3. **Customize for your use case:**
   - Update URLs in config
   - Set appropriate language codes
   - Add custom metadata

## Language Codes Reference

Common ISO 639-1 codes:
- `en` - English
- `hi` - Hindi
- `ta` - Tamil
- `te` - Telugu
- `kn` - Kannada
- `ml` - Malayalam
- `mr` - Marathi
- `gu` - Gujarati
- `bn` - Bengali
- `pa` - Punjabi
- `ur` - Urdu

See `docs/WEB_PDF_DOWNLOADER.md` for complete guide.
