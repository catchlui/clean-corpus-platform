# Web PDF Downloader Guide

This guide explains how to download PDFs from websites (like NCERT) and process them into dataset format with automatic language detection and metadata extraction.

## Features

- **Download PDFs from URLs** - Single URL, list of URLs, or URL patterns
- **Automatic Language Detection** - Detects language from PDF content
- **Metadata Extraction** - Extracts title, author, language, source URL
- **Multi-language Support** - Handles documents in different languages
- **Resume Support** - Skips already downloaded files
- **Configurable** - Full control via YAML configuration

## Installation

Install required dependencies:

```bash
pip install requests beautifulsoup4 langdetect
# Or install all optional dependencies
pip install -e ".[web_pdf]"
```

## Configuration

### Basic Example: Download Specific PDFs

```yaml
run:
  run_id: "NCERT_2026-01-29"
  out_dir: "storage_ncert"
  shard_docs: 100
  checkpoint_every_docs: 50

execution:
  mode: local

pdf:
  chunk_mode: "page"
  extractor: "pymupdf"
  min_text_length: 200

sources:
  - name: "ncert_hindi"
    type: "batch"
    kind: "web_pdf"
    urls:
      - "https://ncert.nic.in/textbook/pdf/iehi101.pdf"
      - "https://ncert.nic.in/textbook/pdf/iehi102.pdf"
    download_dir: "downloads/ncert/hindi"
    language: "hi"  # Hindi (ISO 639-1)
    auto_detect_language: true
    metadata:
      source: "NCERT"
      license: "CC-BY-NC"
      category: "textbook"
      subject: "Hindi"
```

### Multi-language Example

```yaml
sources:
  # Hindi NCERT books
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
      language: "hi"
  
  # English NCERT books
  - name: "ncert_english"
    kind: "web_pdf"
    urls:
      - "https://ncert.nic.in/textbook/pdf/ieen101.pdf"
    download_dir: "downloads/ncert/english"
    language: "en"
    auto_detect_language: true
    metadata:
      source: "NCERT"
      license: "CC-BY-NC"
      language: "en"
  
  # Tamil NCERT books
  - name: "ncert_tamil"
    kind: "web_pdf"
    urls:
      - "https://ncert.nic.in/textbook/pdf/ieta101.pdf"
    download_dir: "downloads/ncert/tamil"
    language: "ta"
    auto_detect_language: true
    metadata:
      source: "NCERT"
      license: "CC-BY-NC"
      language: "ta"
```

### URL Pattern Scraping

Download all PDFs matching a pattern from a webpage:

```yaml
sources:
  - name: "ncert_all"
    kind: "web_pdf"
    url_pattern: "https://ncert.nic.in/textbook/pdf/*.pdf"
    base_url: "https://ncert.nic.in/textbook/"
    download_dir: "downloads/ncert/all"
    auto_detect_language: true
    metadata:
      source: "NCERT"
      license: "CC-BY-NC"
```

## Configuration Options

### Source Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `urls` | List[str] | [] | List of PDF URLs to download |
| `url_pattern` | str | None | URL pattern to scrape (e.g., "https://site.com/pdf/*.pdf") |
| `base_url` | str | None | Base URL for relative URLs |
| `download_dir` | str | `downloads/{name}` | Directory to download PDFs to |
| `language` | str | None | ISO 639-1 language code (en, hi, ta, etc.) |
| `auto_detect_language` | bool | true | Automatically detect language from PDF content |
| `resume_download` | bool | true | Skip already downloaded files |
| `timeout` | int | 30 | Download timeout in seconds |
| `max_retries` | int | 3 | Maximum download retries |
| `metadata` | dict | {} | Additional metadata to add to documents |

### Language Codes (ISO 639-1)

Common language codes:
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
- `or` - Odia
- `as` - Assamese

## How It Works

1. **Download Phase:**
   - Downloads PDFs from specified URLs
   - Saves to `download_dir`
   - Skips already downloaded files (if `resume_download: true`)

2. **Language Detection:**
   - If `auto_detect_language: true`, analyzes PDF content
   - Falls back to `language` setting if detection fails
   - Language stored in document metadata and `lang` field

3. **Processing Phase:**
   - Uses PDF source to extract text
   - Applies chunking strategy (page/document/fixed_size)
   - Extracts metadata (title, author, etc.)

4. **Metadata Enhancement:**
   - Adds web-specific metadata (source_url, download_timestamp)
   - Adds user-provided metadata from config
   - Adds detected language

## Output Format

Each document includes:

- **Text**: Extracted PDF content
- **Language**: Detected or configured language (ISO 639-1 code)
- **Metadata**:
  - `source_url`: Original PDF URL
  - `source_file`: Local downloaded file path
  - `language`: Detected/configured language
  - `title`: PDF title (if available)
  - `author`: PDF author (if available)
  - `download_timestamp`: When PDF was downloaded
  - Custom metadata from config

## Example: NCERT Books

### Step 1: Create Configuration

Create `build_ncert.yaml`:

```yaml
run:
  run_id: "NCERT_MultiLang_2026-01-29"
  out_dir: "storage_ncert"
  shard_docs: 100
  checkpoint_every_docs: 50

execution:
  mode: local

pdf:
  chunk_mode: "page"
  extractor: "pymupdf"
  min_text_length: 200
  metadata_fields: ["title", "author", "page_number", "language"]

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
      publisher: "NCERT"

policies:
  licenses: "src/clean_corpus/policies/defaults/licenses.yaml"
  quality: "src/clean_corpus/policies/defaults/quality.yaml"
  pii: "src/clean_corpus/policies/defaults/pii.yaml"
  curriculum: "src/clean_corpus/policies/defaults/curriculum.yaml"

stages:
  - license_gate
  - sanitize
  - quality_gate
  - pii_policy_gate
  - curriculum_eligibility
```

### Step 2: Run Pipeline

```bash
python -m clean_corpus.cli build --config build_ncert.yaml
```

### Step 3: Check Output

```bash
# View downloaded PDFs
dir downloads\ncert\hindi

# View processed documents
dir storage_ncert\docs

# View metadata
dir storage_ncert\metadata
```

## Language Detection

The system automatically detects language from PDF content:

1. **Extracts sample text** from first few pages
2. **Uses langdetect library** to identify language
3. **Falls back** to configured `language` if detection fails
4. **Stores** language in both `lang` field and metadata

### Manual Language Override

To force a specific language without detection:

```yaml
sources:
  - name: "ncert_hindi"
    kind: "web_pdf"
    urls: [...]
    language: "hi"
    auto_detect_language: false  # Use configured language only
```

## Metadata Fields

### Automatic Metadata

- `source_url`: Original PDF URL
- `source_file`: Local file path
- `download_timestamp`: Unix timestamp
- `filename`: PDF filename
- `language`: Detected/configured language
- `title`: PDF title (if available)
- `author`: PDF author (if available)
- `subject`: PDF subject (if available)

### Custom Metadata

Add custom fields via `metadata` config:

```yaml
metadata:
  source: "NCERT"
  license: "CC-BY-NC"
  category: "textbook"
  subject: "Mathematics"
  grade: "10"
  publisher: "NCERT"
```

All custom metadata is included in document `extra` field and available in output.

## Troubleshooting

### Download Fails

**Problem**: PDF download fails or times out.

**Solutions:**
1. Increase timeout:
   ```yaml
   timeout: 60  # Increase from default 30
   ```
2. Increase retries:
   ```yaml
   max_retries: 5  # Increase from default 3
   ```
3. Check URL accessibility manually

### Language Detection Fails

**Problem**: Wrong language detected or detection fails.

**Solutions:**
1. Set explicit language:
   ```yaml
   language: "hi"
   auto_detect_language: false
   ```
2. Check PDF content quality (may be too short or corrupted)

### URL Pattern Not Working

**Problem**: URL pattern scraping doesn't find PDFs.

**Solutions:**
1. Verify base URL is accessible
2. Check if webpage structure matches expected format
3. Use direct URL list instead:
   ```yaml
   urls:
     - "https://site.com/pdf1.pdf"
     - "https://site.com/pdf2.pdf"
   ```

## Best Practices

1. **Organize by Language**: Use separate sources for different languages
2. **Set Explicit Language**: For known languages, set `language` explicitly
3. **Use Resume**: Keep `resume_download: true` to avoid re-downloading
4. **Add Metadata**: Include source, license, and category in metadata
5. **Test Small First**: Test with 1-2 PDFs before processing large batches

## See Also

- `examples/build_web_pdf_ncert.yaml` - Complete NCERT example
- `examples/build_pdf.yaml` - Local PDF processing example
- `docs/HOW_TO_ADD_FILES.md` - General file processing guide
