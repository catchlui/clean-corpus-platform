# NCERT PDF Download Guide

This guide explains how to download and process NCERT textbooks using the web PDF downloader.

## Finding NCERT PDF URLs

NCERT provides textbooks through their website at: https://ncert.nic.in/textbook.php

**Important:** NCERT website uses JavaScript to load content dynamically, so simple HTML scraping doesn't work. Use one of these methods:

### Method 1: Manual Discovery (Recommended)

1. **Visit NCERT Textbook Portal:**
   ```
   https://ncert.nic.in/textbook.php?bhsr1=0-26
   ```
   (Replace `26` with your class number: 1-12)

2. **Select Class and Subject:**
   - Use the dropdown menus to select:
     - Class (I-XII)
     - Subject (Hindi, English, Mathematics, etc.)
     - Book Title

3. **Get PDF URL:**
   - Click on a book title
   - The PDF will open in browser
   - Copy the PDF URL from address bar
   - Example: `https://ncert.nic.in/textbook/pdf/iehi101.pdf`

4. **Add to Configuration:**
   ```yaml
   sources:
     - name: "ncert_hindi"
       kind: "web_pdf"
       urls:
         - "https://ncert.nic.in/textbook/pdf/iehi101.pdf"
         - "https://ncert.nic.in/textbook/pdf/iehi102.pdf"
       download_dir: "downloads/ncert/hindi"
       language: "hi"
   ```

### Method 2: Using Discovery Script

Use the helper script to discover PDF URLs:

```bash
# Discover PDFs for Class 10 Hindi
python scripts/discover_ncert_pdfs.py --class 10 --subject hindi --output ncert_urls.yaml --language hi

# Discover PDFs for Class 12 Mathematics
python scripts/discover_ncert_pdfs.py --class 12 --subject mathematics --output ncert_urls.yaml --language en
```

This will generate a YAML file with discovered URLs that you can use directly or merge into your config.

## NCERT URL Patterns

NCERT PDF URLs typically follow these patterns:

- **Hindi**: `https://ncert.nic.in/textbook/pdf/iehi[class][book].pdf`
- **English**: `https://ncert.nic.in/textbook/pdf/ieen[class][book].pdf`
- **Mathematics**: `https://ncert.nic.in/textbook/pdf/iemh[class][book].pdf`
- **Science**: `https://ncert.nic.in/textbook/pdf/iesc[class][book].pdf`

**Note:** Actual URLs may vary. Always verify by visiting the NCERT website.

## Complete Example Configuration

### Single Language (Hindi)

```yaml
run:
  run_id: "NCERT_Hindi_2026-01-29"
  out_dir: "storage_ncert_hindi"
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
  - name: "ncert_hindi_class10"
    kind: "web_pdf"
    urls:
      - "https://ncert.nic.in/textbook/pdf/iehi101.pdf"
      - "https://ncert.nic.in/textbook/pdf/iehi102.pdf"
    download_dir: "downloads/ncert/hindi/class10"
    language: "hi"
    auto_detect_language: true
    metadata:
      source: "NCERT"
      license: "CC-BY-NC"
      category: "textbook"
      subject: "Hindi"
      class: "10"
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

### Multiple Languages

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
      subject: "Hindi"
  
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
      subject: "English"
  
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
      subject: "Tamil"
```

## Running the Pipeline

### Step 1: Install Dependencies

```bash
pip install requests beautifulsoup4 langdetect pymupdf
```

### Step 2: Create Configuration

Create `build_ncert.yaml` with your NCERT PDF URLs (see examples above).

### Step 3: Run Pipeline

```bash
python -m clean_corpus.cli build --config build_ncert.yaml
```

### Step 4: Monitor Progress

In another terminal:

```bash
python -m clean_corpus.monitor.unified_app storage_ncert_hindi --refresh 5.0
```

## Language Codes for NCERT

Common languages used in NCERT textbooks:

| Language | Code | Example URL Pattern |
|----------|------|-------------------|
| Hindi | `hi` | `iehi*.pdf` |
| English | `en` | `ieen*.pdf` |
| Tamil | `ta` | `ieta*.pdf` |
| Telugu | `te` | `iete*.pdf` |
| Kannada | `kn` | `ieka*.pdf` |
| Malayalam | `ml` | `ieml*.pdf` |
| Marathi | `mr` | `iemr*.pdf` |
| Gujarati | `gu` | `iegu*.pdf` |
| Bengali | `bn` | `iebn*.pdf` |
| Punjabi | `pa` | `iepa*.pdf` |
| Urdu | `ur` | `ieur*.pdf` |
| Odia | `or` | `ieor*.pdf` |
| Assamese | `as` | `ieas*.pdf` |
| Sanskrit | `sa` | `iesa*.pdf` |

## Troubleshooting

### PDF URLs Not Found

**Problem:** Discovery script doesn't find PDFs.

**Solutions:**
1. **NCERT website uses JavaScript** - Simple HTML scraping won't work
2. **Use manual method** (recommended):
   - Visit https://ncert.nic.in/textbook.php
   - Select class → subject → book
   - Copy PDF URL from browser
   - Use `manual_ncert_url_extractor.py` to create config
3. **Use Selenium** if you need automation (requires browser driver)
4. Check NCERT website structure - it may have changed

### JavaScript-Heavy Website

**Problem:** Website requires JavaScript to load content.

**Solutions:**
1. Use manual method: Copy URLs from browser
2. Use Playwright-based script: `discover_ncert_pdfs_playwright.py` (recommended)
3. Use Selenium-based script: `discover_ncert_pdfs_selenium.py` (legacy)
3. Use browser developer tools:
   - Open browser DevTools (F12)
   - Go to Network tab
   - Filter by "pdf"
   - Load NCERT page and click books
   - Copy PDF URLs from network requests

### Download Fails

**Problem:** PDF download fails or times out.

**Solutions:**
1. Verify URL is accessible in browser
2. Increase timeout:
   ```yaml
   timeout: 60
   ```
3. Check network connectivity
4. Some NCERT PDFs may require authentication or have rate limits

### Wrong Language Detected

**Problem:** Language detection identifies wrong language.

**Solutions:**
1. Set explicit language:
   ```yaml
   language: "hi"
   auto_detect_language: false
   ```
2. Check PDF content quality
3. Some PDFs may have mixed languages (e.g., Hindi + English)

## Best Practices

1. **Organize by Class and Language:**
   ```yaml
   download_dir: "downloads/ncert/hindi/class10"
   ```

2. **Use Descriptive Source Names:**
   ```yaml
   name: "ncert_hindi_class10"
   ```

3. **Set Explicit Language:**
   ```yaml
   language: "hi"
   auto_detect_language: true  # Still verify
   ```

4. **Add Complete Metadata:**
   ```yaml
   metadata:
     source: "NCERT"
     license: "CC-BY-NC"
     category: "textbook"
     subject: "Hindi"
     class: "10"
     publisher: "NCERT"
   ```

5. **Test with Small Batch First:**
   - Start with 1-2 PDFs
   - Verify output format and language
   - Then process full batch

## Example Workflow

```bash
# 1. Discover PDFs for Class 10 Hindi
python scripts/discover_ncert_pdfs.py --class 10 --subject hindi --output ncert_hindi_10.yaml --language hi

# 2. Review generated YAML and update if needed
# Edit ncert_hindi_10.yaml to add/remove URLs

# 3. Run pipeline
python -m clean_corpus.cli build --config ncert_hindi_10.yaml

# 4. Monitor progress
python -m clean_corpus.monitor.unified_app storage_ncert_hindi --refresh 5.0

# 5. Check output
dir storage_ncert_hindi\docs
dir storage_ncert_hindi\metadata
```

## See Also

- `docs/WEB_PDF_DOWNLOADER.md` - General web PDF downloader guide
- `examples/build_web_pdf_ncert.yaml` - Complete NCERT example
- `scripts/discover_ncert_pdfs.py` - PDF discovery helper
- `scripts/download_web_pdfs.py` - Standalone downloader
