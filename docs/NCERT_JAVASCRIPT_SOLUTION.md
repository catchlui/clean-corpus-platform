# NCERT JavaScript Website - Solutions

NCERT website (https://ncert.nic.in/textbook.php) uses JavaScript to dynamically load content, so simple HTML scraping doesn't work. Here are practical solutions:

## Solution 1: Manual URL Extraction (Easiest & Recommended)

This is the most reliable method for JavaScript-heavy sites.

### Step-by-Step:

1. **Visit NCERT Website:**
   ```
   https://ncert.nic.in/textbook.php?bhsr1=0-26
   ```
   (Change `26` to your class number: 1-12)

2. **Copy PDF URLs:**
   - Select class → subject → book from dropdowns
   - Right-click on PDF link → "Copy link address"
   - Or click PDF → copy URL from browser address bar
   - Repeat for all books you need

3. **Use Manual Extractor Script:**
   ```bash
   # Option A: Paste URLs directly
   python scripts/manual_ncert_url_extractor.py \
     --urls "https://ncert.nic.in/textbook/pdf/iehi101.pdf https://ncert.nic.in/textbook/pdf/iehi102.pdf" \
     --class 10 \
     --language hi \
     --output ncert_hindi.yaml
   
   # Option B: Save URLs to file (one per line)
   # Create urls.txt:
   # https://ncert.nic.in/textbook/pdf/iehi101.pdf
   # https://ncert.nic.in/textbook/pdf/iehi102.pdf
   
   python scripts/manual_ncert_url_extractor.py \
     --file urls.txt \
     --class 10 \
     --language hi \
     --output ncert_hindi.yaml
   ```

4. **Run Pipeline:**
   ```bash
   python -m clean_corpus.cli build --config ncert_hindi.yaml
   ```

## Solution 2: Browser Developer Tools

Extract URLs from browser network requests:

1. **Open Browser DevTools:**
   - Press `F12` or right-click → "Inspect"
   - Go to "Network" tab
   - Filter by "pdf" or "textbook"

2. **Load NCERT Page:**
   - Visit https://ncert.nic.in/textbook.php?bhsr1=0-26
   - Select class → subject → book
   - Watch network tab for PDF requests

3. **Copy PDF URLs:**
   - Find PDF requests in network tab
   - Right-click → "Copy" → "Copy URL"
   - Save all URLs to a text file

4. **Use Manual Extractor:**
   ```bash
   python scripts/manual_ncert_url_extractor.py \
     --file urls.txt \
     --class 10 \
     --language hi \
     --output ncert_hindi.yaml
   ```

## Solution 3: Playwright (Automated - Recommended)

For automated discovery with JavaScript support. Playwright is more reliable than Selenium for modern JS sites.

### Installation:

**Windows (PowerShell) - Easiest:**
```powershell
# One command to install everything
python scripts/setup_playwright.py
```

**Windows (PowerShell) - Manual:**
```powershell
pip install playwright
python -m playwright install chromium  # Use python -m playwright on Windows
# Or use helper script (after installing playwright):
python scripts/install_playwright_browsers.py chromium
```

**Linux/Mac:**
```bash
pip install playwright
playwright install chromium  # Or: playwright install firefox
```

### Usage:

```bash
# Discover PDFs automatically (Chromium)
python scripts/discover_ncert_pdfs_playwright.py \
  --class 10 \
  --language hi \
  --output ncert_urls.yaml

# Use Firefox instead
python scripts/discover_ncert_pdfs_playwright.py \
  --class 10 \
  --language hi \
  --output ncert_urls.yaml \
  --browser firefox

# Show browser window (for debugging)
python scripts/discover_ncert_pdfs_playwright.py \
  --class 10 \
  --language hi \
  --output ncert_urls.yaml \
  --no-headless

# Increase wait time if page loads slowly
python scripts/discover_ncert_pdfs_playwright.py \
  --class 10 \
  --language hi \
  --output ncert_urls.yaml \
  --wait-time 10
```

**Note:** Playwright automatically downloads browser binaries on first run. More reliable than Selenium for modern JavaScript sites.

### Troubleshooting Playwright:

- **Browser not installed:**
  ```bash
  python -m playwright install chromium
  # Or: python -m playwright install firefox
  ```

- **Page loads slowly:**
  ```bash
  # Increase wait time
  --wait-time 10
  ```

- **Try different browser:**
  ```bash
  --browser firefox  # Or webkit
  ```

## Solution 4: Selenium (Alternative - Legacy)

For automated discovery with Selenium (use Playwright instead):

```bash
pip install selenium webdriver-manager
python scripts/discover_ncert_pdfs_selenium.py --class 10 --language hi --output ncert_urls.yaml
```

## Solution 4: Direct URL Patterns

If you know NCERT's URL pattern, you can construct URLs directly:

```yaml
sources:
  - name: "ncert_hindi"
    kind: "web_pdf"
    urls:
      # Pattern: https://ncert.nic.in/textbook/pdf/[bookcode].pdf
      # Hindi: iehi[class][book].pdf
      - "https://ncert.nic.in/textbook/pdf/iehi101.pdf"
      - "https://ncert.nic.in/textbook/pdf/iehi102.pdf"
      # English: ieen[class][book].pdf
      - "https://ncert.nic.in/textbook/pdf/ieen101.pdf"
    download_dir: "downloads/ncert/hindi"
    language: "hi"
```

**Note:** You'll need to know the book codes, which can be found by inspecting URLs from the website.

## Quick Reference

### Manual Method (Best for JavaScript Sites)

```bash
# 1. Copy URLs from browser
# 2. Create urls.txt file
# 3. Run extractor
python scripts/manual_ncert_url_extractor.py \
  --file urls.txt \
  --class 10 \
  --language hi \
  --output ncert.yaml

# 4. Run pipeline
python -m clean_corpus.cli build --config ncert.yaml
```

### Playwright Method (Automated - Recommended)

```bash
# 1. Install dependencies
pip install playwright
playwright install chromium

# 2. Discover URLs
python scripts/discover_ncert_pdfs_playwright.py \
  --class 10 \
  --language hi \
  --output ncert.yaml

# 3. Run pipeline
python -m clean_corpus.cli build --config ncert.yaml
```

### Selenium Method (Alternative - Legacy)

```bash
# 1. Install dependencies
pip install selenium webdriver-manager

# 2. Discover URLs
python scripts/discover_ncert_pdfs_selenium.py \
  --class 10 \
  --language hi \
  --output ncert.yaml

# 3. Run pipeline
python -m clean_corpus.cli build --config ncert.yaml
```

## Why Manual Method is Recommended

1. **Reliable:** Works even if website structure changes
2. **No Dependencies:** Doesn't require Selenium/browser drivers
3. **Fast:** No browser automation overhead
4. **Accurate:** You verify URLs before processing
5. **Works Everywhere:** No browser compatibility issues

## Example Workflow

```bash
# Step 1: Visit NCERT website and copy 5 PDF URLs
# Save to urls.txt:
# https://ncert.nic.in/textbook/pdf/iehi101.pdf
# https://ncert.nic.in/textbook/pdf/iehi102.pdf
# https://ncert.nic.in/textbook/pdf/iehi103.pdf
# https://ncert.nic.in/textbook/pdf/iehi104.pdf
# https://ncert.nic.in/textbook/pdf/iehi105.pdf

# Step 2: Generate config
python scripts/manual_ncert_url_extractor.py \
  --file urls.txt \
  --class 10 \
  --language hi \
  --subject "Hindi" \
  --output ncert_hindi_10.yaml

# Step 3: Review config
cat ncert_hindi_10.yaml

# Step 4: Run pipeline
python -m clean_corpus.cli build --config ncert_hindi_10.yaml

# Step 5: Monitor
python -m clean_corpus.monitor.unified_app storage_ncert --refresh 5.0
```

## See Also

- `docs/NCERT_DOWNLOAD_GUIDE.md` - Complete NCERT guide
- `scripts/manual_ncert_url_extractor.py` - Manual URL extractor (recommended)
- `scripts/discover_ncert_pdfs_playwright.py` - Playwright-based discovery (automated)
- `scripts/discover_ncert_pdfs_selenium.py` - Selenium-based discovery (legacy)
- `docs/WEB_PDF_DOWNLOADER.md` - General web PDF guide
