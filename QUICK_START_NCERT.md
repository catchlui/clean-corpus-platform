# Quick Start: NCERT PDF Download with Playwright

Quick guide to download and process NCERT textbooks using Playwright.

## Installation

### Windows (PowerShell) - Easiest Method (Fixed for File Locking)

```powershell
# Safe installation (handles Windows file locking)
python scripts/install_playwright_safe.py

# Or use the updated setup script
python scripts/setup_playwright.py

# Or install specific browser
python scripts/setup_playwright.py chromium
```

### Windows (PowerShell) - Manual Method

```powershell
# Step 1: Install Playwright package
pip install playwright

# Step 2: Install browser (use python -m playwright on Windows)
python -m playwright install chromium

# Or use helper script (after installing playwright)
python scripts/install_playwright_browsers.py chromium
```

### Linux/Mac

```bash
# Install Playwright
pip install playwright
playwright install chromium  # Downloads browser automatically
```

## Quick Steps

### 1. Discover PDF URLs

```bash
# Discover PDFs for Class 10 Hindi
python scripts/discover_ncert_pdfs_playwright.py \
  --class 10 \
  --language hi \
  --output ncert_hindi_10.yaml

# Use Firefox if Chromium doesn't work
python scripts/discover_ncert_pdfs_playwright.py \
  --class 10 \
  --language hi \
  --output ncert_hindi_10.yaml \
  --browser firefox

# Show browser window (for debugging)
python scripts/discover_ncert_pdfs_playwright.py \
  --class 10 \
  --language hi \
  --output ncert_hindi_10.yaml \
  --no-headless
```

### 2. Review Generated Config

```bash
# Check the generated YAML file
cat ncert_hindi_10.yaml
```

### 3. Run Pipeline

```bash
python -m clean_corpus.cli build --config ncert_hindi_10.yaml
```

### 4. Monitor Progress

```bash
python -m clean_corpus.monitor.unified_app storage_ncert --refresh 5.0
```

## Alternative: Manual Method (More Reliable)

If Playwright doesn't find URLs, use manual method:

1. **Visit NCERT website:**
   ```
   https://ncert.nic.in/textbook.php?bhsr1=0-26
   ```
   (Change `26` to your class: 1-12)

2. **Copy PDF URLs:**
   - Select class → subject → book
   - Right-click PDF link → "Copy link address"
   - Save URLs to `urls.txt` (one per line)

3. **Generate config:**
   ```bash
   python scripts/manual_ncert_url_extractor.py \
     --file urls.txt \
     --class 10 \
     --language hi \
     --output ncert_hindi_10.yaml
   ```

4. **Run pipeline:**
   ```bash
   python -m clean_corpus.cli build --config ncert_hindi_10.yaml
   ```

## Troubleshooting

### Playwright Browser Not Found

```bash
python -m playwright install chromium
# Or
python -m playwright install firefox
```

### Page Loads Slowly

```bash
# Increase wait time
python scripts/discover_ncert_pdfs_playwright.py \
  --class 10 \
  --language hi \
  --output ncert_hindi_10.yaml \
  --wait-time 10
```

### No PDFs Found

1. Try different browser: `--browser firefox`
2. Show browser window: `--no-headless` (to see what's happening)
3. Use manual method instead

## See Also

- `docs/NCERT_DOWNLOAD_GUIDE.md` - Complete guide
- `docs/NCERT_JAVASCRIPT_SOLUTION.md` - JavaScript solutions
- `docs/WEB_PDF_DOWNLOADER.md` - General web PDF guide
