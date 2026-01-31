# Windows Playwright Installation Troubleshooting

Common issues when installing Playwright on Windows and their solutions.

## Error: "Could not install packages due to an OSError: [WinError 2]"

This is a Windows file locking issue. The `playwright.exe` script is locked or in use.

### Solution 1: Close Python Processes (Quick Fix)

```powershell
# Close all Python processes
taskkill /F /IM python.exe /T

# Then try again
pip install playwright
python -m playwright install chromium
```

### Solution 2: Use --user Flag (Avoids Permissions)

```powershell
# Install to user site (doesn't require admin)
pip install --user playwright
python -m playwright install chromium
```

### Solution 3: Use Virtual Environment (Recommended)

```powershell
# Create virtual environment
python -m venv .venv

# Activate it
.venv\Scripts\activate

# Install Playwright
pip install playwright
python -m playwright install chromium
```

Or use the helper script:

```powershell
python scripts/setup_playwright_venv.py
```

### Solution 4: Run as Administrator

1. Right-click PowerShell
2. Select "Run as Administrator"
3. Run installation commands

### Solution 5: Use Setup Script with Error Handling

```powershell
# The setup script tries multiple methods automatically
python scripts/setup_playwright.py
```

## Error: "No module named playwright"

Playwright package is not installed.

### Solution:

```powershell
pip install playwright
# Or with --user flag
pip install --user playwright
```

## Error: "playwright: command not found" (Windows)

On Windows, use `python -m playwright` instead of `playwright`.

### Solution:

```powershell
# Wrong (Linux/Mac):
playwright install chromium

# Correct (Windows):
python -m playwright install chromium

# Or use helper script:
python scripts/install_playwright_browsers.py chromium
```

## Error: Browser Not Found After Installation

Browser binaries weren't downloaded.

### Solution:

```powershell
# Install browser explicitly
python -m playwright install chromium

# Or use helper script
python scripts/install_playwright_browsers.py chromium

# Or use setup script
python scripts/setup_playwright.py
```

## Best Practices for Windows

1. **Use Virtual Environment:**
   ```powershell
   python -m venv .venv
   .venv\Scripts\activate
   pip install playwright
   python -m playwright install chromium
   ```

2. **Use Helper Scripts:**
   ```powershell
   # Complete setup
   python scripts/setup_playwright.py
   
   # Or virtual environment setup
   python scripts/setup_playwright_venv.py
   ```

3. **Close Python Processes Before Installing:**
   ```powershell
   taskkill /F /IM python.exe /T
   ```

4. **Use --user Flag if Permission Issues:**
   ```powershell
   pip install --user playwright
   ```

## Quick Reference

### Complete Setup (Easiest)

```powershell
# Method 1: Use setup script
python scripts/setup_playwright.py

# Method 2: Virtual environment (recommended)
python scripts/setup_playwright_venv.py
```

### Manual Setup

```powershell
# Step 1: Install package
pip install playwright
# Or: pip install --user playwright

# Step 2: Install browser
python -m playwright install chromium
# Or: python scripts/install_playwright_browsers.py chromium
```

### Verify Installation

```powershell
# Check if Playwright is installed
python -c "import playwright; print('OK')"

# Check if browser is installed
python -m playwright install --help
```

## Still Having Issues?

1. **Check Python Version:**
   ```powershell
   python --version  # Should be 3.8+
   ```

2. **Check pip Version:**
   ```powershell
   pip --version
   pip install --upgrade pip
   ```

3. **Try Different Installation Method:**
   - Virtual environment (most reliable)
   - --user flag (avoids permissions)
   - Administrator mode (if needed)

4. **Check Antivirus:**
   - Temporarily disable antivirus
   - Add Python/Scripts folder to exclusions

5. **Use Manual Method Instead:**
   - Skip Playwright automation
   - Use `scripts/manual_ncert_url_extractor.py` instead
   - Copy URLs from browser manually

## See Also

- `QUICK_START_NCERT.md` - Quick start guide
- `docs/NCERT_JAVASCRIPT_SOLUTION.md` - JavaScript solutions
- `scripts/setup_playwright.py` - Setup script
- `scripts/setup_playwright_venv.py` - Virtual environment setup
