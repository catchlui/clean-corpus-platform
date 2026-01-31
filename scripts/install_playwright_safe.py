#!/usr/bin/env python3
"""Safe Playwright installation for Windows.

This script handles Windows file locking issues by:
1. Using --user flag (avoids system directory locks)
2. Checking for running processes
3. Providing fallback options

Usage:
    python scripts/install_playwright_safe.py
"""

from __future__ import annotations
import sys
import subprocess
import os

# Fix Windows Unicode encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

# Use ASCII-safe characters for Windows
OK = "[OK]"
FAIL = "[FAIL]"
WARN = "[WARN]"

def install_playwright_safe():
    """Install Playwright using the safest method for Windows."""
    print("="*60)
    print("Safe Playwright Installation (Windows)")
    print("="*60)
    
    # Method 1: Try --user flag (safest, avoids file locking)
    print("\nMethod 1: Installing to user site (safest)...")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--user", "playwright"],
            check=True,
            capture_output=True,
            text=True
        )
        print(f"{OK} Playwright installed successfully to user site")
        
        # Install browser
        print("\nInstalling Chromium browser...")
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            check=True
        )
        print(f"{OK} Chromium installed successfully")
        
        print("\n" + "="*60)
        print(f"{OK} Setup completed successfully!")
        print("="*60)
        print("\nYou can now use:")
        print("  python scripts/discover_ncert_pdfs_playwright.py --class 10 --language hi --output ncert.yaml")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"{FAIL} Installation failed: {e}")
        if e.stderr:
            print(e.stderr)
        
        # Check if it's a file locking error
        error_text = (e.stderr or "").lower()
        if "winerror" in error_text or "file specified" in error_text:
            print("\n" + "="*60)
            print(f"{WARN} Windows File Locking Issue Detected")
            print("="*60)
            print("\nRecommended solutions:")
            print("\n1. Close all Python processes and try again:")
            print("   taskkill /F /IM python.exe /T")
            print("   python scripts/install_playwright_safe.py")
            print("\n2. Use virtual environment (most reliable):")
            print("   python scripts/setup_playwright_venv.py")
            print("\n3. Use manual method (no installation needed):")
            print("   python scripts/manual_ncert_url_extractor.py")
            print("   (Copy URLs from browser, no Playwright needed)")
        
        return False
    except Exception as e:
        print(f"{FAIL} Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = install_playwright_safe()
    sys.exit(0 if success else 1)
