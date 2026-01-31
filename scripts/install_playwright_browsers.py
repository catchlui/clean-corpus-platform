#!/usr/bin/env python3
"""Helper script to install Playwright browsers.

This script helps install Playwright browsers on Windows where
the 'playwright' command might not be in PATH.

Usage:
    python scripts/install_playwright_browsers.py chromium
    python scripts/install_playwright_browsers.py firefox
    python scripts/install_playwright_browsers.py webkit
    python scripts/install_playwright_browsers.py all
"""

from __future__ import annotations
import sys
import subprocess

def check_playwright_installed():
    """Check if Playwright is installed."""
    try:
        import playwright
        return True
    except ImportError:
        return False

def install_browser(browser: str):
    """Install a Playwright browser."""
    try:
        print(f"\nInstalling {browser}...")
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", browser],
            check=True,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        print(f"✓ {browser} installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Error installing {browser}:")
        print(e.stderr)
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

def main():
    # Check if Playwright is installed first
    if not check_playwright_installed():
        print("Error: Playwright is not installed.")
        print("\nPlease install Playwright first:")
        print("  pip install playwright")
        print("\nThen run this script again:")
        print("  python scripts/install_playwright_browsers.py chromium")
        sys.exit(1)
    
    if len(sys.argv) < 2:
        print("Usage: python scripts/install_playwright_browsers.py <browser>")
        print("\nBrowsers:")
        print("  chromium  - Install Chromium")
        print("  firefox   - Install Firefox")
        print("  webkit    - Install WebKit")
        print("  all       - Install all browsers")
        print("\nExample:")
        print("  python scripts/install_playwright_browsers.py chromium")
        sys.exit(1)
    
    browser = sys.argv[1].lower()
    
    if browser == "all":
        browsers = ["chromium", "firefox", "webkit"]
        success = True
        for b in browsers:
            if not install_browser(b):
                success = False
        if success:
            print("\n✓ All browsers installed successfully")
        else:
            print("\n✗ Some browsers failed to install")
            sys.exit(1)
    elif browser in ["chromium", "firefox", "webkit"]:
        if not install_browser(browser):
            sys.exit(1)
    else:
        print(f"Error: Unknown browser '{browser}'")
        print("Supported: chromium, firefox, webkit, all")
        sys.exit(1)

if __name__ == "__main__":
    main()
