#!/usr/bin/env python3
"""Setup Playwright in a virtual environment (recommended for Windows).

This script creates a virtual environment and installs Playwright there,
avoiding Windows file locking issues.

Usage:
    python scripts/setup_playwright_venv.py
"""

from __future__ import annotations
import sys
import os
import subprocess
from pathlib import Path

VENV_DIR = Path(".venv_playwright")
VENV_PYTHON = VENV_DIR / "Scripts" / "python.exe" if sys.platform == "win32" else VENV_DIR / "bin" / "python"

def run_command(cmd: list[str], description: str, cwd: Path = None) -> bool:
    """Run a command and return success status."""
    print(f"\n{description}...")
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            cwd=cwd
        )
        if result.stdout:
            print(result.stdout)
        print(f"✓ {description} completed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Error: {e}")
        if e.stderr:
            print(e.stderr)
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

def main():
    print("="*60)
    print("Playwright Setup (Virtual Environment)")
    print("="*60)
    print("\nThis will create a virtual environment to avoid Windows")
    print("file locking issues.")
    
    # Step 1: Create virtual environment
    if VENV_DIR.exists():
        print(f"\n⚠ Virtual environment already exists at {VENV_DIR}")
        response = input("Delete and recreate? (y/n): ").strip().lower()
        if response == 'y':
            import shutil
            shutil.rmtree(VENV_DIR)
        else:
            print("Using existing virtual environment...")
    
    if not VENV_DIR.exists():
        print("\nStep 1: Creating virtual environment...")
        if not run_command(
            [sys.executable, "-m", "venv", str(VENV_DIR)],
            "Creating virtual environment"
        ):
            print("\n✗ Failed to create virtual environment")
            sys.exit(1)
    
    # Step 2: Install Playwright
    print("\nStep 2: Installing Playwright...")
    if not run_command(
        [str(VENV_PYTHON), "-m", "pip", "install", "playwright"],
        "Installing Playwright"
    ):
        print("\n✗ Failed to install Playwright")
        sys.exit(1)
    
    # Step 3: Install browser
    print("\nStep 3: Installing Chromium browser...")
    if not run_command(
        [str(VENV_PYTHON), "-m", "playwright", "install", "chromium"],
        "Installing Chromium"
    ):
        print("\n✗ Failed to install browser")
        sys.exit(1)
    
    print("\n" + "="*60)
    print("✓ Playwright setup completed in virtual environment!")
    print("="*60)
    print(f"\nVirtual environment location: {VENV_DIR.absolute()}")
    print("\nTo use Playwright scripts:")
    print(f"  {VENV_PYTHON} scripts/discover_ncert_pdfs_playwright.py --class 10 --language hi --output ncert.yaml")
    print("\nOr activate the virtual environment:")
    if sys.platform == "win32":
        print(f"  {VENV_DIR}\\Scripts\\activate")
    else:
        print(f"  source {VENV_DIR}/bin/activate")
    print("  python scripts/discover_ncert_pdfs_playwright.py --class 10 --language hi --output ncert.yaml")

if __name__ == "__main__":
    main()
