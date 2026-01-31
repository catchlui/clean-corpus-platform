#!/usr/bin/env python3
"""Complete Playwright setup script.

This script installs Playwright and browsers in one go.
Handles Windows file locking issues automatically.

Usage:
    python scripts/setup_playwright.py
    python scripts/setup_playwright.py chromium  # Install specific browser only
"""

from __future__ import annotations
import sys
import subprocess
import os
import time

def run_command(cmd: list[str], description: str, use_user_site: bool = False) -> bool:
    """Run a command and return success status."""
    print(f"\n{description}...")
    
    # Add --user flag to install to user site (avoids permission issues)
    if use_user_site and "pip install" in " ".join(cmd):
        cmd = cmd + ["--user"]
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        if result.stdout:
            print(result.stdout)
        print(f"✓ {description} completed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Error: {e}")
        if e.stderr:
            print(e.stderr)
        
        # Check for Windows file locking error
        error_msg = (e.stderr or "").lower()
        if "winerror" in error_msg or "cannot find the file" in error_msg or "file specified" in error_msg:
            print("\n⚠ Windows file locking issue detected.")
            print("This usually happens when:")
            print("  1. Python processes are still running")
            print("  2. Antivirus is blocking file access")
            print("  3. Need administrator permissions")
            print("\nTry these solutions:")
            print("  1. Close all Python processes and try again")
            print("  2. Run PowerShell as Administrator")
            print("  3. Use --user flag: pip install --user playwright")
            print("  4. Install in virtual environment (recommended)")
        
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

def check_python_processes():
    """Check if there are other Python processes running."""
    if sys.platform != "win32":
        return False
    
    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq python.exe"],
            capture_output=True,
            text=True
        )
        # Count lines with "python.exe" (excluding header)
        lines = [l for l in result.stdout.split('\n') if 'python.exe' in l.lower()]
        return len(lines) > 1  # More than just this process
    except:
        return False

def close_python_processes():
    """Close other Python processes (Windows only)."""
    if sys.platform != "win32":
        return False
    
    try:
        print("Closing other Python processes...")
        result = subprocess.run(
            ["taskkill", "/F", "/IM", "python.exe", "/T"],
            capture_output=True,
            text=True
        )
        time.sleep(2)  # Wait for processes to close
        return True
    except Exception as e:
        print(f"Warning: Could not close processes: {e}")
        return False

def main():
    print("="*60)
    print("Playwright Setup")
    print("="*60)
    
    # Check for running Python processes on Windows
    if sys.platform == "win32" and check_python_processes():
        print("\n⚠ Other Python processes detected (may cause file locking)")
        response = input("Close other Python processes? (y/n): ").strip().lower()
        if response == 'y':
            close_python_processes()
    
    # Step 1: Install Playwright package
    print("\nStep 1: Installing Playwright package...")
    
    # Try with --user flag first (avoids most Windows issues)
    print("Trying with --user flag (avoids Windows file locking)...")
    success = run_command(
        [sys.executable, "-m", "pip", "install", "--user", "playwright"],
        "Installing Playwright package (user site)"
    )
    
    # If failed, try normal installation
    if not success:
        print("\n⚠ User site installation failed, trying system installation...")
        success = run_command(
            [sys.executable, "-m", "pip", "install", "playwright"],
            "Installing Playwright package"
        )
    
    if not success:
        print("\n✗ Failed to install Playwright package")
        print("\nTroubleshooting options:")
        print("\n1. Close all Python processes manually:")
        print("   taskkill /F /IM python.exe /T")
        print("   Then run this script again")
        print("\n2. Use virtual environment (most reliable):")
        print("   python scripts/setup_playwright_venv.py")
        print("\n3. Install manually:")
        print("   pip install --user playwright")
        print("   python -m playwright install chromium")
        print("\n4. Use manual method (no Playwright needed):")
        print("   python scripts/manual_ncert_url_extractor.py")
        sys.exit(1)
    
    # Step 2: Install browser(s)
    browser = sys.argv[1] if len(sys.argv) > 1 else "chromium"
    
    if browser.lower() == "all":
        browsers = ["chromium", "firefox", "webkit"]
    elif browser.lower() in ["chromium", "firefox", "webkit"]:
        browsers = [browser.lower()]
    else:
        print(f"\nError: Unknown browser '{browser}'")
        print("Supported: chromium, firefox, webkit, all")
        sys.exit(1)
    
    print(f"\nStep 2: Installing browser(s): {', '.join(browsers)}...")
    success = True
    for b in browsers:
        if not run_command(
            [sys.executable, "-m", "playwright", "install", b],
            f"Installing {b}"
        ):
            success = False
    
    if success:
        print("\n" + "="*60)
        print("✓ Playwright setup completed successfully!")
        print("="*60)
        print("\nYou can now use:")
        print("  python scripts/discover_ncert_pdfs_playwright.py --class 10 --language hi --output ncert.yaml")
    else:
        print("\n" + "="*60)
        print("✗ Some browsers failed to install")
        print("="*60)
        print("\nYou can try installing manually:")
        print("  python -m playwright install chromium")
        sys.exit(1)

if __name__ == "__main__":
    main()
