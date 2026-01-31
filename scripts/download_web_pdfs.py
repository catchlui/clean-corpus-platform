#!/usr/bin/env python3
"""Standalone script to download PDFs from URLs.

This script can be used to download PDFs before processing, or to test
web PDF download functionality.

Usage:
    python scripts/download_web_pdfs.py <url1> [url2] [url3] ... --output-dir DIR [--language LANG]
    
Examples:
    # Download single PDF
    python scripts/download_web_pdfs.py https://ncert.nic.in/textbook/pdf/iehi101.pdf --output-dir downloads/ncert
    
    # Download multiple PDFs
    python scripts/download_web_pdfs.py url1.pdf url2.pdf url3.pdf --output-dir downloads
    
    # Download with language tag
    python scripts/download_web_pdfs.py url.pdf --output-dir downloads --language hi
"""

from __future__ import annotations
import sys
import os
import argparse
from pathlib import Path

def download_pdfs(urls: list[str], output_dir: str, language: str | None = None):
    """Download PDFs from URLs."""
    try:
        import requests
    except ImportError:
        print("Error: 'requests' library not installed.")
        print("Install with: pip install requests")
        sys.exit(1)
    
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Downloading PDFs")
    print(f"{'='*60}\n")
    print(f"Output Directory: {output_dir}")
    print(f"Language: {language or 'auto-detect'}")
    print(f"URLs: {len(urls)}\n")
    
    downloaded = []
    failed = []
    
    for i, url in enumerate(urls, 1):
        print(f"[{i}/{len(urls)}] Downloading: {url}")
        
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            filename = os.path.basename(parsed.path) or f"document_{i}.pdf"
            if not filename.endswith('.pdf'):
                filename += '.pdf'
            
            file_path = Path(output_dir) / filename
            
            # Skip if already exists
            if file_path.exists():
                print(f"  [SKIP] Already exists: {file_path}")
                downloaded.append(str(file_path))
                continue
            
            # Download
            response = requests.get(url, timeout=30, stream=True)
            response.raise_for_status()
            
            # Verify content type
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and not url.endswith('.pdf'):
                print(f"  [WARN] Content type is not PDF: {content_type}")
            
            # Save
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            size_mb = file_path.stat().st_size / 1024 / 1024
            print(f"  [OK] Saved: {file_path} ({size_mb:.2f} MB)")
            downloaded.append(str(file_path))
            
        except Exception as e:
            print(f"  [FAIL] Error: {e}")
            failed.append(url)
    
    print(f"\n{'='*60}")
    print(f"Download Summary")
    print(f"{'='*60}\n")
    print(f"Successfully downloaded: {len(downloaded)}")
    print(f"Failed: {len(failed)}")
    
    if downloaded:
        print(f"\nDownloaded files:")
        for f in downloaded:
            print(f"  - {f}")
    
    if failed:
        print(f"\nFailed URLs:")
        for url in failed:
            print(f"  - {url}")
    
    # Create metadata file
    if downloaded and language:
        metadata_file = Path(output_dir) / "metadata.json"
        metadata = {
            "language": language,
            "files": downloaded,
            "source_urls": urls
        }
        import json
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        print(f"\nMetadata saved to: {metadata_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download PDFs from URLs"
    )
    parser.add_argument("urls", nargs="+", help="PDF URLs to download")
    parser.add_argument("--output-dir", "-o", required=True, help="Output directory")
    parser.add_argument("--language", "-l", help="Language code (ISO 639-1)")
    
    args = parser.parse_args()
    
    download_pdfs(args.urls, args.output_dir, args.language)
