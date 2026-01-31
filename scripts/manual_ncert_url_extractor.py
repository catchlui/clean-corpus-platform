#!/usr/bin/env python3
"""Manual NCERT URL Extractor - Helper for copying URLs from browser.

Since NCERT website uses JavaScript, this script helps you:
1. Extract URLs from browser developer tools
2. Convert browser bookmarks/links to YAML config
3. Validate NCERT PDF URLs

Usage:
    # Method 1: Paste URLs from browser
    python scripts/manual_ncert_url_extractor.py --urls "url1.pdf url2.pdf url3.pdf" --class 10 --language hi --output ncert.yaml
    
    # Method 2: Read from text file (one URL per line)
    python scripts/manual_ncert_url_extractor.py --file urls.txt --class 10 --language hi --output ncert.yaml
    
    # Method 3: Validate existing URLs
    python scripts/manual_ncert_url_extractor.py --validate --file urls.txt
"""

from __future__ import annotations
import sys
import argparse
import re
from urllib.parse import urlparse

def is_valid_ncert_url(url: str) -> bool:
    """Check if URL looks like a valid NCERT PDF URL."""
    parsed = urlparse(url)
    return (
        'ncert.nic.in' in parsed.netloc and
        '.pdf' in parsed.path.lower()
    )

def extract_urls_from_text(text: str) -> list[str]:
    """Extract URLs from text (handles various formats)."""
    # Pattern for URLs
    url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+\.pdf'
    urls = re.findall(url_pattern, text)
    
    # Also check for lines that look like URLs
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if line.startswith('http') and '.pdf' in line:
            # Extract URL (might have extra text)
            match = re.search(r'(https?://[^\s<>"{}|\\^`\[\]]+\.pdf)', line)
            if match:
                url = match.group(1)
                if url not in urls:
                    urls.append(url)
    
    return urls

def create_yaml_config(urls: list[str], class_num: str, language: str, subject: str = None, source_name: str = "ncert") -> str:
    """Create YAML configuration from URLs."""
    # Filter valid NCERT URLs
    valid_urls = [url for url in urls if is_valid_ncert_url(url)]
    invalid_urls = [url for url in urls if not is_valid_ncert_url(url)]
    
    if invalid_urls:
        print(f"\nWarning: {len(invalid_urls)} invalid URL(s) found:")
        for url in invalid_urls:
            print(f"  - {url}")
    
    if not valid_urls:
        print("\nError: No valid NCERT PDF URLs found")
        return ""
    
    yaml_content = f"""# NCERT PDF URLs (manually extracted)
# Class: {class_num}
# Language: {language}
"""
    if subject:
        yaml_content += f"# Subject: {subject}\n"
    
    yaml_content += f"""
sources:
  - name: "{source_name}"
    type: "batch"
    kind: "web_pdf"
    urls:
"""
    for url in valid_urls:
        yaml_content += f'      - "{url}"\n'
    
    yaml_content += f"""    download_dir: "downloads/ncert/class_{class_num}"
    language: "{language}"
    auto_detect_language: true
    timeout: 30
    max_retries: 3
    metadata:
      source: "NCERT"
      license: "CC-BY-NC"
      category: "textbook"
      class: "{class_num}"
"""
    if subject:
        yaml_content += f"      subject: \"{subject}\"\n"
    
    yaml_content += """      publisher: "National Council of Educational Research and Training"

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
"""
    
    return yaml_content

def validate_urls(urls: list[str]) -> dict:
    """Validate URLs and return results."""
    results = {
        "valid": [],
        "invalid": [],
        "total": len(urls)
    }
    
    for url in urls:
        if is_valid_ncert_url(url):
            results["valid"].append(url)
        else:
            results["invalid"].append(url)
    
    return results

def main():
    parser = argparse.ArgumentParser(
        description="Manual NCERT URL Extractor - Helper for JavaScript-heavy sites"
    )
    parser.add_argument("--urls", "-u", help="Space-separated list of URLs")
    parser.add_argument("--file", "-f", help="Text file with URLs (one per line)")
    parser.add_argument("--class", "-c", dest="class_num", help="Class number (1-12)")
    parser.add_argument("--subject", "-s", help="Subject name")
    parser.add_argument("--language", "-l", default="hi", help="Language code (hi, en, ta, etc.)")
    parser.add_argument("--output", "-o", help="Output YAML file")
    parser.add_argument("--source-name", default="ncert", help="Source name")
    parser.add_argument("--validate", action="store_true", help="Only validate URLs, don't create config")
    
    args = parser.parse_args()
    
    # Collect URLs
    urls = []
    
    if args.urls:
        urls.extend(args.urls.split())
    
    if args.file:
        try:
            with open(args.file, 'r', encoding='utf-8') as f:
                content = f.read()
                file_urls = extract_urls_from_text(content)
                urls.extend(file_urls)
        except Exception as e:
            print(f"Error reading file: {e}")
            sys.exit(1)
    
    if not urls:
        print("Error: No URLs provided")
        print("\nUsage:")
        print("  python scripts/manual_ncert_url_extractor.py --urls 'url1.pdf url2.pdf' --class 10 --language hi --output ncert.yaml")
        print("  python scripts/manual_ncert_url_extractor.py --file urls.txt --class 10 --language hi --output ncert.yaml")
        sys.exit(1)
    
    # Remove duplicates
    urls = list(set(urls))
    
    print(f"\n{'='*60}")
    print(f"NCERT URL Extractor")
    print(f"{'='*60}\n")
    print(f"Found {len(urls)} URL(s)")
    
    if args.validate:
        # Only validate
        results = validate_urls(urls)
        print(f"\nValid URLs: {len(results['valid'])}")
        print(f"Invalid URLs: {len(results['invalid'])}")
        
        if results['valid']:
            print("\nValid URLs:")
            for url in results['valid']:
                print(f"  ✓ {url}")
        
        if results['invalid']:
            print("\nInvalid URLs:")
            for url in results['invalid']:
                print(f"  ✗ {url}")
    else:
        # Create config
        if not args.class_num:
            print("Error: --class is required when creating config")
            sys.exit(1)
        
        yaml_content = create_yaml_config(
            urls,
            args.class_num,
            args.language,
            args.subject,
            args.source_name
        )
        
        if not yaml_content:
            sys.exit(1)
        
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(yaml_content)
            print(f"\n✓ YAML configuration saved to: {args.output}")
            print(f"\nNext steps:")
            print(f"1. Review {args.output}")
            print("2. Verify URLs are correct")
            print("3. Run: python -m clean_corpus.cli build --config", args.output)
        else:
            print("\n" + yaml_content)

if __name__ == "__main__":
    main()
