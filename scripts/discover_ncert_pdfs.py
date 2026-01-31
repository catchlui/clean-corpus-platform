#!/usr/bin/env python3
"""Discover and list NCERT PDF URLs from NCERT website.

This script helps discover PDF URLs from NCERT's textbook portal.
It can extract PDF links from the NCERT website and generate a list
for use with the web_pdf source.

Usage:
    python scripts/discover_ncert_pdfs.py --class 10 --subject hindi --output urls.yaml
    python scripts/discover_ncert_pdfs.py --class 12 --subject mathematics --output urls.yaml
"""

from __future__ import annotations
import sys
import argparse
import re
import json
from urllib.parse import urljoin, urlparse

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Error: 'requests' and 'beautifulsoup4' required.")
    print("Install with: pip install requests beautifulsoup4")
    sys.exit(1)

NCERT_BASE_URL = "https://ncert.nic.in"
NCERT_TEXTBOOK_URL = "https://ncert.nic.in/textbook.php"

def get_ncert_page(class_num: str) -> str:
    """Get NCERT textbook page HTML."""
    params = {"bhsr1": f"0-{class_num}"}
    try:
        response = requests.get(NCERT_TEXTBOOK_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching NCERT page: {e}")
        return ""

def extract_pdf_urls(html: str, base_url: str = NCERT_BASE_URL) -> list[str]:
    """Extract PDF URLs from NCERT page HTML."""
    soup = BeautifulSoup(html, 'html.parser')
    pdf_urls = []
    
    # Look for links ending in .pdf
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.endswith('.pdf'):
            full_url = urljoin(base_url, href)
            if full_url not in pdf_urls:
                pdf_urls.append(full_url)
    
    return pdf_urls

def save_urls_to_yaml(urls: list[dict[str, any]], output_file: str, source_name: str = "ncert"):
    """Save discovered URLs to a YAML format compatible with web_pdf source."""
    yaml_content = f"""# NCERT PDF URLs discovered automatically
# Generated from NCERT website
# Note: You may need to manually verify and update these URLs

sources:
  - name: "{source_name}"
    type: "batch"
    kind: "web_pdf"
    urls:
"""
    for item in urls:
        yaml_content += f'      - "{item["url"]}"\n'
    
    if urls:
        first_item = urls[0]
        yaml_content += f"""    download_dir: "downloads/ncert/class_{first_item['class']}"
    language: "{first_item.get('language', 'hi')}"  # Adjust as needed
    auto_detect_language: true
    metadata:
      source: "NCERT"
      license: "CC-BY-NC"
      category: "textbook"
      class: "{first_item['class']}"
"""
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(yaml_content)
    
    print(f"\nYAML configuration saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Discover NCERT PDF URLs from NCERT website"
    )
    parser.add_argument("--class", "-c", dest="class_num", required=True, help="Class number (1-12)")
    parser.add_argument("--subject", "-s", help="Subject name")
    parser.add_argument("--language", "-l", default="hi", help="Language code (hi, en, ta, etc.)")
    parser.add_argument("--output", "-o", required=True, help="Output YAML file")
    parser.add_argument("--source-name", default="ncert", help="Source name for YAML config")
    
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"Discovering NCERT PDFs")
    print(f"{'='*60}\n")
    print(f"Class: {args.class_num}")
    if args.subject:
        print(f"Subject: {args.subject}")
    print(f"Language: {args.language}")
    print()
    
    html = get_ncert_page(args.class_num)
    if not html:
        print("Failed to fetch NCERT page")
        sys.exit(1)
    
    pdf_urls = extract_pdf_urls(html)
    
    if not pdf_urls:
        print("No PDF URLs found on the page.")
        print("\nNote: NCERT website may use JavaScript to load PDFs dynamically.")
        print("Alternative: Manually visit https://ncert.nic.in/textbook.php")
        print("Select class → subject → book → copy PDF URL")
        sys.exit(1)
    
    results = []
    for url in pdf_urls:
        result = {
            "url": url,
            "class": args.class_num,
            "subject": args.subject or "unknown",
            "language": args.language
        }
        results.append(result)
        print(f"Found: {url}")
    
    print(f"\nTotal PDFs found: {len(results)}")
    
    save_urls_to_yaml(results, args.output, args.source_name)
    print("\nNext steps:")
    print(f"1. Review {args.output}")
    print("2. Update URLs if needed (NCERT may require manual verification)")
    print("3. Run pipeline: python -m clean_corpus.cli build --config <your_config.yaml>")

if __name__ == "__main__":
    main()
