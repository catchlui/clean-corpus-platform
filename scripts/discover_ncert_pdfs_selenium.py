#!/usr/bin/env python3
"""Discover NCERT PDF URLs using Selenium (handles JavaScript).

This script uses Selenium to interact with NCERT's JavaScript-heavy website
and extract PDF URLs.

Prerequisites:
    pip install selenium beautifulsoup4
    # Download ChromeDriver or GeckoDriver
    # Or use: pip install webdriver-manager

Usage:
    python scripts/discover_ncert_pdfs_selenium.py --class 10 --subject hindi --output urls.yaml
"""

from __future__ import annotations
import sys
import argparse
import time
import json
from urllib.parse import urljoin, urlparse

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

NCERT_BASE_URL = "https://ncert.nic.in"
NCERT_TEXTBOOK_URL = "https://ncert.nic.in/textbook.php"

def create_driver(headless: bool = True):
    """Create Selenium WebDriver."""
    if not HAS_SELENIUM:
        print("Error: Selenium not installed.")
        print("Install with: pip install selenium")
        print("Also install webdriver-manager: pip install webdriver-manager")
        sys.exit(1)
    
    # Try Chrome first, then Firefox
    try:
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        
        chrome_options = ChromeOptions()
        if headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    except Exception as e:
        print(f"Chrome driver failed: {e}")
        try:
            from selenium.webdriver.firefox.service import Service
            from webdriver_manager.firefox import GeckoDriverManager
            
            firefox_options = FirefoxOptions()
            if headless:
                firefox_options.add_argument('--headless')
            
            service = Service(GeckoDriverManager().install())
            driver = webdriver.Firefox(service=service, options=firefox_options)
            return driver
        except Exception as e2:
            print(f"Firefox driver also failed: {e2}")
            print("\nManual setup required:")
            print("1. Install ChromeDriver: https://chromedriver.chromium.org/")
            print("2. Or install GeckoDriver: https://github.com/mozilla/geckodriver")
            print("3. Or use: pip install webdriver-manager")
            sys.exit(1)

def discover_ncert_pdfs_selenium(class_num: str, subject: str = None, language: str = "hi", headless: bool = True) -> list[str]:
    """Discover NCERT PDFs using Selenium."""
    driver = None
    try:
        print(f"\n{'='*60}")
        print(f"Discovering NCERT PDFs (JavaScript-enabled)")
        print(f"{'='*60}\n")
        print(f"Class: {class_num}")
        if subject:
            print(f"Subject: {subject}")
        print(f"Language: {language}")
        print("\nStarting browser...")
        
        driver = create_driver(headless=headless)
        
        # Navigate to NCERT page
        url = f"{NCERT_TEXTBOOK_URL}?bhsr1=0-{class_num}"
        print(f"Loading: {url}")
        driver.get(url)
        
        # Wait for page to load (JavaScript content)
        print("Waiting for page to load...")
        time.sleep(5)  # Give JavaScript time to load
        
        # Try to find and interact with dropdowns
        pdf_urls = []
        
        # Method 1: Look for PDF links in the loaded page
        try:
            # Wait for links to be present
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "a"))
            )
            
            # Find all links
            links = driver.find_elements(By.TAG_NAME, "a")
            print(f"Found {len(links)} links on page")
            
            for link in links:
                href = link.get_attribute('href')
                if href and '.pdf' in href.lower():
                    if href not in pdf_urls:
                        pdf_urls.append(href)
                        print(f"  Found PDF: {href}")
        except Exception as e:
            print(f"Error finding links: {e}")
        
        # Method 2: Get page source and parse with BeautifulSoup
        if HAS_BS4:
            try:
                html = driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if '.pdf' in href.lower():
                        full_url = urljoin(NCERT_BASE_URL, href)
                        if full_url not in pdf_urls:
                            pdf_urls.append(full_url)
            except Exception as e:
                print(f"Error parsing HTML: {e}")
        
        print(f"\nTotal PDFs found: {len(pdf_urls)}")
        return pdf_urls
        
    except Exception as e:
        print(f"Error: {e}")
        return []
    finally:
        if driver:
            driver.quit()

def save_urls_to_yaml(urls: list[str], output_file: str, class_num: str, language: str, source_name: str = "ncert"):
    """Save URLs to YAML config."""
    yaml_content = f"""# NCERT PDF URLs discovered using Selenium
# Class: {class_num}
# Language: {language}
# Note: Verify these URLs manually before processing

sources:
  - name: "{source_name}"
    type: "batch"
    kind: "web_pdf"
    urls:
"""
    for url in urls:
        yaml_content += f'      - "{url}"\n'
    
    yaml_content += f"""    download_dir: "downloads/ncert/class_{class_num}"
    language: "{language}"
    auto_detect_language: true
    metadata:
      source: "NCERT"
      license: "CC-BY-NC"
      category: "textbook"
      class: "{class_num}"
"""
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(yaml_content)
    
    print(f"\nYAML configuration saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Discover NCERT PDF URLs using Selenium (handles JavaScript)"
    )
    parser.add_argument("--class", "-c", dest="class_num", required=True, help="Class number (1-12)")
    parser.add_argument("--subject", "-s", help="Subject name")
    parser.add_argument("--language", "-l", default="hi", help="Language code (hi, en, ta, etc.)")
    parser.add_argument("--output", "-o", required=True, help="Output YAML file")
    parser.add_argument("--source-name", default="ncert", help="Source name")
    parser.add_argument("--no-headless", action="store_true", help="Show browser window")
    
    args = parser.parse_args()
    
    if not HAS_SELENIUM:
        print("Error: Selenium not installed.")
        print("\nInstall with:")
        print("  pip install selenium webdriver-manager")
        print("\nOr use manual method (see docs/NCERT_DOWNLOAD_GUIDE.md)")
        sys.exit(1)
    
    urls = discover_ncert_pdfs_selenium(
        args.class_num,
        args.subject,
        args.language,
        headless=not args.no_headless
    )
    
    if not urls:
        print("\nNo PDFs found. This might be because:")
        print("1. NCERT website structure has changed")
        print("2. Page requires more time to load")
        print("3. PDFs are loaded via AJAX after user interaction")
        print("\nRecommendation: Use manual method (see docs/NCERT_DOWNLOAD_GUIDE.md)")
        sys.exit(1)
    
    save_urls_to_yaml(urls, args.output, args.class_num, args.language, args.source_name)
    print("\nNext steps:")
    print(f"1. Review {args.output}")
    print("2. Verify URLs are correct")
    print("3. Run pipeline: python -m clean_corpus.cli build --config <your_config.yaml>")

if __name__ == "__main__":
    main()
