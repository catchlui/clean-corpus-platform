#!/usr/bin/env python3
"""Discover NCERT PDF URLs using Playwright (handles JavaScript).

This script uses Playwright to interact with NCERT's JavaScript-heavy website
and extract PDF URLs. Playwright is more reliable than Selenium for modern JS sites.

Prerequisites:
    pip install playwright beautifulsoup4
    python -m playwright install chromium  # Or firefox, webkit

Usage:
    python scripts/discover_ncert_pdfs_playwright.py --class 10 --language hi --output urls.yaml
    python scripts/discover_ncert_pdfs_playwright.py --class 10 --language hi --output urls.yaml --browser firefox
"""

from __future__ import annotations
import sys
import argparse
import asyncio
import json
import re
from urllib.parse import urljoin, urlparse
from typing import List

# Ensure user site-packages is in path BEFORE importing (for --user installations)
import site
try:
    user_site = site.getusersitepackages()
    if user_site and user_site not in sys.path:
        sys.path.insert(0, user_site)
except Exception:
    pass

# Try to import Playwright
HAS_PLAYWRIGHT = False
try:
    from playwright.async_api import async_playwright, Browser, Page
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

NCERT_BASE_URL = "https://ncert.nic.in"
NCERT_TEXTBOOK_URL = "https://ncert.nic.in/textbook.php"

async def discover_ncert_pdfs_playwright(
    class_num: str,
    subject: str = None,
    language: str = "hi",
    browser_type: str = "chromium",
    headless: bool = True,
    wait_time: int = 5
) -> List[str]:
    """Discover NCERT PDFs using Playwright."""
    pdf_urls = []
    
    print(f"\n{'='*60}")
    print(f"Discovering NCERT PDFs (Playwright - JavaScript-enabled)")
    print(f"{'='*60}\n")
    print(f"Class: {class_num}")
    if subject:
        print(f"Subject: {subject}")
    print(f"Language: {language}")
    print(f"Browser: {browser_type}")
    print(f"Headless: {headless}")
    print("\nStarting browser...")
    
    async with async_playwright() as p:
        try:
            # Launch browser
            if browser_type == "chromium":
                browser = await p.chromium.launch(headless=headless)
            elif browser_type == "firefox":
                browser = await p.firefox.launch(headless=headless)
            elif browser_type == "webkit":
                browser = await p.webkit.launch(headless=headless)
            else:
                print(f"Error: Unknown browser type: {browser_type}")
                print("Supported: chromium, firefox, webkit")
                return []
            
            # Create context and page
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            # Navigate to NCERT page
            url = f"{NCERT_TEXTBOOK_URL}?bhsr1=0-{class_num}"
            print(f"Loading: {url}")
            
            await page.goto(url, wait_until="networkidle", timeout=30000)
            
            # Wait for JavaScript to load content
            print("Waiting for page content to load...")
            await asyncio.sleep(wait_time)
            
            # Set up network request listener BEFORE any interactions
            pdf_requests = []
            def handle_response(response):
                url = response.url
                content_type = response.headers.get('content-type', '').lower()
                if '.pdf' in url.lower() or 'pdf' in content_type:
                    if url not in pdf_requests:
                        pdf_requests.append(url)
                        print(f"  Network PDF request: {url}")
            
            page.on("response", handle_response)
            
            # Wait for dropdowns to be visible
            try:
                await page.wait_for_selector("select", timeout=10000)
                print("Found dropdown elements on page")
            except Exception:
                print("Warning: Could not find dropdown elements, continuing anyway...")
            
            # Try to interact with dropdowns to trigger PDF link loading
            selects = await page.query_selector_all("select")
            if selects:
                print(f"Found {len(selects)} dropdown(s), attempting to interact...")
                
                # NCERT website uses cascading dropdowns: class -> subject -> book
                # First dropdown is class (tclass), second is subject (tsubject), third is book (tbook)
                
                # Step 1: Select the requested class
                if len(selects) > 0:
                    class_select = selects[0]
                    class_name = await class_select.get_attribute("name")
                    print(f"  Dropdown 1 (Class): name={class_name}")
                    
                    options = await class_select.query_selector_all("option")
                    print(f"    Found {len(options)} class options")
                    
                    # Find and select the requested class
                    class_selected = False
                    for opt in options:
                        try:
                            opt_value = await opt.get_attribute("value")
                            opt_text = await opt.inner_text()
                            
                            # Match class number (e.g., "Class X" or "10" or value="10")
                            if opt_value and opt_value.isdigit() and int(opt_value) == int(class_num):
                                print(f"    Selecting class: '{opt_text}' (value={opt_value})")
                                await class_select.select_option(value=opt_value)
                                await asyncio.sleep(3)  # Wait for subject dropdown to populate
                                class_selected = True
                                break
                        except (ValueError, AttributeError):
                            continue
                    
                    if not class_selected:
                        print(f"    WARNING: Could not find class {class_num}, trying first available class")
                        # Fallback: try first non-empty option
                        for opt_idx in range(1, min(len(options), 5)):
                            try:
                                opt_value = await options[opt_idx].get_attribute("value")
                                if opt_value and opt_value.isdigit():
                                    await class_select.select_option(value=opt_value)
                                    await asyncio.sleep(3)
                                    break
                            except:
                                continue
                
                # Step 2: Select subject (if specified or try first available)
                if len(selects) > 1:
                    subject_select = selects[1]
                    subject_name = await subject_select.get_attribute("name")
                    print(f"  Dropdown 2 (Subject): name={subject_name}")
                    
                    # Wait for subject dropdown to populate after class selection
                    await asyncio.sleep(2)
                    options = await subject_select.query_selector_all("option")
                    print(f"    Found {len(options)} subject options")
                    
                    # Show available subjects
                    if options:
                        for opt_idx in range(min(5, len(options))):
                            try:
                                opt_text = await options[opt_idx].inner_text()
                                opt_value = await options[opt_idx].get_attribute("value")
                                print(f"    Subject option {opt_idx}: '{opt_text}' (value={opt_value})")
                            except:
                                pass
                    
                    # Try to select a subject - NCERT uses click-based selection, not value-based
                    subject_selected = False
                    
                    # Try selecting by text (label) since values might be None
                    target_subject = subject if subject else None
                    # If language is specified, try to match subject name
                    if not target_subject and language:
                        lang_to_subject = {
                            'hi': 'Hindi', 'en': 'English', 'ta': 'Tamil', 'te': 'Telugu',
                            'kn': 'Kannada', 'ml': 'Malayalam', 'mr': 'Marathi', 'gu': 'Gujarati',
                            'bn': 'Bengali', 'pa': 'Punjabi', 'ur': 'Urdu', 'or': 'Odia',
                            'as': 'Assamese', 'sa': 'Sanskrit'
                        }
                        target_subject = lang_to_subject.get(language.lower())
                    
                    if target_subject:
                        # Try to find matching subject
                        for opt in options:
                            try:
                                opt_text = await opt.inner_text()
                                if target_subject.lower() in opt_text.lower():
                                    print(f"    Selecting subject by text: '{opt_text}'")
                                    # Try selecting by label/text
                                    try:
                                        await subject_select.select_option(label=opt_text)
                                        await asyncio.sleep(3)
                                        subject_selected = True
                                        break
                                    except:
                                        # Fallback: click on the option
                                        try:
                                            await opt.click()
                                            await asyncio.sleep(3)
                                            subject_selected = True
                                            break
                                        except:
                                            pass
                            except:
                                continue
                    
                    # If no subject specified or not found, try first available
                    if not subject_selected and options:
                        for opt_idx in range(1, min(len(options), 10)):
                            try:
                                opt_text = await options[opt_idx].inner_text()
                                opt_value = await options[opt_idx].get_attribute("value")
                                if opt_text.strip() and opt_text != "..Select Subject..":
                                    print(f"    Selecting first available subject: '{opt_text}'")
                                    # Try selecting by label
                                    try:
                                        await subject_select.select_option(label=opt_text)
                                        await asyncio.sleep(3)
                                        subject_selected = True
                                        break
                                    except:
                                        # Fallback: click on the option
                                        try:
                                            await options[opt_idx].click()
                                            await asyncio.sleep(3)
                                            subject_selected = True
                                            break
                                        except:
                                            pass
                            except:
                                continue
                    
                    if not subject_selected:
                        print(f"    WARNING: Could not select subject (options may require click instead of select)")
                
                # Step 3: Select book (try multiple books to get more PDFs)
                if len(selects) > 2:
                    book_select = selects[2]
                    book_name = await book_select.get_attribute("name")
                    print(f"  Dropdown 3 (Book): name={book_name}")
                    
                    # Wait for book dropdown to populate after subject selection
                    await asyncio.sleep(2)
                    options = await book_select.query_selector_all("option")
                    print(f"    Found {len(options)} book options")
                    
                    # Try selecting multiple books to discover more PDFs
                    # NCERT books also might need click-based selection
                    books_tried = 0
                    for opt_idx in range(1, min(len(options), 10)):  # Try first 10 books
                        try:
                            opt_value = await options[opt_idx].get_attribute("value")
                            opt_text = await options[opt_idx].inner_text()
                            
                            if opt_text.strip() and opt_text != "..Select Book Title..." and opt_text != "":
                                print(f"    Selecting book {books_tried + 1}: '{opt_text}'")
                                
                                # Try selecting by label first
                                try:
                                    if opt_value and opt_value != "-1":
                                        await book_select.select_option(value=opt_value)
                                    else:
                                        await book_select.select_option(label=opt_text)
                                    await asyncio.sleep(2)  # Wait for PDF links to appear
                                    books_tried += 1
                                    
                                    # After selecting book, NCERT might load PDFs dynamically
                                    # Try multiple methods to find the actual PDF links
                                    
                                    # Method 1: Check page HTML for embedded PDF URLs (might be in JavaScript/data attributes)
                                    try:
                                        html_content = await page.content()
                                        # Look for PDF URLs in the HTML (might be in data attributes or JavaScript)
                                        pdf_pattern = r'https?://[^\s"\'<>]+textbook/pdf/[^\s"\'<>]+\.pdf'
                                        embedded_urls = re.findall(pdf_pattern, html_content, re.IGNORECASE)
                                        for url in embedded_urls:
                                            if url not in pdf_urls and 'instruction' not in url.lower():
                                                pdf_urls.append(url)
                                                print(f"        ✓ Found embedded PDF URL: {url}")
                                    except Exception as e:
                                        pass
                                    
                                    # Method 2: Look for links with textbook/pdf pattern
                                    pdf_links = await page.query_selector_all("a[href*='textbook/pdf']")
                                    
                                    # Method 3: Look for any clickable elements that might reveal PDFs
                                    # Try clicking on book title links if they exist
                                    try:
                                        book_title_links = await page.query_selector_all(f"a:has-text('{opt_text}'), a[title*='{opt_text}']")
                                        for title_link in book_title_links[:1]:  # Try first match
                                            try:
                                                await title_link.click()
                                                await asyncio.sleep(2)
                                                # Check for PDFs after click
                                                clicked_pdfs = await page.query_selector_all("a[href*='textbook/pdf']")
                                                if clicked_pdfs:
                                                    pdf_links.extend(clicked_pdfs)
                                            except:
                                                pass
                                    except:
                                        pass
                                    
                                    # Method 4: Look for PDF links in tables or content divs
                                    if not pdf_links or len(pdf_links) < 2:
                                        content_areas = await page.query_selector_all("table a, .content a, #content a, .book-list a, tr a, td a")
                                        for area_link in content_areas:
                                            try:
                                                href = await area_link.get_attribute("href")
                                                if href and 'textbook/pdf' in href.lower() and '.pdf' in href.lower():
                                                    if area_link not in pdf_links:
                                                        pdf_links.append(area_link)
                                            except:
                                                pass
                                    
                                    if pdf_links:
                                        print(f"      Found {len(pdf_links)} potential PDF links")
                                        # Extract URLs from the links
                                        for link in pdf_links:
                                            try:
                                                href = await link.get_attribute("href")
                                                if href:
                                                    href_lower = href.lower()
                                                    full_url = urljoin(NCERT_BASE_URL, href)
                                                    
                                                    # Filter: only textbook PDFs, exclude common non-textbook files
                                                    exclude_patterns = ['instruction', 'leading_the_change', 'download', '.zip']
                                                    is_excluded = any(pattern in href_lower for pattern in exclude_patterns)
                                                    
                                                    # Include if it's a PDF in textbook/pdf directory
                                                    if (href_lower.endswith('.pdf') and 
                                                        'textbook/pdf' in href_lower and
                                                        not is_excluded):
                                                        if full_url not in pdf_urls:
                                                            pdf_urls.append(full_url)
                                                            print(f"        ✓ Extracted textbook PDF: {full_url}")
                                                    elif href_lower.endswith('.pdf'):
                                                        # Debug: show PDFs that were excluded
                                                        print(f"        - Excluded (not textbook): {full_url}")
                                            except Exception as e:
                                                continue
                                    else:
                                        print(f"      No PDF links found for this book")
                                except:
                                    # Fallback: try clicking the option
                                    try:
                                        await options[opt_idx].click()
                                        await asyncio.sleep(2)
                                        books_tried += 1
                                        
                                        # Extract PDF URLs after click
                                        pdf_links = await page.query_selector_all("a[href*='textbook/pdf'], a[href*='.pdf']")
                                        if pdf_links:
                                            print(f"      Found {len(pdf_links)} PDF links (via click)")
                                            for link in pdf_links:
                                                try:
                                                    href = await link.get_attribute("href")
                                                    if href:
                                                        href_lower = href.lower()
                                                        full_url = urljoin(NCERT_BASE_URL, href)
                                                        print(f"        Link href: {href} -> {full_url}")
                                                        
                                                        if (href_lower.endswith('.pdf') and 
                                                            'textbook/pdf' in href_lower and
                                                            'instruction' not in href_lower):
                                                            if full_url not in pdf_urls:
                                                                pdf_urls.append(full_url)
                                                                print(f"        ✓ Extracted textbook PDF: {full_url}")
                                                except Exception as e:
                                                    print(f"        Error extracting link: {e}")
                                                    continue
                                    except:
                                        pass
                        except Exception as e:
                            continue
                    
                    if books_tried == 0:
                        print(f"    WARNING: Could not select any books")
                
                # Wait a bit more after all interactions
                print("Waiting for all content to load after dropdown interactions...")
                await asyncio.sleep(3)
            else:
                print("No dropdowns found - PDFs may be loaded differently")
            
            # Scroll page to trigger lazy loading
            print("Scrolling page to trigger lazy loading...")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
            await page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(1)
            
            # Method 1: Extract PDF URLs from page links (after interactions)
            print("Extracting PDF URLs from page links...")
            links = await page.query_selector_all("a[href]")
            print(f"Found {len(links)} total links on page")
            
            for link in links:
                try:
                    href = await link.get_attribute("href")
                    if href:
                        # Check if it's a PDF link - be more specific
                        href_lower = href.lower()
                        if ('.pdf' in href_lower and 'textbook/pdf' in href_lower) or \
                           (href_lower.endswith('.pdf') and 'ncert.nic.in' in href_lower):
                            # Make absolute URL
                            full_url = urljoin(NCERT_BASE_URL, href)
                            # Filter out non-textbook PDFs (like instruction.pdf, leading_the_change.pdf)
                            if 'textbook/pdf' in full_url.lower() and full_url not in pdf_urls:
                                pdf_urls.append(full_url)
                                print(f"  Found PDF: {full_url}")
                except Exception as e:
                    continue
            
            # Method 2: Get page source and parse with BeautifulSoup
            if HAS_BS4:
                try:
                    html = await page.content()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        href_lower = href.lower()
                        # More specific filtering for textbook PDFs
                        if 'textbook/pdf' in href_lower and href_lower.endswith('.pdf'):
                            full_url = urljoin(NCERT_BASE_URL, href)
                            if full_url not in pdf_urls:
                                pdf_urls.append(full_url)
                                print(f"  Found PDF (HTML): {full_url}")
                except Exception as e:
                    print(f"Error parsing HTML: {e}")
            
            # Method 3: Collect PDF requests from network
            print(f"Checking network requests for PDFs... ({len(pdf_requests)} found)")
            
            # Filter network requests to only include textbook PDFs
            for url in pdf_requests:
                if 'textbook/pdf' in url.lower() and url not in pdf_urls:
                    pdf_urls.append(url)
            
            # Remove duplicates and filter out non-textbook PDFs
            pdf_urls = list(set(pdf_urls))
            # Filter: only keep URLs that look like textbook PDFs
            filtered_urls = []
            for url in pdf_urls:
                # Exclude common non-textbook PDFs
                exclude_patterns = ['instruction', 'leading_the_change', 'download', 'zip']
                if any(pattern in url.lower() for pattern in exclude_patterns):
                    continue
                # Include if it's in textbook/pdf directory
                if 'textbook/pdf' in url.lower() and url.lower().endswith('.pdf'):
                    filtered_urls.append(url)
            
            pdf_urls = filtered_urls
            
            await browser.close()
            
            print(f"\n{'='*60}")
            print(f"Discovery Complete")
            print(f"{'='*60}")
            print(f"Total textbook PDFs found: {len(pdf_urls)}")
            
            if len(pdf_urls) == 0:
                print("\nWARNING: No textbook PDFs found. Possible reasons:")
                print("  1. NCERT website structure may have changed")
                print("  2. PDFs require specific class/subject selection")
                print("  3. Page needs more time to load (try --wait-time 10)")
                print("  4. Try running with --no-headless to see what's happening")
                print("\nRecommendation: Use manual method for reliable results:")
                print("  python scripts/manual_ncert_url_extractor.py --help")
            else:
                print("\nSUCCESS: Found textbook PDFs:")
                for url in pdf_urls[:10]:  # Show first 10
                    print(f"  - {url}")
                if len(pdf_urls) > 10:
                    print(f"  ... and {len(pdf_urls) - 10} more")
            
            return pdf_urls
            
        except Exception as e:
            print(f"Error during discovery: {e}")
            import traceback
            traceback.print_exc()
            try:
                await browser.close()
            except:
                pass
            return []

def save_urls_to_yaml(
    urls: List[str],
    output_file: str,
    class_num: str,
    language: str,
    subject: str = None,
    source_name: str = "ncert"
) -> None:
    """Save URLs to YAML config."""
    yaml_content = f"""# NCERT PDF URLs discovered using Playwright
# Class: {class_num}
# Language: {language}
"""
    if subject:
        yaml_content += f"# Subject: {subject}\n"
    
    yaml_content += """# Note: Verify these URLs manually before processing

sources:
  - name: "{source_name}"
    type: "batch"
    kind: "web_pdf"
    urls:
""".format(source_name=source_name)
    
    for url in urls:
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
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(yaml_content)
    
    print(f"\n✓ YAML configuration saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Discover NCERT PDF URLs using Playwright (handles JavaScript)"
    )
    parser.add_argument("--class", "-c", dest="class_num", required=True, help="Class number (1-12)")
    parser.add_argument("--subject", "-s", help="Subject name")
    parser.add_argument("--language", "-l", default="hi", help="Language code (hi, en, ta, etc.)")
    parser.add_argument("--output", "-o", required=True, help="Output YAML file")
    parser.add_argument("--source-name", default="ncert", help="Source name")
    parser.add_argument("--browser", "-b", choices=["chromium", "firefox", "webkit"], default="chromium", help="Browser to use")
    parser.add_argument("--no-headless", action="store_true", help="Show browser window")
    parser.add_argument("--wait-time", "-w", type=int, default=5, help="Wait time for page load (seconds)")
    
    args = parser.parse_args()
    
    if not HAS_PLAYWRIGHT:
        print("Error: Playwright not installed or not found.")
        print("\nInstall with:")
        print("  pip install playwright")
        print("  # Or use --user flag (avoids Windows file locking):")
        print("  pip install --user playwright")
        print("  python -m playwright install chromium  # Or firefox, webkit")
        print("\nOr use the safe installer:")
        print("  python scripts/install_playwright_safe.py")
        print("\nOr use manual method (no Playwright needed):")
        print("  python scripts/manual_ncert_url_extractor.py")
        print("  (See docs/NCERT_DOWNLOAD_GUIDE.md)")
        sys.exit(1)
    
    # Run async function
    urls = asyncio.run(discover_ncert_pdfs_playwright(
        args.class_num,
        args.subject,
        args.language,
        args.browser,
        headless=not args.no_headless,
        wait_time=args.wait_time
    ))
    
    if not urls:
        print("\nNo PDFs found. This might be because:")
        print("1. NCERT website structure has changed")
        print("2. Page requires more time to load (try --wait-time 10)")
        print("3. PDFs are loaded via AJAX after user interaction")
        print("4. Try different browser: --browser firefox")
        print("\nRecommendation: Use manual method (see docs/NCERT_DOWNLOAD_GUIDE.md)")
        print("Or try: python scripts/manual_ncert_url_extractor.py")
        sys.exit(1)
    
    save_urls_to_yaml(urls, args.output, args.class_num, args.language, args.subject, args.source_name)
    print("\nNext steps:")
    print(f"1. Review {args.output}")
    print("2. Verify URLs are correct")
    print("3. Run pipeline: python -m clean_corpus.cli build --config", args.output)

if __name__ == "__main__":
    main()
