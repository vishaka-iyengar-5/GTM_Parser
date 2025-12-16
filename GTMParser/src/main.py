#!/usr/bin/env python3
"""
Main Execution - Clean GTM Analysis with ONLY Real Ghostery TrackerDB
Uses GitHub releases with Docker volume mounted fallback
NO hardcoded patterns - only real TrackerDB data for enhanced tracker detection
Fixed CSV export bug and improved labeling
SEQUENTIAL PROCESSING with batch range selection
"""

import asyncio
import csv
import json
import sys
import random
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from playwright.async_api import async_playwright

from simple_detector import StealthDetector
from progress_manager import ProgressManager


class CleanGTMAnalyzer:
    """Main analyzer class for clean GTM detection with ONLY real TrackerDB integration"""
    
    def __init__(self, debug_mode: bool = True, session_name: str = None):
        self.debug_mode = debug_mode
        self.detector = StealthDetector(debug_mode=debug_mode)
        self.progress_manager = ProgressManager(session_name=session_name, debug_mode=debug_mode) if session_name else None
        
        # Ensure output directories exist
        self.setup_output_directories()
    
    def setup_output_directories(self):
        """Create organized output directory structure"""
        directories = [
            "/app/output",
            "/app/output/csv"
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            
        print(f"📁 Output directories ready:")
        print(f"   CSV files: /app/output/csv/")
    
    async def analyze_websites(self, urls: List[str], output_file: str = None) -> List[Dict[str, Any]]:
        """
        SEQUENTIAL METHOD - Analyze multiple websites for GTM and consent mode with ONLY real TrackerDB detection
        
        Args:
            urls: List of URLs to analyze
            output_file: CSV file to save results (optional)
            
        Returns:
            List of analysis results
        """
        print(f" Starting Clean GTM Analysis with ONLY Real TrackerDB for {len(urls)} websites...")
        print("="*80)
        
        # Initialize TrackerDB
        trackerdb_success = await self.detector.initialize()
        if trackerdb_success:
            print(" Real TrackerDB loaded successfully - enhanced tracker detection enabled")
        else:
            print(" Real TrackerDB not available - NO tracker detection available")
            print(" For tracker detection, mount Docker volume: docker run -v ./data:/app/data")
        
        print("="*80)
        
        results = []
        
        try:
            async with async_playwright() as p:
                # Launch browser with stealth settings
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-features=VizDisplayCompositor',
                        '--disable-dev-shm-usage',
                        '--no-sandbox',
                        '--disable-web-security',
                        '--disable-features=site-per-process',
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-renderer-backgrounding',
                        '--no-first-run',
                        '--no-default-browser-check',
                        '--disable-extensions',
                        '--disable-plugins',
                        '--disable-javascript-harmony-shipping',
                        '--disable-ipc-flooding-protection',
                        '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                    ]
                )
                
                # Create context with realistic settings
                user_agents = [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15"
                ]
                
                context = await browser.new_context(
                    user_agent=random.choice(user_agents),
                    locale='en-US',
                    timezone_id='America/New_York',
                    extra_http_headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Cache-Control': 'max-age=0'
                    }
                )
                
                page = await context.new_page()
                
                for i, url in enumerate(urls, 1):
                    print(f"\n[{i}/{len(urls)}]  Analyzing: {url}")
                    
                    # Add random delay between requests (human-like behavior)
                    if i > 1:
                        delay = random.uniform(2, 5)  # 2-5 second delay
                        print(f"   ⏳ Human-like delay: {delay:.1f}s")
                        await asyncio.sleep(delay)
                    
                    result = await self.detector.analyze_website(page, url)
                    results.append(result)
                    
                    # Clear network requests after each analysis to save memory
                    self.detector.network_requests = []
                    
                    # Print quick summary with clean data
                    gtm = "✅" if result['gtm_detected'] else "❌"
                    consent = "✅" if result['consent_mode'] else "❌"
                    status = result['status']
                    urls_found = result['google_urls_count']
                    
                    # Clean summary with TrackerDB info
                    trackerdb_status = result['trackerdb_status']
                    events_count = len(result['gtm_events']) if result['gtm_events'] != 'not_applicable' else 0
                    trackers_count = len(result['third_party_trackers']) if result['third_party_trackers'] != 'not_applicable' else 0
                    domains_count = result.get('third_party_domains_count', 0)
                    
                    print(f"   Result: {gtm} GTM | {consent} Consent | {status} | {urls_found} Google URLs")
                    if result['gtm_detected']:
                        print(f"    Events: {events_count} |  3rd Party Trackers: {trackers_count} |  3rd Party Domains: {domains_count}")
                        print(f"    TrackerDB: {trackerdb_status['pattern_count']} patterns ({trackerdb_status['data_source']})")
                
                await browser.close()
        
        except Exception as e:
            print(f" Clean analysis failed: {str(e)}")
            return results
        
        # Save results if output file specified
        if output_file:
            self.save_to_csv(results, output_file)
        
        # Print final summary
        self.print_summary(results)
        
        return results
    
    async def analyze_websites_with_batches(self, urls: List[str], batch_size: int = 100) -> Dict[str, Any]:
        """
        SEQUENTIAL Batch processing with progress management
        """
        if not self.progress_manager:
            raise ValueError("Progress manager not initialized - use session_name parameter")
        
        print(f" Starting Sequential Batch Analysis for {len(urls)} websites (batch size: {batch_size})...")
        
        # Initialize TrackerDB
        await self.detector.initialize()
        
        # Initialize session
        session_info = self.progress_manager.initialize_session(urls, batch_size)
        remaining_urls = session_info['urls_to_process']
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
            context = await browser.new_context()
            page = await context.new_page()
            
            while remaining_urls:
                batch_urls = self.progress_manager.get_next_batch(remaining_urls)
                if not batch_urls:
                    break
                
                # Process batch using SEQUENTIAL detection logic
                batch_results = []
                for url in batch_urls:
                    if url == "https://www.visittrentino.info/" or url == "https://chuckanddons.com/":
                        print(f"⏭️  Skipping problematic website: {url}")
                        skipped_result = {
                            'url': url, 'gtm_detected': False, 'consent_mode': False, 'gtm_events': 'not_applicable', 
                            'third_party_trackers': 'not_applicable', 'third_party_domains_count': 0, 'third_party_domains_list': 'not_applicable',
                            'trackerdb_status': {'pattern_count': 0, 'data_source': 'github_releases'}, 'status': 'skipped', 
                            'google_urls_count': 0, 'analysis_time': 0, 'timestamp': asyncio.get_event_loop().time(), 'raw_urls': []
                        }
                        batch_results.append(skipped_result)
                        continue


                    result = await self.detector.analyze_website(page, url)
                    batch_results.append(result)
                    self.detector.network_requests = []  # Clear memory
                
                # Save batch and update progress
                self.progress_manager.mark_batch_completed(batch_urls, batch_results)
                
                # Remove from remaining
                for url in batch_urls:
                    if url in remaining_urls:
                        remaining_urls.remove(url)
                
                print(f" Batch {self.progress_manager.current_batch} completed")
            
            await browser.close()
        
        return self.progress_manager.get_session_stats()
    
    def save_to_csv(self, results: List[Dict[str, Any]], filename: str):
        """Save results to CSV file with clean TrackerDB format"""
        
        # Save to organized CSV directory (/app/output/csv/)
        csv_dir = Path("/app/output/csv")
        filepath = csv_dir / filename
        
        print(f"\n Saving clean results to: {filepath}")
        
        # Clean CSV headers with FIXED field names + NEW domain columns
        fieldnames = [
            'url', 'gtm_detected', 'consent_mode', 'gtm_events', 
            'third_party_trackers',
            'third_party_domains_count',
            'third_party_domains_list',
            'trackerdb_patterns_count', 'trackerdb_data_source',
            'status', 'google_urls_count', 'analysis_time', 'timestamp', 'raw_urls'
        ]
        
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for result in results:
                    # Convert complex data to CSV format
                    csv_result = {}
                    
                    # Basic fields
                    csv_result['url'] = result['url']
                    csv_result['gtm_detected'] = result['gtm_detected']
                    csv_result['consent_mode'] = result['consent_mode']
                    csv_result['status'] = result['status']
                    csv_result['google_urls_count'] = result['google_urls_count']
                    csv_result['analysis_time'] = result['analysis_time']
                    
                    # Handle GTM Events
                    if result.get('gtm_events') == 'not_applicable':
                        csv_result['gtm_events'] = 'not_applicable'
                    else:
                        events = result.get('gtm_events', [])
                        csv_result['gtm_events'] = ', '.join(events) if events else 'none'
                    
                    # Handle 3rd Party Trackers
                    if result.get('third_party_trackers') == 'not_applicable':
                        csv_result['third_party_trackers'] = 'not_applicable'
                    else:
                        trackers = result.get('third_party_trackers', [])
                        csv_result['third_party_trackers'] = ', '.join(trackers) if trackers else 'none'
                    
                    # Handle 3rd Party Domains Count
                    csv_result['third_party_domains_count'] = result.get('third_party_domains_count', 0)
                    
                    # Handle 3rd Party Domains List
                    if result.get('third_party_domains_list') == 'not_applicable':
                        csv_result['third_party_domains_list'] = 'not_applicable'
                    else:
                        domains = result.get('third_party_domains_list', [])
                        csv_result['third_party_domains_list'] = ', '.join(domains) if domains else 'none'
                    
                    # Handle TrackerDB Status
                    trackerdb_status = result.get('trackerdb_status', {})
                    csv_result['trackerdb_patterns_count'] = trackerdb_status.get('pattern_count', 0)
                    csv_result['trackerdb_data_source'] = trackerdb_status.get('data_source', 'none')
                    
                    # Handle other fields
                    csv_result['raw_urls'] = json.dumps(result.get('raw_urls', []))
                    csv_result['timestamp'] = datetime.fromtimestamp(result['timestamp']).isoformat()
                    
                    writer.writerow(csv_result)
            
            print(f" Clean results saved successfully!")
            print(f" Records saved: {len(results)}")
            
        except Exception as e:
            print(f"❌ Error saving CSV: {str(e)}")
    
    def print_summary(self, results: List[Dict[str, Any]]):
        """Print clean analysis summary with ONLY real TrackerDB insights"""
        if not results:
            print("\n No results to summarize")
            return
        
        total = len(results)
        gtm_count = sum(1 for r in results if r['gtm_detected'])
        consent_count = sum(1 for r in results if r['consent_mode'])
        both_count = sum(1 for r in results if r['gtm_detected'] and r['consent_mode'])
        
        success_count = sum(1 for r in results if r['status'] == 'success')
        timeout_count = sum(1 for r in results if r['status'] == 'timeout')
        error_count = sum(1 for r in results if r['status'] == 'error')
        
        print(f"\n{'='*80}")
        print("🧹 SEQUENTIAL GTM ANALYSIS WITH ONLY REAL TRACKERDB")
        print(f"{'='*80}")
        print(f" Total websites analyzed: {total}")
        print(f" Successful analyses: {success_count}")
        print(f" Timeouts: {timeout_count}")
        print(f" Errors: {error_count}")
        
        print(f"\n GTM Detection:")
        print(f"   Websites with GTM: {gtm_count}/{total} ({gtm_count/total*100:.1f}%)")
        
        print(f" Consent Mode:")
        print(f"   Websites with consent mode: {consent_count}/{total} ({consent_count/total*100:.1f}%)")
        
        print(f" Combined:")
        print(f"   GTM + Consent mode: {both_count}/{total} ({both_count/total*100:.1f}%)")


# CSV Loading Functions
def load_ecommerce_urls_from_csv(max_urls: int = None) -> List[str]:
    """Load e-commerce URLs from CSV file"""
    csv_paths = [
        "/app/data/ecommerce_urls/20250601 10k Unique ecommerce websites csv.csv",
        "/app/data/ecommerce_urls/2025-06-01 10k Unique e-commerce websites -csv.csv",
        "/app/data/ecommerce_urls/ecommerce_websites.csv"
    ]
    
    # Try to find the CSV file
    found_file = None
    for path in csv_paths:
        if os.path.exists(path):
            found_file = path
            print(f"📄 Found CSV: {path}")
            break
    
    if not found_file:
        print("❌ CSV file not found in any expected location:")
        for path in csv_paths:
            print(f"   - {path}")
        return []
    
    urls = []
    try:
        with open(found_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip first row
            headers = next(reader)  # Real headers
            print(f" Headers: {headers}")
            
            for row in reader:
                if row and row[0].strip():
                    url = row[0].strip()
                    if not url.startswith('http'):
                        url = f"https://{url}"
                    urls.append(url)
                    
                    if max_urls and len(urls) >= max_urls:
                        break
        
        print(f" Loaded {len(urls)} URLs")
        return urls
        
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return []


def get_test_urls() -> List[str]:
    """Get test URLs for validation"""
    return [
        "https://neetcode.io",
        "https://sport-exercise.ed.ac.uk/gym-memberships/bucs-universal-scheme",
        "https://www.zoopla.co.uk/to-rent/map/property/3-bedrooms/edinburgh-county/?keywords=hmo&price_frequency=per_month&price_max=1750&q=edinburgh&radius=3&search_source=to-rent",
        "https://gitlab.inria.fr/web-smartphone-privacy/Google-Tag-Manager-Hidden-Data-Leaks-and-its-Potential-Violations-under-EU-Data-Protection-Law/-/tree/main/comparison%20in%20depth%20and%20automated"
    ]


def get_comprehensive_test_urls() -> List[str]:
    """Get comprehensive test URLs (13 websites) for thorough validation"""
    return [
        "https://neetcode.io",
        "https://sport-exercise.ed.ac.uk/gym-memberships/bucs-universal-scheme", 
        "https://www.zoopla.co.uk/to-rent/map/property/3-bedrooms/edinburgh-county/?keywords=hmo&price_frequency=per_month&price_max=1750&q=edinburgh&radius=3&search_source=to-rent",
        "https://gitlab.inria.fr/web-smartphone-privacy/Google-Tag-Manager-Hidden-Data-Leaks-and-its-Potential-Violations-under-EU-Data-Protection-Law/-/tree/main/comparison%20in%20depth%20and%20automated",
        "https://www.hdfcbank.com/personal/resources/learning-centre/pay/what-is-add-on-credit-card-and-its-working",
        "https://learn.microsoft.com/en-us/credentials/",
        "https://www.onthemarket.com/to-rent/3-bed-property/glasgow-central-/?max-price=1750&radius=3.0&view=map-only",
        "https://www.linkedin.com/search/results/all/?fetchDeterministicClustersOnly=true&heroEntityKey=urn%3Ali%3Aorganization%3A11193112&keywords=coalition%2C%20inc.&origin=RICH_QUERY_SUGGESTION&position=2&searchId=4098f2c9-8811-47d9-a765-828fc8535387&sid=JLP&spellCorrectionEnabled=false",
        "https://www.rightmove.co.uk/property-to-rent/map.html?locationIdentifier=STATION^1652&maxBedrooms=3&minBedrooms=3&maxPrice=1750&numberOfPropertiesPerPage=499&radius=3.0&propertyTypes=&viewType=MAP&mustHave=&dontShow=&furnishTypes=&viewport=-2.34169%2C-2.27989%2C53.4737%2C53.4923&keywords=",
        "https://www.amazon.co.uk/",
        "https://www.temu.com/ul/kuiper/un2.html?_p_rfs=1&subj=un-search-web&_p_jump_id=960&_x_vst_scene=adg&search_key=Temu&_x_ads_channel=google&_x_ads_sub_channel=search&_x_ads_account=3954917911&_x_ads_set=20030620447&_x_ads_id=145204446901&_x_ads_creative_id=656070467910&_x_ns_source=g&_x_ns_gclid=CjwKCAjw1dLDBhBoEiwAQNRiQdi9Tdy75V6RosoSkMvr8DIjvrvYwdq1h_YodlZEaEJZCZXx7MwitxoC8sEQAvD_BwE&_x_ns_placement=&_x_ns_match_type=p&_x_ns_ad_position=&_x_ns_product_id=&_x_ns_target=&_x_ns_devicemodel=&_x_ns_wbraid=CkAKCAjwss3DBhBFEjAAkIzbRNshoCR0O0n5ASiA0HnZY2SCd3LvQcWNcwIKIncfMvNI_xS8EwBa6iNN3hUaAoSS&_x_ns_gbraid=0AAAAAo4mICEPHGVCLmwgCRwjpd8BUSbH1&_x_ns_keyword=temu&_x_ns_targetid=kwd-336866712715&gad_source=1&gad_campaignid=20030620447&gbraid=0AAAAAo4mICEPHGVCLmwgCRwjpd8BUSbH1&gclid=CjwKCAjw1dLDBhBoEiwAQNRiQdi9Tdy75V6RosoSkMvr8DIjvrvYwdq1h_YodlZEaEJZCZXx7MwitxoC8sEQAvD_BwE&adg_ctx=f-48f8423",
        "https://www.comptia.org/en/",
        "https://www.ncsc.gov.uk/information/certified-training"
    ]


async def main():
    """Main execution function with batch range selection for SEQUENTIAL processing"""
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--test":
            # Test mode with original 4 URLs
            urls = get_test_urls()
            output_file = f"clean_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            analyzer = CleanGTMAnalyzer(debug_mode=True)
            return await analyzer.analyze_websites(urls, output_file)
            
        elif sys.argv[1] == "--comprehensive":
            # Comprehensive test with 13 URLs
            urls = get_comprehensive_test_urls()
            output_file = f"clean_comprehensive_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            analyzer = CleanGTMAnalyzer(debug_mode=True)
            return await analyzer.analyze_websites(urls, output_file)
            
        elif sys.argv[1] == "--batch-test":
            # ENHANCED: Batch processing with optional batch range selection
            all_urls = load_ecommerce_urls_from_csv(max_urls=300)
            
            # Parse optional batch range parameters
            start_batch = 1
            num_batches = 3  # Default to 3 batches for test
            batch_size = 100
            
            for arg in sys.argv[2:]:
                if arg.startswith("--start-batch="):
                    start_batch = int(arg.split("=")[1])
                elif arg.startswith("--num-batches="):
                    num_batches = int(arg.split("=")[1])
                elif arg.startswith("--batch-size="):
                    batch_size = int(arg.split("=")[1])
            
            # Calculate URL range based on batch selection
            start_url_index = (start_batch - 1) * batch_size
            end_url_index = start_url_index + (num_batches * batch_size)
            
            # Slice URLs for the specified batch range
            urls = all_urls[start_url_index:end_url_index]
            
            print(f" SEQUENTIAL Batch Range Selection:")
            print(f"   Starting from batch: {start_batch}")
            print(f"   Number of batches: {num_batches}")
            print(f"   Batch size: {batch_size}")
            print(f"   URL range: {start_url_index + 1} to {min(end_url_index, len(all_urls))}")
            print(f"   Total URLs to process: {len(urls)}")
            
            session_name = f"batch_test_b{start_batch}-{start_batch + num_batches - 1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            analyzer = CleanGTMAnalyzer(debug_mode=True, session_name=session_name)
            return await analyzer.analyze_websites_with_batches(urls, batch_size=batch_size)
            
        elif sys.argv[1] == "--full-ecommerce":
            # ENHANCED: Full e-commerce analysis with optional batch range selection
            all_urls = load_ecommerce_urls_from_csv(max_urls=None)
            
            # Parse optional batch range parameters
            start_batch = 1
            num_batches = None  # Process all remaining batches by default
            batch_size = 100
            
            for arg in sys.argv[2:]:
                if arg.startswith("--start-batch="):
                    start_batch = int(arg.split("=")[1])
                elif arg.startswith("--num-batches="):
                    num_batches = int(arg.split("=")[1])
                elif arg.startswith("--batch-size="):
                    batch_size = int(arg.split("=")[1])
            
            # Calculate URL range based on batch selection
            start_url_index = (start_batch - 1) * batch_size
            
            if num_batches:
                end_url_index = start_url_index + (num_batches * batch_size)
                urls = all_urls[start_url_index:end_url_index]
                session_suffix = f"b{start_batch}-{start_batch + num_batches - 1}"
            else:
                urls = all_urls[start_url_index:]
                session_suffix = f"b{start_batch}-end"
            
            print(f" SEQUENTIAL Full E-commerce Analysis - Batch Range Selection:")
            print(f"   Starting from batch: {start_batch}")
            print(f"   Number of batches: {num_batches if num_batches else 'All remaining'}")
            print(f"   Batch size: {batch_size}")
            print(f"   URL range: {start_url_index + 1} to {min(start_url_index + len(urls), len(all_urls))}")
            print(f"   Total URLs to process: {len(urls)}")
            
            session_name = f"full_ecommerce_{session_suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            analyzer = CleanGTMAnalyzer(debug_mode=True, session_name=session_name)
            return await analyzer.analyze_websites_with_batches(urls, batch_size=batch_size)
            
        elif sys.argv[1].startswith("--resume="):
            # Resume existing session
            session_name = sys.argv[1].split("=")[1]
            urls = load_ecommerce_urls_from_csv(max_urls=None)
            analyzer = CleanGTMAnalyzer(debug_mode=True, session_name=session_name)
            return await analyzer.analyze_websites_with_batches(urls, batch_size=100)
            
        else:
            # Single URL
            urls = [sys.argv[1]]
            analyzer = CleanGTMAnalyzer(debug_mode=True)
            return await analyzer.analyze_websites(urls, output_file=None)
    else:
        # Default: show usage and run test
        print(" No arguments specified, running test mode...")
        print("Usage:")
        print("  python main.py https://example.com                    # Single URL")
        print("  python main.py --test                                 # Original 4 test URLs")
        print("  python main.py --comprehensive                        # Comprehensive 13 URLs")
        print("  python main.py --batch-test                           # 300 URLs (batches 1-3)")
        print("  python main.py --batch-test --start-batch=2           # 300 URLs (batches 2-4)")
        print("  python main.py --batch-test --start-batch=4 --num-batches=2  # 200 URLs (batches 4-5)")
        print("  python main.py --full-ecommerce                       # Full 10k e-commerce analysis")
        print("  python main.py --full-ecommerce --start-batch=10      # Start from batch 10 (URL 901+)")
        print("  python main.py --full-ecommerce --start-batch=5 --num-batches=5  # Batches 5-9 only")
        print("  python main.py --resume=session_name                  # Resume existing session")
        print()
        print(" SEQUENTIAL Batch Parameters:")
        print("  --start-batch=N     Start from batch N (default: 1)")
        print("  --num-batches=N     Process N batches (default: 3 for test, all for full)")
        print("  --batch-size=N      URLs per batch (default: 100)")
        
        urls = get_test_urls()
        output_file = f"clean_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        analyzer = CleanGTMAnalyzer(debug_mode=True)
        return await analyzer.analyze_websites(urls, output_file)


if __name__ == "__main__":
    try:
        results = asyncio.run(main())
        
        if isinstance(results, list):
            # Sequential method results
            print(f"\n Sequential GTM analysis with ONLY real TrackerDB complete!")
            print(f" Processed {len(results)} websites with clean tracker detection.")
            
        else:
            # Batch method results (dictionary with stats)
            print(f"\n Sequential Batch GTM analysis complete!")
            print(f" Final stats: {results['completed']} completed, {results['failed']} failed")
            print(f" Results saved to: {results['csv_file']}")
            print(f" Total time: {results['elapsed_time_hours']:.1f} hours")
            print(f" Success rate: {results['success_rate']:.1f}%")
            
            if results['completion_percentage'] >= 100:
                print(f"✅ Analysis completed successfully!")
            else:
                print(f"⚠️ Analysis incomplete: {results['completion_percentage']:.1f}% done")
                        
    except KeyboardInterrupt:
        print("\n⚠️ Analysis interrupted by user")
        print(" Progress has been saved and can be resumed")
    except Exception as e:
        print(f"\n Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()