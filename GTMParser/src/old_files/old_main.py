#!/usr/bin/env python3
"""
GTM Parser - Main Execution Module
Orchestrates all detection modules and testing
"""

import asyncio
import json
import sys
from playwright.async_api import async_playwright

# Import detection modules
from gtm_detector import GTMDetector
from pii_detector import PIIDetector


async def test_combined_detection():
    """Test both GTM and PII detection together"""
    print("🚀 GTM Parser Starting...")
    print("🔍 Testing Combined GTM + PII Detection...")
    
    # Test URLs - mix of GTM and non-GTM sites
    test_urls = [
        "https://neetcode.io",           # Known GTM site from your example
        "https://sport-exercise.ed.ac.uk/gym-memberships/bucs-universal-scheme",           # Basic site, likely no GTM
        "https://www.zoopla.co.uk/to-rent/map/property/3-bedrooms/edinburgh-county/?keywords=hmo&price_frequency=per_month&price_max=1750&q=edinburgh&radius=3&search_source=to-rent",            # Likely has GTM
        "https://gitlab.inria.fr/web-smartphone-privacy/Google-Tag-Manager-Hidden-Data-Leaks-and-its-Potential-Violations-under-EU-Data-Protection-Law/-/tree/main/comparison%20in%20depth%20and%20automated",     # does NOT have GTM
    ]
    
    gtm_detector = GTMDetector(debug_mode=True)
    pii_detector = PIIDetector(debug_mode=True)
    combined_results = []
    
    try:
        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            
            page = await browser.new_page()
            
            # Test each URL with both detectors
            for url in test_urls:
                print(f"\n{'='*60}")
                print(f"🌐 Analyzing: {url}")
                print(f"{'='*60}")
                
                try:
                    # Run GTM detection
                    print("🔍 Running GTM Detection...")
                    gtm_result = await gtm_detector.analyze_website(page, url)
                    
                    # Run PII detection on the same page
                    print("🔍 Running PII Detection...")
                    pii_result = await pii_detector.analyze_website(page, url)
                    
                    # Combine results
                    combined_result = {
                        'url': url,
                        'timestamp': gtm_result['timestamp'],
                        'gtm_results': gtm_result,
                        'pii_results': pii_result,
                        'summary': {
                            'gtm_detected': gtm_result['gtm_detected'],
                            'gtm_confidence': gtm_result['confidence_score'],
                            'pii_detected': pii_result['pii_detected'],
                            'total_pii_instances': pii_result['total_pii_instances'],
                            'has_tracking_and_pii': gtm_result['gtm_detected'] and pii_result['pii_detected']
                        }
                    }
                    
                    combined_results.append(combined_result)
                    
                    # Print combined summary
                    print_combined_summary(combined_result)
                    
                except Exception as e:
                    print(f"❌ Failed to analyze {url}: {str(e)}")
                    combined_results.append({
                        'url': url,
                        'error': str(e),
                        'gtm_detected': False,
                        'pii_detected': False
                    })
                
                # Wait between requests to be respectful
                await asyncio.sleep(2)
            
            await browser.close()
            
    except Exception as e:
        print(f"❌ Browser error: {str(e)}")
        return False
    
    # Print final combined summary
    print_final_combined_summary(combined_results)
    
    # Save combined results to file
    save_combined_results(combined_results)
    
    return True


async def analyze_single_url_combined(url: str):
    """Analyze a single URL for both GTM and PII"""
    print(f"🔍 Analyzing single URL for GTM + PII: {url}")
    
    gtm_detector = GTMDetector(debug_mode=True)
    pii_detector = PIIDetector(debug_mode=True)
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            page = await browser.new_page()
            
            # Run both detections
            print("🔍 Running GTM Detection...")
            gtm_result = await gtm_detector.analyze_website(page, url)
            
            print("🔍 Running PII Detection...")
            pii_result = await pii_detector.analyze_website(page, url)
            
            # Combine and display results
            combined_result = {
                'url': url,
                'gtm_results': gtm_result,
                'pii_results': pii_result,
                'summary': {
                    'gtm_detected': gtm_result['gtm_detected'],
                    'gtm_confidence': gtm_result['confidence_score'],
                    'pii_detected': pii_result['pii_detected'],
                    'total_pii_instances': pii_result['total_pii_instances'],
                    'has_tracking_and_pii': gtm_result['gtm_detected'] and pii_result['pii_detected']
                }
            }
            
            print_combined_summary(combined_result)
            
            await browser.close()
            return combined_result
            
    except Exception as e:
        print(f"❌ Error analyzing {url}: {str(e)}")
        return None


def print_combined_summary(result):
    """Print combined GTM + PII detection summary"""
    url = result['url']
    gtm_data = result['gtm_results']
    pii_data = result['pii_results']
    summary = result['summary']
    
    print(f"\n📊 COMBINED RESULTS for {url}:")
    print(f"{'='*60}")
    
    # GTM Results
    print(f"🏷️  GTM Detection:")
    print(f"   GTM Detected: {'✅ YES' if summary['gtm_detected'] else '❌ NO'}")
    print(f"   Confidence: {summary['gtm_confidence']:.2f}")
    if gtm_data.get('container_ids'):
        print(f"   Container IDs: {', '.join(gtm_data['container_ids'])}")
    
    # PII Results
    print(f"🔒 PII Detection:")
    print(f"   PII Detected: {'✅ YES' if summary['pii_detected'] else '❌ NO'}")
    print(f"   Total PII Instances: {summary['total_pii_instances']}")
    if pii_data.get('pii_types_found'):
        print(f"   PII Types: {', '.join(pii_data['pii_types_found'])}")
    
    # Risk Assessment
    print(f"⚠️  Risk Assessment:")
    has_both = summary['has_tracking_and_pii']
    print(f"   Tracking + PII Combo: {'🚨 HIGH RISK' if has_both else '✅ Lower Risk'}")
    
    if has_both:
        print(f"   ⚠️  This site has BOTH tracking (GTM) AND PII leakage!")
    
    # Timing
    if 'timing' in gtm_data and gtm_data['timing'].get('detection_duration'):
        gtm_time = gtm_data['timing']['detection_duration']
        pii_time = pii_data['timing']['detection_duration']
        print(f"   Analysis Time: GTM={gtm_time:.2f}s, PII={pii_time:.2f}s")


def print_final_combined_summary(results):
    """Print final summary of all combined results"""
    print(f"\n{'='*60}")
    print("📈 FINAL COMBINED SUMMARY")
    print(f"{'='*60}")
    
    total_sites = len(results)
    gtm_sites = len([r for r in results if r.get('summary', {}).get('gtm_detected', False)])
    pii_sites = len([r for r in results if r.get('summary', {}).get('pii_detected', False)])
    high_risk_sites = len([r for r in results if r.get('summary', {}).get('has_tracking_and_pii', False)])
    
    print(f"Total Sites Analyzed: {total_sites}")
    print(f"Sites with GTM: {gtm_sites}")
    print(f"Sites with PII Leakage: {pii_sites}")
    print(f"🚨 HIGH RISK Sites (GTM + PII): {high_risk_sites}")
    
    if total_sites > 0:
        print(f"GTM Detection Rate: {(gtm_sites/total_sites)*100:.1f}%")
        print(f"PII Leakage Rate: {(pii_sites/total_sites)*100:.1f}%")
        print(f"High Risk Rate: {(high_risk_sites/total_sites)*100:.1f}%")
    
    print(f"\n🚨 HIGH RISK Sites (Both GTM + PII):")
    for result in results:
        if result.get('summary', {}).get('has_tracking_and_pii', False):
            url = result['url']
            pii_count = result['summary']['total_pii_instances']
            print(f"   🚨 {url} - {pii_count} PII instances")
    
    print(f"\n🏷️ GTM Sites Found:")
    for result in results:
        if result.get('summary', {}).get('gtm_detected', False):
            url = result['url']
            confidence = result['summary']['gtm_confidence']
            containers = result.get('gtm_results', {}).get('container_ids', [])
            print(f"   ✅ {url} (Confidence: {confidence:.2f})")
            if containers:
                print(f"      Containers: {', '.join(containers)}")


def save_combined_results(results):
    """Save combined results to JSON file"""
    try:
        output_file = 'combined_detection_results.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n💾 Combined results saved to: {output_file}")
    except Exception as e:
        print(f"❌ Failed to save combined results: {str(e)}")


# Legacy functions for backward compatibility
def print_detection_summary(result):
    """Print a summary of GTM detection results (legacy)"""
    url = result['url']
    detected = result['gtm_detected']
    confidence = result.get('confidence_score', 0)
    methods = result.get('detection_methods', [])
    containers = result.get('container_ids', [])
    
    print(f"📊 GTM Results for {url}:")
    print(f"   GTM Detected: {'✅ YES' if detected else '❌ NO'}")
    print(f"   Confidence: {confidence:.2f}")
    print(f"   Detection Methods: {', '.join(methods) if methods else 'None'}")
    print(f"   Container IDs: {', '.join(containers) if containers else 'None'}")
    
    if 'timing' in result and result['timing'].get('detection_duration'):
        duration = result['timing']['detection_duration']
        print(f"   Analysis Time: {duration:.2f} seconds")
    
    if 'details' in result:
        details = result['details']
        print(f"   Network Requests: {details.get('network_requests_count', 0)}")
        print(f"   DOM Elements: {details.get('dom_elements_count', 0)}")
        print(f"   JS Objects: {details.get('javascript_objects_count', 0)}")
        print(f"   Cookies: {details.get('cookies_count', 0)}")
        print(f"   Data Attributes: {details.get('data_attributes_count', 0)}")
    
    # Enhanced tracking analysis
    if 'tracking_analysis' in result:
        tracking = result['tracking_analysis']
        print(f"   E-commerce Tracking: {'✅' if tracking.get('has_ecommerce_tracking') else '❌'}")
        print(f"   User Data Collection: {'✅' if tracking.get('has_user_data_collection') else '❌'}")
        print(f"   Consent Implementation: {'✅' if tracking.get('has_consent_implementation') else '❌'}")


async def test_gtm_detector():
    """Test the GTM detector with known GTM sites (legacy)"""
    print("🚀 GTM Parser Starting...")
    print("🔍 Testing GTM Detection...")
    
    # Test URLs - mix of GTM and non-GTM sites
    test_urls = [
        "https://neetcode.io",           # Known GTM site from your example
        "https://sport-exercise.ed.ac.uk/gym-memberships/bucs-universal-scheme",           # Basic site, likely no GTM
        "https://www.zoopla.co.uk/to-rent/map/property/3-bedrooms/edinburgh-county/?keywords=hmo&price_frequency=per_month&price_max=1750&q=edinburgh&radius=3&search_source=to-rent",            # Likely has GTM
        "https://gitlab.inria.fr/web-smartphone-privacy/Google-Tag-Manager-Hidden-Data-Leaks-and-its-Potential-Violations-under-EU-Data-Protection-Law/-/tree/main/comparison%20in%20depth%20and%20automated",     # does NOT have GTM
    ]
    
    detector = GTMDetector(debug_mode=True)
    results = []
    
    try:
        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            
            page = await browser.new_page()
            
            # Test each URL
            for url in test_urls:
                print(f"\n{'='*60}")
                print(f"🌐 Analyzing: {url}")
                print(f"{'='*60}")
                
                try:
                    result = await detector.analyze_website(page, url)
                    results.append(result)
                    
                    # Print summary
                    print_detection_summary(result)
                    
                except Exception as e:
                    print(f"❌ Failed to analyze {url}: {str(e)}")
                    results.append({
                        'url': url,
                        'error': str(e),
                        'gtm_detected': False
                    })
                
                # Wait between requests to be respectful
                await asyncio.sleep(2)
            
            await browser.close()
            
    except Exception as e:
        print(f"❌ Browser error: {str(e)}")
        return False
    
    # Print final summary
    print_final_summary(results)
    
    # Save results to file
    save_results(results)
    
    return True


def print_final_summary(results):
    """Print final summary of all results (legacy)"""
    print(f"\n{'='*60}")
    print("📈 FINAL SUMMARY")
    print(f"{'='*60}")
    
    total_sites = len(results)
    gtm_sites = len([r for r in results if r.get('gtm_detected', False)])
    
    print(f"Total Sites Analyzed: {total_sites}")
    print(f"Sites with GTM: {gtm_sites}")
    print(f"Sites without GTM: {total_sites - gtm_sites}")
    print(f"GTM Detection Rate: {(gtm_sites/total_sites)*100:.1f}%")
    
    print(f"\n🏷️ GTM Sites Found:")
    for result in results:
        if result.get('gtm_detected', False):
            url = result['url']
            containers = result.get('container_ids', [])
            confidence = result.get('confidence_score', 0)
            print(f"   ✅ {url} (Confidence: {confidence:.2f})")
            if containers:
                print(f"      Containers: {', '.join(containers)}")


def save_results(results):
    """Save results to JSON file (legacy)"""
    try:
        output_file = 'gtm_detection_results.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n💾 Results saved to: {output_file}")
    except Exception as e:
        print(f"❌ Failed to save results: {str(e)}")


async def main():
    """Main function"""
    print("🔍 GTM Parser - Google Tag Manager Detection Tool")
    print("=" * 60)
    
    # Check if URL provided as command line argument
    if len(sys.argv) > 1:
        url = sys.argv[1]
        if len(sys.argv) > 2 and sys.argv[2] == '--gtm-only':
            # GTM only analysis
            detector = GTMDetector(debug_mode=True)
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
                page = await browser.new_page()
                result = await detector.analyze_website(page, url)
                print_detection_summary(result)
                await browser.close()
        else:
            # Combined analysis (default)
            await analyze_single_url_combined(url)
    else:
        # Run combined test suite
        success = await test_combined_detection()
        if success:
            print("\n🎉 Combined GTM + PII Detection test completed successfully!")
        else:
            print("\n❌ Combined Detection test failed!")
            sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ Analysis interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        sys.exit(1)