#!/usr/bin/env python3
"""
Enhanced GTM and Consent Mode Detector with ONLY Real Ghostery TrackerDB
Uses GitHub releases with fallback to local zip file (Docker volume mount)
NO hardcoded patterns - only real TrackerDB data used for tracker detection
"""

import time
import logging
import random
import json
import zipfile
import os
import tempfile
import requests
from typing import Dict, List, Any, Optional, Tuple
from playwright.async_api import Page, Request
from playwright_stealth import stealth_async
from urllib.parse import urlparse


class GhosteryTrackerDB:
    """
    Ghostery TrackerDB integration class that downloads from GitHub releases
    with fallback to Docker volume mounted zip file
    NO HARDCODED PATTERNS - only real TrackerDB data
    """
    
    def __init__(self, debug_mode: bool = True):
        self.debug_mode = debug_mode
        self.logger = self._setup_logging()
        self.tracker_data = {}
        self.patterns = {}
        self.categories = {}
        self.organizations = {}
        self.is_loaded = False
        
        # Only GitHub releases API
        self.github_releases_url = 'https://api.github.com/repos/ghostery/trackerdb/releases/latest'
        
        # Docker volume mount paths
        self.fallback_paths = [
            '/app/data/fallback/ghostery_trackerdb_backup_july2025.zip',  # Updated path
            '/app/data/trackerdb.zip',                     # Alternative name
            'data/ghostery_trackerdb_backup_july2025.zip',       # Local development
            'ghostery_trackerdb_backup_july2025.zip'             # Same directory fallback
        ]
        
        # Cache settings
        self.cache_duration = 24 * 60 * 60  # 24 hours in seconds
        self.cache_cleanup_duration = 7 * 24 * 60 * 60  # 7 days in seconds
        self.cache_path = "/app/data/trackerdb/cached_trackerdb.json"
        
        # Ensure cache directory exists
        os.makedirs("/app/data/trackerdb", exist_ok=True)
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('GhosteryTrackerDB')
        logger.setLevel(logging.DEBUG if self.debug_mode else logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _is_cache_valid(self) -> bool:
        """Check if cached TrackerDB is still valid (within 24 hours)"""
        try:
            if not os.path.exists(self.cache_path):
                return False
            
            cache_age = time.time() - os.path.getmtime(self.cache_path)
            is_valid = cache_age < self.cache_duration
            
            if is_valid:
                hours_old = cache_age / 3600
                self.logger.info(f"📋 Valid cache found (age: {hours_old:.1f} hours)")
            else:
                self.logger.info(f"⏰ Cache expired (age: {cache_age/3600:.1f} hours)")
            
            return is_valid
            
        except Exception as e:
            self.logger.debug(f" Cache validation error: {e}")
            return False
    
    def _is_cache_too_old(self) -> bool:
        """Check if cache is older than 1 week"""
        try:
            if not os.path.exists(self.cache_path):
                return False
            
            cache_age = time.time() - os.path.getmtime(self.cache_path)
            is_too_old = cache_age > self.cache_cleanup_duration
            
            if is_too_old:
                days_old = cache_age / (24 * 3600)
                self.logger.info(f"🗑️ Cache is too old ({days_old:.1f} days), will delete after download failure")
            
            return is_too_old
            
        except Exception as e:
            self.logger.debug(f" Cache age check error: {e}")
            return False
    
    def _delete_old_cache(self):
        """Delete old cache file"""
        try:
            if os.path.exists(self.cache_path):
                os.remove(self.cache_path)
                self.logger.info(f"🗑️ Deleted old cache file")
        except Exception as e:
            self.logger.error(f" Failed to delete old cache: {e}")
    
    async def load_tracker_data(self) -> bool:
        """
        Load TrackerDB data from GitHub releases with fallback to local zip
        NO hardcoded patterns - fails gracefully if no data available
        
        Returns:
            bool: True if data loaded successfully, False otherwise
        """
        self.logger.info("🔄 Loading Ghostery TrackerDB data...")
        
        # 1. Try cached data first (if valid)
        if self._is_cache_valid() and await self._load_from_cache():
            self.is_loaded = True
            self.logger.info(" Successfully loaded TrackerDB from cache")
            return True
        
        # 2. Try loading from GitHub releases first
        if await self._load_from_github_releases():
            self.is_loaded = True
            self.logger.info(" Successfully loaded TrackerDB from GitHub releases")
            return True
        
        # 3. Download failed - check if cache is too old
        if self._is_cache_too_old():
            self.logger.info("🗑️ Download failed and cache is > 1 week old, deleting cache...")
            self._delete_old_cache()
            # Skip trying expired cache, go straight to zip fallback
        else:
            # 4. Try existing cache (if not too old)
            self.logger.info("🔄 Download failed, trying expired cache...")
            if await self._load_from_cache():
                self.is_loaded = True
                self.logger.warning("⚠ Using expired cache due to download failure")
                return True
        
        # 5. Fallback to local Docker volume mounted zip file
        self.logger.warning("⚠ Failed to load from GitHub releases, trying Docker volume mounted zip...")
        if await self._load_from_local_zip():
            self.is_loaded = True
            self.logger.info(" Successfully loaded TrackerDB from Docker volume mounted zip")
            return True
        
        # NO hardcoded patterns - fail gracefully
        self.logger.error(" TrackerDB could not be loaded from GitHub releases or Docker volume")
        self.logger.error(" Please ensure Docker volume is mounted with TrackerDB zip file")
        self.logger.error(" Expected: docker run -v ./data:/app/data your-image")
        self.logger.error(" NO tracker detection available without real TrackerDB data")
        self.is_loaded = False
        return False
    
    async def _load_from_cache(self) -> bool:
        """Load TrackerDB from cache"""
        try:
            if os.path.exists(self.cache_path):
                self.logger.debug(f"📁 Loading from cache: {self.cache_path}")
                
                with open(self.cache_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                self._parse_trackerdb_data(data)
                return True
                    
        except Exception as e:
            self.logger.error(f" Failed to load from cache: {e}")
        
        return False
    
    async def _load_from_github_releases(self) -> bool:
        """Load TrackerDB data from GitHub releases"""
        self.logger.debug("🔄 Attempting to load from GitHub releases...")
        
        try:
            headers = {
                'User-Agent': 'GTM-Research-Tool/1.0',
                'Accept': 'application/vnd.github.v3+json'
            }
            
            response = requests.get(self.github_releases_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                release_data = response.json()
                self.logger.debug(f" Found release: {release_data.get('tag_name', 'unknown')}")
                
                assets = release_data.get('assets', [])
                
                # Priority order for asset types
                asset_priorities = [
                    ('.json', 'JSON data file'),
                    ('.zip', 'ZIP archive'),
                ]
                
                for extension, description in asset_priorities:
                    for asset in assets:
                        asset_name = asset['name'].lower()
                        if asset_name.endswith(extension):
                            download_url = asset['browser_download_url']
                            self.logger.debug(f"📥 Downloading {description}: {asset['name']}")
                            
                            try:
                                asset_response = requests.get(download_url, headers=headers, timeout=60)
                                if asset_response.status_code == 200:
                                    if extension == '.json':
                                        tracker_data = asset_response.json()
                                        # Save to cache
                                        with open(self.cache_path, 'w', encoding='utf-8') as f:
                                            json.dump(tracker_data, f)
                                        self.logger.info(f"💾 TrackerDB cached for 24 hours")
                                        self._parse_trackerdb_data(tracker_data)
                                        return True
                                    elif extension == '.zip':
                                        # Save to cache first
                                        temp_data = await self._process_zip_file_data(asset_response.content)
                                        if temp_data:
                                            with open(self.cache_path, 'w', encoding='utf-8') as f:
                                                json.dump(temp_data, f)
                                            self.logger.info(f"💾 TrackerDB cached for 24 hours")
                                            return True
                            except Exception as e:
                                self.logger.debug(f" Failed to download {asset['name']}: {e}")
                                continue
                
                self.logger.debug(" No suitable assets found in GitHub release")
                
            elif response.status_code == 403:
                self.logger.warning("⚠️ GitHub API rate limited - trying local zip fallback")
            else:
                self.logger.debug(f" GitHub API returned status {response.status_code}")
                
        except Exception as e:
            self.logger.debug(f" GitHub releases failed: {e}")
        
        return False
    
    async def _load_from_local_zip(self) -> bool:
        """Load TrackerDB data from Docker volume mounted zip file"""
        self.logger.debug("🔍 Searching for Docker volume mounted TrackerDB zip file...")
        
        found_path = None
        for path in self.fallback_paths:
            if os.path.exists(path):
                found_path = path
                self.logger.debug(f"📁 Found zip file at: {path}")
                break
        
        if not found_path:
            self.logger.error(" No TrackerDB zip file found in Docker volume mount locations:")
            for path in self.fallback_paths:
                self.logger.error(f"   - {path}")
            self.logger.error("🐳 Mount a volume with: docker run -v ./data:/app/data")
            self.logger.error("📁 Place zip file as: ./data/fallback/ghostery_trackerdb_backup_july2025.zip")
            return False
        
        try:
            with open(found_path, 'rb') as f:
                zip_data = f.read()
            
            self.logger.debug(f" Loading TrackerDB from: {found_path}")
            return await self._process_zip_file_data(zip_data)
            
        except Exception as e:
            self.logger.error(f" Error loading zip file {found_path}: {e}")
            return False
    
    async def _process_zip_file_data(self, zip_data: bytes) -> bool:
        """Process ZIP file containing TrackerDB data"""
        try:
            with tempfile.NamedTemporaryFile() as tmp_file:
                tmp_file.write(zip_data)
                tmp_file.flush()
                
                with zipfile.ZipFile(tmp_file.name, 'r') as zip_file:
                    self.logger.debug(f"📋 ZIP contents: {zip_file.namelist()}")
                    
                    # Look for TrackerDB JSON files in priority order
                    priority_files = [
                        'trackerdb.json',
                        'dist/trackerdb.json',
                        'data/trackerdb.json',
                        'patterns.json', 
                        'trackers.json'
                    ]
                    
                    # Try priority files first
                    for priority_file in priority_files:
                        for file_name in zip_file.namelist():
                            if file_name.endswith(priority_file) or file_name == priority_file:
                                try:
                                    with zip_file.open(file_name) as json_file:
                                        tracker_data = json.load(json_file)
                                        self._parse_trackerdb_data(tracker_data)
                                        self.logger.debug(f" Successfully parsed: {file_name}")
                                        return True
                                except Exception as e:
                                    self.logger.debug(f" Failed to parse {file_name}: {e}")
                                    continue
                    
                    # If no priority files found, try any JSON file with tracker-like content
                    for file_name in zip_file.namelist():
                        if file_name.endswith('.json'):
                            try:
                                with zip_file.open(file_name) as json_file:
                                    data = json.load(json_file)
                                    if isinstance(data, dict) and (
                                        'patterns' in data or 
                                        'trackers' in data or 
                                        len(data) > 50  # Assume tracker data if many entries
                                    ):
                                        self._parse_trackerdb_data(data)
                                        self.logger.debug(f" Successfully parsed: {file_name}")
                                        return True
                            except Exception as e:
                                self.logger.debug(f" Failed to parse {file_name}: {e}")
                                continue
                    
                    self.logger.error(" No valid TrackerDB JSON data found in ZIP file")
                
        except Exception as e:
            self.logger.error(f" Error processing ZIP file: {e}")
        
        return False
    
    def _parse_trackerdb_data(self, data: Dict[str, Any]):
        """Parse TrackerDB JSON data into usable format"""
        self.logger.debug("🔄 Parsing TrackerDB data...")
        
        try:
            # Handle different possible data structures
            if 'patterns' in data:
                self.patterns = data['patterns']
            elif 'trackers' in data:
                self.patterns = data['trackers']
            else:
                # Assume the entire data is patterns
                self.patterns = data
            
            # Extract categories and organizations if available
            self.categories = data.get('categories', {})
            self.organizations = data.get('organizations', {})
            
            # Build lookup tables for efficient pattern matching
            self._build_lookup_tables()
            
            self.logger.info(f"📊 Successfully parsed {len(self.patterns)} tracker patterns from TrackerDB")
            
        except Exception as e:
            self.logger.error(f" Error parsing TrackerDB data: {e}")
            raise
    
    def _build_lookup_tables(self):
        """Build efficient lookup tables from TrackerDB patterns"""
        self.tracker_data = {
            'domain_patterns': {},
            'tracker_info': {},
            'category_counts': {}
        }
        
        pattern_count = 0
        category_counts = {}
        
        for pattern_key, pattern_data in self.patterns.items():
            if isinstance(pattern_data, dict):
                # Extract domain information
                domains = pattern_data.get('domains', [])
                if isinstance(domains, str):
                    domains = [domains]
                
                # Extract tracker info
                tracker_info = {
                    'name': pattern_data.get('name', pattern_key),
                    'category': pattern_data.get('category', 'unknown'),
                    'organization': pattern_data.get('organization', 'unknown'),
                    'website_url': pattern_data.get('website_url', ''),
                    'domains': domains,
                    'pattern_key': pattern_key
                }
                
                # Count categories
                category = tracker_info['category']
                category_counts[category] = category_counts.get(category, 0) + 1
                
                # Store in lookup tables
                self.tracker_data['tracker_info'][pattern_key] = tracker_info
                
                # Build domain lookup
                for domain in domains:
                    if domain:  # Skip empty domains
                        self.tracker_data['domain_patterns'][domain.lower()] = pattern_key
                        pattern_count += 1
        
        self.tracker_data['category_counts'] = category_counts
        
        self.logger.debug(f" Built lookup tables with {pattern_count} domain patterns")
        self.logger.debug(f" Categories found: {dict(list(category_counts.items())[:5])}...")
    
    def identify_tracker(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Identify a tracker from a URL using ONLY TrackerDB data
        
        Args:
            url: URL to check
            
        Returns:
            Dictionary with tracker information or None if not found
        """
        if not self.is_loaded:
            return None
        
        try:
            parsed_url = urlparse(url.lower())
            domain = parsed_url.netloc
            
            # Remove common prefixes
            domain = domain.replace('www.', '')
            
            # Check exact domain match
            if domain in self.tracker_data['domain_patterns']:
                pattern_key = self.tracker_data['domain_patterns'][domain]
                tracker_info = self.tracker_data['tracker_info'][pattern_key].copy()
                tracker_info['matched_domain'] = domain
                tracker_info['full_url'] = url
                return tracker_info
            
            # Check subdomain matches
            for tracked_domain, pattern_key in self.tracker_data['domain_patterns'].items():
                if domain.endswith('.' + tracked_domain) or tracked_domain in domain:
                    tracker_info = self.tracker_data['tracker_info'][pattern_key].copy()
                    tracker_info['matched_domain'] = tracked_domain
                    tracker_info['full_url'] = url
                    return tracker_info
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error identifying tracker for {url}: {e}")
            return None
    
    def get_loading_status(self) -> Dict[str, Any]:
        """Get detailed status of TrackerDB loading"""
        return {
            'is_loaded': self.is_loaded,
            'pattern_count': len(self.patterns),
            'domain_patterns_count': len(self.tracker_data.get('domain_patterns', {})),
            'category_counts': self.tracker_data.get('category_counts', {}),
            'data_source': 'github_releases' if self.is_loaded and len(self.patterns) > 100 else 'local_zip' if self.is_loaded else 'none'
        }


class StealthDetector:
    """Enhanced GTM and consent detection with ONLY real Ghostery TrackerDB integration"""
    
    def __init__(self, debug_mode: bool = True):
        self.debug_mode = debug_mode
        self.logger = self._setup_logging()
        
        # Initialize Ghostery TrackerDB
        self.ghostery_db = GhosteryTrackerDB(debug_mode=debug_mode)
        
        # Network request tracking for 3rd party tracker detection
        self.network_requests = []
        self.gtm_load_time = None
        self.gtm_detected = False
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('StealthDetector')
        logger.setLevel(logging.DEBUG if self.debug_mode else logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    async def initialize(self) -> bool:
        """Initialize the detector by loading TrackerDB data"""
        self.logger.info("🚀 Initializing Enhanced Stealth Detector with Ghostery TrackerDB...")
        
        success = await self.ghostery_db.load_tracker_data()
        
        # Print loading status
        status = self.ghostery_db.get_loading_status()
        if success:
            self.logger.info(f" TrackerDB loaded: {status['pattern_count']} patterns from {status['data_source']}")
            self.logger.info(" Detector initialization complete")
        else:
            self.logger.warning(" TrackerDB could not be loaded - NO tracker detection available")
            self.logger.warning(" Ensure Docker volume is mounted: docker run -v ./data:/app/data")
        
        return success
    
    async def analyze_website(self, page: Page, url: str) -> Dict[str, Any]:
        """
        Analyze a website for GTM and consent mode with ONLY TrackerDB-based tracker detection
        """
        self.logger.info(f"🔍 Analyzing with TrackerDB: {url}")
        start_time = time.time()
        
        # Reset tracking data for this analysis
        self.network_requests = []
        self.gtm_load_time = None
        self.gtm_detected = False
        
        try:
            # Apply stealth to the page
            await stealth_async(page)
            
            # Setup network monitoring BEFORE navigation
            await self._setup_network_monitoring(page)
            
            # Navigate to website with retry logic
            success = await self._load_page_with_retry(page, url)
            
            if not success:
                return self._create_timeout_result(url, start_time)
            
            # Wait for scripts to load
            await page.wait_for_timeout(5000)
            
            # Handle cookie consent banners
            await self._handle_cookie_consent(page)
            
            # Wait for GTM to load after consent
            await page.wait_for_timeout(3000)
            
            # Simple user interactions to trigger GTM (if present)
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                await page.wait_for_timeout(2000)
                await page.click('body')
                await page.wait_for_timeout(2000)
            except:
                pass
            
            # Additional wait for tracker responses
            await page.wait_for_timeout(3000)
            
            # Get all Google URLs using Performance API
            google_urls = await self._get_google_urls(page)
            
            # Analyze for GTM and consent mode
            self.gtm_detected = self._detect_gtm(google_urls)
            
            # Only run tracker detection if GTM is detected AND TrackerDB is loaded
            consent_mode = False
            gtm_events = []
            third_party_trackers = []
            third_party_domains_list = []
            third_party_domains_count = 0
            
            if self.gtm_detected:
                consent_mode = self._detect_consent_mode(google_urls)
                gtm_events = await self._detect_gtm_events(page)
                # Modified to return both trackers and domains
                third_party_trackers, third_party_domains_list = await self._detect_third_party_trackers(page)
                third_party_domains_count = len(third_party_domains_list)
            
            # Calculate analysis time
            analysis_time = time.time() - start_time
            
            # Get TrackerDB status
            trackerdb_status = self.ghostery_db.get_loading_status()
            
            result = {
                'url': url,
                'timestamp': time.time(),
                'gtm_detected': self.gtm_detected,
                'consent_mode': consent_mode,
                'gtm_events': gtm_events if self.gtm_detected else 'not_applicable',
                'third_party_trackers': third_party_trackers if self.gtm_detected else 'not_applicable',
                'third_party_domains_list': third_party_domains_list if self.gtm_detected else 'not_applicable',
                'third_party_domains_count': third_party_domains_count if self.gtm_detected else 0,
                'trackerdb_status': trackerdb_status,
                'status': 'success',
                'google_urls_count': len(google_urls),
                'analysis_time': analysis_time,
                'raw_urls': google_urls
            }
            
            self.logger.info(f" Analysis complete: GTM={self.gtm_detected}, Consent={consent_mode}, TrackerDB={trackerdb_status['data_source']}")
            return result
            
        except Exception as e:
            self.logger.error(f" Error analyzing {url}: {str(e)}")
            return self._create_error_result(url, str(e), start_time)
    
    async def _setup_network_monitoring(self, page: Page):
        """Setup network request monitoring for tracker detection"""
        self.logger.debug("🌐 Setting up network monitoring...")
        
        async def handle_request(request: Request):
            """Handle outgoing requests"""
            try:
                request_data = {
                    'url': request.url,
                    'method': request.method,
                    'timestamp': time.time(),
                    'resource_type': request.resource_type
                }
                
                # Check if this is GTM loading
                if 'googletagmanager.com/gtm.js' in request.url and self.gtm_load_time is None:
                    self.gtm_load_time = time.time()
                    self.logger.debug(f"🏷️ GTM load time recorded: {self.gtm_load_time}")
                
                self.network_requests.append(request_data)
                
            except Exception as e:
                self.logger.debug(f"Error handling request: {e}")
        
        page.on('request', handle_request)
        self.logger.debug(" Network monitoring setup complete")
    
    async def _load_page_with_retry(self, page: Page, url: str, max_retries: int = 1) -> bool:
        """Load page with retry logic"""
        for attempt in range(max_retries + 1):
            try:
                self.logger.debug(f"🌐 Loading page (attempt {attempt + 1}): {url}")
                
                if attempt > 0:
                    delay = random.uniform(3, 7)
                    await page.wait_for_timeout(int(delay * 1000))
                
                await page.goto(url, wait_until='domcontentloaded', timeout=45000)
                self.logger.debug(f" Page loaded successfully")
                return True
                
            except Exception as e:
                if attempt == max_retries:
                    self.logger.error(f" Failed to load page after {max_retries + 1} attempts: {e}")
                    return False
                else:
                    self.logger.warning(f"⚠ Attempt {attempt + 1} failed, retrying: {e}")
        
        return False
    
    async def _get_google_urls(self, page: Page) -> List[str]:
        """Get all Google-related URLs using Performance API"""
        
        performance_script = """
        () => {
            try {
                const resources = performance.getEntriesByType('resource');
                const googleUrls = resources
                    .filter(resource => {
                        const url = resource.name.toLowerCase();
                        return url.includes('google') || 
                               url.includes('gtm') || 
                               url.includes('analytics') ||
                               url.includes('tagmanager') ||
                               url.includes('doubleclick');
                    })
                    .map(resource => resource.name);
                
                return [...new Set(googleUrls)];
            } catch (error) {
                return [];
            }
        }
        """
        
        try:
            google_urls = await page.evaluate(performance_script)
            
            if len(google_urls) == 0:
                self.logger.debug(" No Google URLs found, waiting longer...")
                await page.wait_for_timeout(5000)
                google_urls = await page.evaluate(performance_script)
            
            self.logger.debug(f" Found {len(google_urls)} Google URLs")
            return google_urls
            
        except Exception as e:
            self.logger.error(f" Error executing Performance API script: {e}")
            return []
    
    async def _handle_cookie_consent(self, page: Page):
        """Handle common cookie consent banners"""
        self.logger.debug("🍪 Looking for cookie consent banners...")
        
        consent_selectors = [
            'button:has-text("Accept")',
            'button:has-text("Accept all")',
            'button:has-text("Accept All")',
            'button:has-text("Essential")',
            'button:has-text("OK")',
            'button:has-text("Got it")',
            'button:has-text("Continue")',
            'button:has-text("Agree")',
            'button:has-text("Allow")',
            '[class*="accept"]',
            '[class*="consent"]',
            '[class*="cookie"]',
            '[id*="accept"]',
            '[id*="consent"]',
            '[role="button"][aria-label*="accept"]',
        ]
        
        try:
            for selector in consent_selectors:
                try:
                    element = await page.query_selector(selector)
                    if element:
                        is_visible = await element.is_visible()
                        if is_visible:
                            self.logger.debug(f" Found consent button: {selector}")
                            await element.click()
                            await page.wait_for_timeout(1000)
                            self.logger.debug(" Clicked consent button")
                            return
                except:
                    continue
            
            self.logger.debug(" No consent banner found or already handled")
            
        except Exception as e:
            self.logger.debug(f" Error handling consent: {e}")
    
    def _detect_gtm(self, google_urls: List[str]) -> bool:
        """Detect GTM presence in Google URLs"""
        gtm_patterns = [
            'googletagmanager.com',
            'gtm.js',
            'gtag/js',
            '/gtm?id=',
            'gtm-'
        ]
        
        for url in google_urls:
            url_lower = url.lower()
            for pattern in gtm_patterns:
                if pattern in url_lower:
                    self.logger.debug(f" GTM detected in: {url[:100]}...")
                    return True
        
        return False
    
    def _detect_consent_mode(self, google_urls: List[str]) -> bool:
        """Detect Google Consent Mode implementation"""
        consent_patterns = [
            '&gcs=', '&consent=', '&gcd=', '&npa=', '&pscdl=',
            'consent%3d', 'gcs%3d', 'gcd%3d', 'npa%3d', 'pscdl%3d',
            'consent_state', 'analytics_storage', 'ad_storage'
        ]
        
        for url in google_urls:
            url_lower = url.lower()
            for pattern in consent_patterns:
                if pattern in url_lower:
                    self.logger.debug(f" Consent mode detected in: {url[:100]}...")
                    return True
        
        return False
    
    async def _detect_gtm_events(self, page: Page) -> List[str]:
        """Detect GTM events from dataLayer"""
        self.logger.debug("🎯 Detecting GTM events from dataLayer...")
        
        gtm_events_script = """
        () => {
            try {
                if (typeof window.dataLayer === 'undefined') {
                    return [];
                }
                
                const events = window.dataLayer
                    .filter(item => item && item.event)
                    .map(item => item.event);
                
                return [...new Set(events)];
            } catch (error) {
                return [];
            }
        }
        """
        
        try:
            events = await page.evaluate(gtm_events_script)
            self.logger.debug(f" Found {len(events)} unique GTM events")
            return events
            
        except Exception as e:
            self.logger.error(f" Error detecting GTM events: {e}")
            return []
    
    async def _detect_third_party_trackers(self, page: Page) -> Tuple[List[str], List[str]]:
        """
        Detect 3rd party trackers using ONLY real TrackerDB data with time-based correlation
        NO hardcoded patterns used
        
        Returns:
            Tuple of (tracker_names, domain_names) - both lists for TrackerDB-identified requests only
        """
        self.logger.debug(" Detecting 3rd party trackers using real TrackerDB...")
        
        detected_trackers = []
        detected_domains = []
        
        # Only analyze if TrackerDB is loaded
        if not self.ghostery_db.is_loaded:
            self.logger.debug(" TrackerDB not loaded, NO tracker detection available")
            return detected_trackers, detected_domains
        
        # Check if GTM was detected (use GTM flag, not just load time)
        if not self.gtm_detected:
            self.logger.debug(" No GTM detected, skipping 3rd party tracker analysis")
            return detected_trackers, detected_domains
        
        # Analyze network requests for tracker patterns using ONLY TrackerDB
        gtm_attributed_requests = []
        
        for request in self.network_requests:
            url = request['url']
            timestamp = request['timestamp']
            
            # Skip GTM's own requests
            if 'googletagmanager.com' in url:
                continue
            
            # TIME-BASED CORRELATION: Check GTM attribution
            attribution_score = self._calculate_gtm_attribution(timestamp, url)
            
            if attribution_score > 0.5:  # Threshold for GTM attribution
                # Use ONLY TrackerDB to identify the tracker (NO hardcoded patterns)
                tracker_info = self.ghostery_db.identify_tracker(url)
                
                if tracker_info:
                    tracker_info['attribution_score'] = attribution_score
                    tracker_info['original_request_url'] = url  # Store original URL
                    if self.gtm_load_time:
                        tracker_info['timing_delta'] = timestamp - self.gtm_load_time
                    gtm_attributed_requests.append(tracker_info)
        
        # Extract unique tracker names and domains
        unique_trackers = set()
        unique_domains = set()
        
        for tracker_info in gtm_attributed_requests:
            tracker_name = tracker_info.get('name', tracker_info.get('matched_domain', 'Unknown'))
            original_url = tracker_info.get('original_request_url', '')
            
            # Add unique tracker names
            if tracker_name not in unique_trackers:
                unique_trackers.add(tracker_name)
                detected_trackers.append(tracker_name)
            
            # Extract domain from original URL and add to unique domains
            if original_url:
                try:
                    parsed_url = urlparse(original_url)
                    domain = parsed_url.netloc  # Gets "connect.facebook.net" from full URL
                    if domain and domain not in unique_domains:
                        unique_domains.add(domain)
                        detected_domains.append(domain)
                except Exception as e:
                    self.logger.debug(f"Error parsing domain from {original_url}: {e}")
        
        # Sort for consistent output
        detected_trackers.sort()
        detected_domains.sort()
        
        self.logger.debug(f" Real TrackerDB detection complete: {len(detected_trackers)} trackers, {len(detected_domains)} domains found")
        
        # Log detected trackers with attribution scores
        for i, tracker in enumerate(detected_trackers[:5]):
            # Find the attribution score for this tracker
            attribution_score = 0.0
            for tracker_info in gtm_attributed_requests:
                if tracker_info.get('name') == tracker or tracker_info.get('matched_domain') == tracker:
                    attribution_score = tracker_info.get('attribution_score', 0.0)
                    break
            
            self.logger.debug(f" Tracker {i+1}: {tracker} (confidence: {attribution_score:.2f})")
        
        return detected_trackers, detected_domains
    
    def _calculate_gtm_attribution(self, request_timestamp: float, url: str) -> float:
        """
        Calculate likelihood that this request was initiated by GTM using time-based correlation
        
        Args:
            request_timestamp: When the tracker request was made
            url: The tracker request URL
            
        Returns:
            Attribution score between 0 and 1 (higher = more likely GTM-initiated)
        """
        if self.gtm_load_time is None:
            # If no GTM load time recorded, use GTM detection flag + reasonable attribution
            if self.gtm_detected:
                return 0.7  # Medium confidence if GTM detected but no load time
            return 0.0
        
        # Calculate time difference
        time_delta = request_timestamp - self.gtm_load_time
        
        # Time-based attribution scoring
        if time_delta < 0:
            # Request happened BEFORE GTM loaded = definitely not GTM-initiated
            return 0.0
        elif time_delta <= 5:
            # Very high confidence - within 5 seconds of GTM load
            return 0.9
        elif time_delta <= 15:
            # High confidence - within 15 seconds of GTM load
            return 0.8
        elif time_delta <= 30:
            # Medium confidence - within 30 seconds of GTM load
            return 0.6
        else:
            # Lower confidence - more than 30 seconds after GTM load
            return 0.3
    
    def _create_timeout_result(self, url: str, start_time: float) -> Dict[str, Any]:
        """Create result for timed out requests"""
        return {
            'url': url,
            'timestamp': time.time(),
            'gtm_detected': False,
            'consent_mode': False,
            'gtm_events': 'not_applicable',
            'third_party_trackers': 'not_applicable',
            'third_party_domains_list': 'not_applicable',
            'third_party_domains_count': 0,
            'trackerdb_status': self.ghostery_db.get_loading_status(),
            'status': 'timeout',
            'google_urls_count': 0,
            'analysis_time': time.time() - start_time,
            'raw_urls': [],
            'error': 'Page load timeout'
        }
    
    def _create_error_result(self, url: str, error: str, start_time: float) -> Dict[str, Any]:
        """Create result for errors"""
        return {
            'url': url,
            'timestamp': time.time(),
            'gtm_detected': False,
            'consent_mode': False,
            'gtm_events': 'not_applicable',
            'third_party_trackers': 'not_applicable',
            'third_party_domains_list': 'not_applicable',
            'third_party_domains_count': 0,
            'trackerdb_status': self.ghostery_db.get_loading_status(),
            'status': 'error',
            'google_urls_count': 0,
            'analysis_time': time.time() - start_time,
            'raw_urls': [],
            'error': error
        }


# Test function
async def test_clean_detector():
    """Test the clean detector with ONLY real TrackerDB integration"""
    from playwright.async_api import async_playwright
    
    print(" Testing Clean GTM Detection with ONLY Real Ghostery TrackerDB...")
    print("="*80)
    
    test_urls = [
        "https://neetcode.io",  # Known GTM site
        "https://www.zoopla.co.uk/to-rent/map/property/3-bedrooms/edinburgh-county/?keywords=hmo&price_frequency=per_month&price_max=1750&q=edinburgh&radius=3&search_source=to-rent",
    ]
    
    # Initialize detector
    detector = StealthDetector(debug_mode=True)
    
    # Initialize TrackerDB
    await detector.initialize()
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-web-security'
                ]
            )
            
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = await context.new_page()
            
            for url in test_urls:
                print(f"\n Testing clean detection on: {url}")
                print("-" * 80)
                
                result = await detector.analyze_website(page, url)
                
                # Print comprehensive summary
                print(f" Results:")
                print(f"   GTM Detected: {' YES' if result['gtm_detected'] else ' NO'}")
                print(f"   Consent Mode: {' YES' if result['consent_mode'] else ' NO'}")
                print(f"   Status: {result['status']}")
                print(f"   Google URLs: {result['google_urls_count']}")
                print(f"   Analysis Time: {result['analysis_time']:.2f}s")
                
                # Show TrackerDB status
                trackerdb_status = result['trackerdb_status']
                print(f"   TrackerDB: {trackerdb_status['pattern_count']} patterns from {trackerdb_status['data_source']}")
                
                # Show tracker detection results
                if result['third_party_trackers'] != 'not_applicable':
                    trackers = result['third_party_trackers']
                    domains = result['third_party_domains_list']
                    if trackers:
                        print(f"    3rd Party Trackers: {', '.join(trackers[:5])}{'...' if len(trackers) > 5 else ''}")
                        print(f"    Total 3rd Party Trackers: {len(trackers)}")
                        print(f"    3rd Party Domains: {len(domains)}")
                        print(f"    Sample Domains: {', '.join([d[:50] + '...' if len(d) > 50 else d for d in domains[:3]])}")
                    else:
                        if not trackerdb_status['is_loaded']:
                            print(f"    3rd Party Trackers: TrackerDB not loaded")
                        else:
                            print(f"    3rd Party Trackers: None detected")
                else:
                    print(f"    3rd Party Trackers: Not applicable (no GTM)")
            
            await browser.close()
            
    except Exception as e:
        print(f" Test failed: {str(e)}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_clean_detector())