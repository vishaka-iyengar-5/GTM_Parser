#!/usr/bin/env python3
"""
GTM Detector - Google Tag Manager Detection Module
Detects Google Tag Manager implementation on websites
"""

import re #Regular expressions - Pattern matching and text extraction from HTML, URLs, or JavaScript code
import time #Adding delays, measuring page load times, or timestamping data collection
import logging
import asyncio
import json
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, parse_qs #URL Parsing - Breaking down URLs to analyze tracking parameters
from playwright.async_api import Page, Response #Controlling a real browser to interact with websites, capture network traffic, and simulate user behavior


class GTMDetector:
    """Core GTM detection functionality"""
    
    def __init__(self, debug_mode: bool = True):
        self.debug_mode = debug_mode
        self.logger = self._setup_logging()
        
        # GTM detection patterns
        self.gtm_patterns = {
            'network': [
                r'googletagmanager\.com/gtm\.js',
                r'googletagmanager\.com/gtag/js',
                r'googletagmanager\.com/ns\.html',
                r'www\.googletagmanager\.com',
            ],
            'dom': [
                r'googletagmanager\.com',
                r'GTM-[A-Z0-9]+',
                r'G-[A-Z0-9]+',
            ],
            'javascript': [
                'dataLayer',
                'gtag',
                'google_tag_manager',
                'GoogleAnalyticsObject'
            ]
        }
        
        # Container ID patterns
        self.container_patterns = {
            'gtm': r'GTM-[A-Z0-9]{6,8}', 			
            'gtag': r'G-[A-Z0-9]{10}',    				# GA4 IDs are typically exactly 10 chars
            'ga': r'UA-[0-9]{4,9}-[0-9]{1,4}',  			# Legacy GA (being phased out)
            'gtm_server': r'GTM-[A-Z0-9]{6,8}/gtm\.js',  	# Server-side GTM
            'gtm_amp': r'GTM-[A-Z0-9]{6,8}',  			# AMP GTM (same format)
            'gtm_noscript': r'GTM-[A-Z0-9]{6,8}',  		# Noscript fallback
        }
        
        # GTM-related cookie patterns
        self.gtm_cookie_patterns = [
            r'^_ga$',           # Google Analytics main cookie
            r'^_gid$',          # Google Analytics session cookie
            r'^_gat.*',         # Google Analytics throttling cookies
            r'^_gtag_.*',       # gtag.js cookies
            r'^_gcl_.*',        # Google Click Identifier cookies
            r'^_dc_gtm_.*',     # DoubleClick GTM cookies
            r'^__utma$',        # Legacy GA cookies
            r'^__utmb$',
            r'^__utmc$',
            r'^__utmz$',
        ]
        
        # Console log patterns for GTM
        self.gtm_console_patterns = [
            r'Google Tag Manager',
            r'gtag\(',
            r'dataLayer',
            r'GTM-[A-Z0-9]+',
            r'google_tag_manager',
            r'googletagmanager\.com',
            r'Analytics tracking',
        ]
        
        # Data attribute patterns for GTM tracking
        self.gtm_data_attribute_patterns = [
            r'data-gtm.*',          # GTM-specific data attributes
            r'data-track.*',        # Generic tracking attributes
            r'data-analytics.*',    # Analytics-related attributes
            r'data-ga-.*',          # Google Analytics attributes
            r'data-event.*',        # Event tracking attributes
            r'data-category.*',     # Category tracking
            r'data-action.*',       # Action tracking
            r'data-label.*',        # Label tracking
            r'data-value.*',        # Value tracking
            r'data-click.*',        # Click tracking
            r'data-scroll.*',       # Scroll tracking
            r'data-form.*',         # Form tracking
        ]
        
        # Storage for detection results
        self.reset_detection_data() #Initialize the storage so that the data from the previous website does not get mixed with the next one
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('GTMDetector')
        logger.setLevel(logging.DEBUG if self.debug_mode else logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def reset_detection_data(self):
        """Reset detection data for new analysis"""
        self.detection_data = {
            'network_requests': [],
            'dom_elements': [],
            'javascript_objects': [],
            'container_ids': [],
            'cookies': [],
            'console_logs': [],
            'data_attributes': [],
            'loading_pattern': None,
            'timing': {
                'first_gtm_request': None,
                'page_load_start': None,
                'detection_duration': None
            }
        }
    
    async def _setup_console_monitoring(self, page: Page):
        """Setup console log monitoring for GTM-related messages"""
        def handle_console_msg(msg):
            try:
                text = msg.text
                msg_type = msg.type
                
                # Check if console message contains GTM patterns
                for pattern in self.gtm_console_patterns:
                    if re.search(pattern, text, re.IGNORECASE):
                        console_data = {
                            'text': text,
                            'type': msg_type,
                            'timestamp': time.time(),
                            'pattern_matched': pattern
                        }
                        self.detection_data['console_logs'].append(console_data)
                        self.logger.debug(f"📜 GTM console message: {text[:100]}...")
                        break
            except Exception as e:
                self.logger.debug(f"Error processing console message: {e}")
        
        page.on('console', handle_console_msg)

    # Entire GTM detection process for a single website
    async def analyze_website(self, page: Page, url: str) -> Dict[str, Any]:
        """
        Main analysis function for a website
        
        Args:
            page: Playwright page object
            url: Website URL to analyze
            
        Returns:
            Dictionary with complete GTM detection results
        """

        self.logger.info(f"🔍 Starting GTM analysis for: {url}")
        start_time = time.time() #Measure how long the entire analysis takes
        self.reset_detection_data() #clears all previous detection results
        self.detection_data['timing']['page_load_start'] = start_time

        # Setup Network Monitoring on website BEFORE navigation begins
        await self._setup_network_monitoring(page)
        
        # Setup Console Monitoring BEFORE navigation begins
        await self._setup_console_monitoring(page)
        
        try:
            # Navigate to the website
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            # Wait for dynamic content to load
            await page.wait_for_timeout(3000)
            
            # Perform all detection methods
            await self._detect_network_gtm(page) #Network requests
            await self._detect_dom_gtm(page) #DOM Requests
            await self._detect_javascript_gtm(page) #Javascript objects - ENHANCED
            await self._detect_gtm_cookies(page) #Cookies Analysis 
            await self._analyze_console_logs() #Console Logs
            await self._detect_data_attributes(page) #Data Attributes Detection
            await self._extract_container_ids() #Container ID Extraction
            await self._analyze_loading_pattern() #Loading patterns - Sync vs async
            
            # Calculate detection duration
            end_time = time.time()
            self.detection_data['timing']['detection_duration'] = end_time - start_time
            
            # Generate final results
            results = self._generate_results(url)
            self.logger.info(f"✅ Analysis complete for {url}")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Error analyzing {url}: {str(e)}")
            return self._generate_error_result(url, str(e))
    
    async def _setup_network_monitoring(self, page: Page):
        """Setup network request monitoring"""
        def handle_response(response: Response):
            url = response.url
            # Check for GTM patterns in network requests
            for pattern in self.gtm_patterns['network']:
                if re.search(pattern, url, re.IGNORECASE):
                    request_data = {
                        'url': url,
                        'method': response.request.method,
                        'status': response.status,
                        'headers': dict(response.headers),
                        'timestamp': time.time()
                    }
                    
                    # Record first GTM request timing
                    if not self.detection_data['timing']['first_gtm_request']:
                        self.detection_data['timing']['first_gtm_request'] = time.time()
                    
                    self.detection_data['network_requests'].append(request_data)
                    self.logger.debug(f"📡 GTM network request detected: {url}")
                    break
        
        page.on('response', handle_response)
    
    async def _detect_network_gtm(self, page: Page):
        """Detect GTM through network requests"""
        self.logger.debug("🌐 Analyzing network requests for GTM...") #analyzes what was already collected - PASSIVE ANALYSIS
        
        # Network requests are captured via the response handler
        # Additional analysis can be added here if needed
        
        if self.detection_data['network_requests']:
            self.logger.info(f"📡 Found {len(self.detection_data['network_requests'])} GTM network requests")
    
    async def _detect_dom_gtm(self, page: Page): #inspects the HTML structure (DOM) of the webpage to find GTM-related elements
        """Detect GTM through DOM inspection"""
        self.logger.debug("🔍 Analyzing DOM for GTM elements...")
        
        try:
            # Check for GTM script tags
            script_tags = await page.query_selector_all('script') #both external scripts and inline JavaScript included
            
            for script in script_tags:
                src = await script.get_attribute('src')
                content = await script.inner_text() if not src else ""
                
                # Check script src for GTM patterns
                if src:
                    for pattern in self.gtm_patterns['dom']:
                        if re.search(pattern, src, re.IGNORECASE):
                            self.detection_data['dom_elements'].append({
                                'type': 'script_src',
                                'content': src,
                                'pattern_matched': pattern
                            })
                            self.logger.debug(f"🏷️ GTM script src found: {src}")
                
                # Check script content for GTM patterns
                if content:
                    for pattern in self.gtm_patterns['dom']:
                        if re.search(pattern, content, re.IGNORECASE):
                            self.detection_data['dom_elements'].append({
                                'type': 'script_content',
                                'content': content[:200] + "..." if len(content) > 200 else content,
                                'pattern_matched': pattern
                            })
                            self.logger.debug(f"🏷️ GTM pattern in script content: {pattern}")
            
            # Check for GTM noscript tags
            noscript_tags = await page.query_selector_all('noscript')
            for noscript in noscript_tags:
                content = await noscript.inner_html()
                if 'googletagmanager.com/ns.html' in content:
                    self.detection_data['dom_elements'].append({
                        'type': 'noscript',
                        'content': content,
                        'pattern_matched': 'noscript_gtm'
                    })
                    self.logger.debug("🏷️ GTM noscript tag found")
            
            if self.detection_data['dom_elements']:
                self.logger.info(f"🏷️ Found {len(self.detection_data['dom_elements'])} GTM DOM elements")
                
        except Exception as e:
            self.logger.error(f"❌ Error in DOM detection: {str(e)}")

    #ENHANCED - Detects GTM by examining JavaScript objects and function calls on the webpage - Catches GTM that's loaded dynamically after page load
    async def _detect_javascript_gtm(self, page: Page):
        """Detect GTM through JavaScript objects with enhanced analysis"""
        self.logger.debug("🔧 Analyzing JavaScript objects for GTM...")
        
        try:
            # ENHANCED JavaScript objects detection with detailed dataLayer analysis - Creates a JavaScript script that will run inside the browser to check for GTM objects
            js_check_script = """ 					
            () => {
                const gtmObjects = [];
                
                // ENHANCED dataLayer analysis - core communication method between website and GTM
                if (typeof window.dataLayer !== 'undefined') {
                    const dataLayerInfo = {
                        name: 'dataLayer',
                        type: typeof window.dataLayer,
                        length: Array.isArray(window.dataLayer) ? window.dataLayer.length : 'N/A',
                        sample: Array.isArray(window.dataLayer) && window.dataLayer.length > 0 ? window.dataLayer[0] : null,
                        // NEW: Enhanced analysis
                        eventTypes: [],
                        hasUserData: false,
                        hasEcommerce: false,
                        hasCustomDimensions: false,
                        hasConsentData: false,
                        hasPurchaseData: false,
                        hasPersonalData: false,
                        commonEvents: {}
                    };
                    
                    if (Array.isArray(window.dataLayer)) {
                        // Analyze all dataLayer events for tracking extent
                        window.dataLayer.forEach(item => {
                            if (item && typeof item === 'object') {
                                // Track event types
                                if (item.event) {
                                    dataLayerInfo.eventTypes.push(item.event);
                                    // Count occurrences of each event type
                                    dataLayerInfo.commonEvents[item.event] = (dataLayerInfo.commonEvents[item.event] || 0) + 1;
                                }
                                
                                // Check for user data collection
                                if (item.user_id || item.userId || item.user_email || item.customer_id) {
                                    dataLayerInfo.hasUserData = true;
                                    dataLayerInfo.hasPersonalData = true;
                                }
                                
                                // Check for e-commerce tracking
                                if (item.ecommerce || item.purchase || item.transaction_id || item.items) {
                                    dataLayerInfo.hasEcommerce = true;
                                }
                                
                                // Check for purchase/conversion data
                                if (item.purchase || item.transaction_id || item.value || item.revenue) {
                                    dataLayerInfo.hasPurchaseData = true;
                                }
                                
                                // Check for custom dimensions
                                if (item.custom_map || item.customDimensions || item.custom_parameters) {
                                    dataLayerInfo.hasCustomDimensions = true;
                                }
                                
                                // Check for consent data
                                if (item.consent || item.consent_state || item.gtm_consent || item.analytics_storage || item.ad_storage) {
                                    dataLayerInfo.hasConsentData = true;
                                }
                                
                                // Check for personal/sensitive data
                                if (item.email || item.phone || item.address || item.name || item.user_properties) {
                                    dataLayerInfo.hasPersonalData = true;
                                }
                            }
                        });
                        
                        // Remove duplicates and limit to first 15 event types
                        dataLayerInfo.eventTypes = [...new Set(dataLayerInfo.eventTypes)].slice(0, 15);
                        
                        // Get top 5 most common events
                        const sortedEvents = Object.entries(dataLayerInfo.commonEvents)
                            .sort(([,a], [,b]) => b - a)
                            .slice(0, 5);
                        dataLayerInfo.topEvents = Object.fromEntries(sortedEvents);
                    }
                    
                    gtmObjects.push(dataLayerInfo);
                }
                
                // Check for gtag function
                if (typeof window.gtag !== 'undefined') {
                    gtmObjects.push({
                        name: 'gtag',
                        type: typeof window.gtag,
                        length: 'N/A',
                        sample: null
                    });
                }
                
                // Check for google_tag_manager
                if (typeof window.google_tag_manager !== 'undefined') {
                    gtmObjects.push({
                        name: 'google_tag_manager',
                        type: typeof window.google_tag_manager,
                        length: 'N/A',
                        sample: null
                    });
                }
                
                // Check for GoogleAnalyticsObject
                if (typeof window.GoogleAnalyticsObject !== 'undefined') {
                    gtmObjects.push({
                        name: 'GoogleAnalyticsObject',
                        type: typeof window.GoogleAnalyticsObject,
                        length: 'N/A',
                        sample: window.GoogleAnalyticsObject
                    });
                }
                
                return gtmObjects;
            }
            """
            
            js_objects = await page.evaluate(js_check_script)
            self.detection_data['javascript_objects'] = js_objects
            
            # ENHANCED gtag calls detection
            enhanced_gtag_script = """ 							
            () => {
                const scripts = Array.from(document.querySelectorAll('script'));
                const gtagCalls = {
                    config: [],
                    event: [],
                    set: [],
                    consent: [],
                    other: [],
                    summary: {
                        totalCalls: 0,
                        hasConsentCalls: false,
                        hasEventTracking: false,
                        hasConfigCalls: false,
                        hasSetCalls: false
                    }
                };
                
                scripts.forEach(script => {
                    const content = script.textContent || script.innerText || '';
                    
                    const patterns = {
                        config: /gtag\\\\s*\\\\(\\\\s*['"]config['"],\\\\s*['"]([^'"]+)['"][^)]*\\\\)/g,
                        event: /gtag\\\\s*\\\\(\\\\s*['"]event['"],\\\\s*['"]([^'"]+)['"][^)]*\\\\)/g,
                        set: /gtag\\\\s*\\\\(\\\\s*['"]set['"],\\\\s*({[^}]+}|['"][^'"]+['"])/g,
                        consent: /gtag\\\\s*\\\\(\\\\s*['"]consent['"],\\\\s*['"]([^'"]+)['"][^)]*\\\\)/g
                    };
                    
                    for (let [type, pattern] of Object.entries(patterns)) {
                        let matches = [...content.matchAll(pattern)];
                        gtagCalls[type].push(...matches.map(m => m[0]));
                        gtagCalls.summary.totalCalls += matches.length;
                        
                        if (matches.length > 0) {
                            switch(type) {
                                case 'config':
                                    gtagCalls.summary.hasConfigCalls = true;
                                    break;
                                case 'event':
                                    gtagCalls.summary.hasEventTracking = true;
                                    break;
                                case 'consent':
                                    gtagCalls.summary.hasConsentCalls = true;
                                    break;
                                case 'set':
                                    gtagCalls.summary.hasSetCalls = true;
                                    break;
                            }
                        }
                    }
                    
                    const otherPattern = /gtag\\\\s*\\\\(\\\\s*['"](?!config|event|set|consent)[^'"]+['"][^)]*\\\\)/g;
                    let otherMatches = [...content.matchAll(otherPattern)];
                    gtagCalls.other.push(...otherMatches.map(m => m[0]));
                    gtagCalls.summary.totalCalls += otherMatches.length;
                });
                
                return gtagCalls;
            }
            """
            
            gtag_calls = await page.evaluate(enhanced_gtag_script)
            
            # Add enhanced gtag calls to detection data if found
            if gtag_calls['summary']['totalCalls'] > 0:
                # Add enhanced gtag calls as a new type of JavaScript object detection
                js_objects.append({
                    'name': 'enhanced_gtag_calls',
                    'type': 'enhanced_function_calls',
                    'length': gtag_calls['summary']['totalCalls'],
                    'summary': gtag_calls['summary'],
                    'config_calls': gtag_calls['config'][:3], # First 3 config calls
                    'event_calls': gtag_calls['event'][:3], # First 3 event calls
                    'consent_calls': gtag_calls['consent'][:3], # First 3 consent calls
                    'set_calls': gtag_calls['set'][:3], # First 3 set calls
                    'other_calls': gtag_calls['other'][:3] # First 3 other calls
                })
                self.detection_data['javascript_objects'] = js_objects
                self.logger.debug(f"🔧 Found {gtag_calls['summary']['totalCalls']} total gtag calls")
                self.logger.debug(f"🔧 Consent calls: {len(gtag_calls['consent'])}, Event calls: {len(gtag_calls['event'])}")
            
            if js_objects:
                self.logger.info(f"🔧 Found {len(js_objects)} GTM JavaScript objects")
                for obj in js_objects:
                    self.logger.debug(f"🔧 JS Object: {obj['name']} ({obj['type']})")
                    
                    # Enhanced logging for dataLayer analysis
                    if obj['name'] == 'dataLayer' and 'eventTypes' in obj:
                        self.logger.debug(f"🔧 DataLayer events: {len(obj['eventTypes'])} types")
                        self.logger.debug(f"🔧 E-commerce tracking: {obj.get('hasEcommerce', False)}")
                        self.logger.debug(f"🔧 User data collection: {obj.get('hasUserData', False)}")
                        self.logger.debug(f"🔧 Consent data: {obj.get('hasConsentData', False)}")
                    
                    # Enhanced logging for gtag analysis
                    if obj['name'] == 'enhanced_gtag_calls' and 'summary' in obj:
                        summary = obj['summary']
                        self.logger.debug(f"🔧 Gtag summary - Config: {summary['hasConfigCalls']}, Events: {summary['hasEventTracking']}, Consent: {summary['hasConsentCalls']}")
            
        except Exception as e:
            self.logger.error(f"❌ Error in enhanced JavaScript detection: {str(e)}")

    async def _detect_gtm_cookies(self, page: Page):
        """Detect GTM-related cookies"""
        self.logger.debug("🍪 Analyzing cookies for GTM...")
        
        try:
            # Get all cookies from the page
            cookies = await page.context.cookies()
            
            gtm_cookies = []
            for cookie in cookies:
                cookie_name = cookie['name']
                
                # Check if cookie matches GTM patterns
                for pattern in self.gtm_cookie_patterns:
                    if re.match(pattern, cookie_name):
                        gtm_cookie_data = {
                            'name': cookie_name,
                            'value': cookie['value'][:50] + "..." if len(cookie['value']) > 50 else cookie['value'],
                            'domain': cookie['domain'],
                            'path': cookie['path'],
                            'pattern_matched': pattern,
                            'secure': cookie.get('secure', False),
                            'httpOnly': cookie.get('httpOnly', False)
                        }
                        gtm_cookies.append(gtm_cookie_data)
                        self.logger.debug(f"🍪 GTM cookie found: {cookie_name}")
                        break
            
            self.detection_data['cookies'] = gtm_cookies
            
            if gtm_cookies:
                self.logger.info(f"🍪 Found {len(gtm_cookies)} GTM-related cookies")
                
        except Exception as e:
            self.logger.error(f"❌ Error in cookie detection: {str(e)}")

    async def _analyze_console_logs(self):
        """Analyze captured console logs for additional insights"""
        self.logger.debug("📜 Analyzing console logs for GTM...")
        
        if self.detection_data['console_logs']:
            self.logger.info(f"📜 Found {len(self.detection_data['console_logs'])} GTM-related console messages")
            
            # Group by message type for better analysis
            log_types = {}
            for log in self.detection_data['console_logs']:
                msg_type = log['type']
                if msg_type not in log_types:
                    log_types[msg_type] = 0
                log_types[msg_type] += 1
            
            for log_type, count in log_types.items():
                self.logger.debug(f"📜 Console {log_type} messages: {count}")

    async def _detect_data_attributes(self, page: Page):
        """Detect GTM-related data attributes in HTML elements"""
        self.logger.debug("🏃 Analyzing data attributes for GTM tracking...")
        
        try:
            # Search for elements with GTM-related data attributes
            data_attribute_script = """
            () => {
                const gtmDataElements = [];
                const allElements = document.querySelectorAll('*');
                
                allElements.forEach(element => {
                    const attributes = {};
                    let hasGTMAttribute = false;
                    
                    # Check all attributes of the element
                    for (let attr of element.attributes) {
                        const attrName = attr.name.toLowerCase();
                        const attrValue = attr.value;
                        
                        // Check if attribute matches GTM patterns
                        const gtmPatterns = [
                            /^data-gtm.*/,
                            /^data-track.*/,
                            /^data-analytics.*/,
                            /^data-ga-.*/,
                            /^data-event.*/,
                            /^data-category.*/,
                            /^data-action.*/,
                            /^data-label.*/,
                            /^data-value.*/,
                            /^data-click.*/,
                            /^data-scroll.*/,
                            /^data-form.*/
                        ];
                        
                        for (let pattern of gtmPatterns) {
                            if (pattern.test(attrName)) {
                                attributes[attrName] = attrValue;
                                hasGTMAttribute = true;
                                break;
                            }
                        }
                    }
                    
                    // If element has GTM attributes, collect its info
                    if (hasGTMAttribute) {
                        gtmDataElements.push({
                            tagName: element.tagName.toLowerCase(),
                            id: element.id || null,
                            className: element.className || null,
                            attributes: attributes,
                            textContent: element.textContent ? element.textContent.trim().substring(0, 100) : null,
                            innerHTML: element.innerHTML ? element.innerHTML.substring(0, 200) : null
                        });
                    }
                });
                
                return gtmDataElements;
            }
            """
            
            data_elements = await page.evaluate(data_attribute_script)
            
            # Process and store the found data attributes
            processed_data_attributes = []
            for element in data_elements:
                # Match each attribute against our patterns to identify which pattern triggered
                for attr_name, attr_value in element['attributes'].items():
                    for pattern in self.gtm_data_attribute_patterns:
                        if re.match(pattern, attr_name, re.IGNORECASE):
                            processed_data_attributes.append({
                                'element_tag': element['tagName'],
                                'element_id': element['id'],
                                'element_class': element['className'],
                                'attribute_name': attr_name,
                                'attribute_value': attr_value[:100] + "..." if len(attr_value) > 100 else attr_value,
                                'pattern_matched': pattern,
                                'element_text': element['textContent'],
                                'element_html': element['innerHTML'][:150] + "..." if element['innerHTML'] and len(element['innerHTML']) > 150 else element['innerHTML']
                            })
                            self.logger.debug(f"🏃 GTM data attribute found: {attr_name}='{attr_value[:50]}...' on {element['tagName']}")
                            break
            
            self.detection_data['data_attributes'] = processed_data_attributes
            
            if processed_data_attributes:
                self.logger.info(f"🏃 Found {len(processed_data_attributes)} GTM-related data attributes")
                
                # Group by attribute type for analysis
                attr_types = {}
                for attr in processed_data_attributes:
                    attr_type = attr['attribute_name'].split('-')[1] if '-' in attr['attribute_name'] else attr['attribute_name']
                    if attr_type not in attr_types:
                        attr_types[attr_type] = 0
                    attr_types[attr_type] += 1
                
                for attr_type, count in attr_types.items():
                    self.logger.debug(f"🏃 Data attribute type '{attr_type}': {count} elements")
                    
        except Exception as e:
            self.logger.error(f"❌ Error in data attribute detection: {str(e)}")

    #Collects all GTM container IDs that were found across all the different detection methods and consolidates them into one list.
    async def _extract_container_ids(self):
        """Extract GTM container IDs from detected elements"""
        self.logger.debug("🆔 Extracting GTM container IDs...")
        
        container_ids = set()
        
        # Extract from network requests
        for request in self.detection_data['network_requests']:
            url = request['url']
            for container_type, pattern in self.container_patterns.items():
                matches = re.findall(pattern, url)
                for match in matches:
                    container_ids.add((match, container_type, 'network'))
        
        # Extract from DOM elements
        for element in self.detection_data['dom_elements']:
            content = element['content']
            for container_type, pattern in self.container_patterns.items():
                matches = re.findall(pattern, content)
                for match in matches:
                    container_ids.add((match, container_type, 'dom'))
        
        # Extract from console logs
        for log in self.detection_data['console_logs']:
            content = log['text']
            for container_type, pattern in self.container_patterns.items():
                matches = re.findall(pattern, content)
                for match in matches:
                    container_ids.add((match, container_type, 'console'))
        
        # Extract from data attributes
        for attr in self.detection_data['data_attributes']:
            content = f"{attr['attribute_name']} {attr['attribute_value']}"
            for container_type, pattern in self.container_patterns.items():
                matches = re.findall(pattern, content)
                for match in matches:
                    container_ids.add((match, container_type, 'data_attributes'))
        
        # Extract from enhanced JavaScript objects (NEW)
        for js_obj in self.detection_data['javascript_objects']:
            if js_obj['name'] == 'enhanced_gtag_calls':
                # Extract from all gtag call types
                all_calls = (js_obj.get('config_calls', []) + 
                           js_obj.get('event_calls', []) + 
                           js_obj.get('consent_calls', []) + 
                           js_obj.get('set_calls', []) + 
                           js_obj.get('other_calls', []))
                
                for call in all_calls:
                    for container_type, pattern in self.container_patterns.items():
                        matches = re.findall(pattern, call)
                        for match in matches:
                            container_ids.add((match, container_type, 'enhanced_gtag'))
        
        # Convert set back to list for JSON serialization
        self.detection_data['container_ids'] = [
            {'id': item[0], 'type': item[1], 'source': item[2]}
            for item in container_ids
        ]
        
        if self.detection_data['container_ids']:
            self.logger.info(f"🆔 Found {len(self.detection_data['container_ids'])} unique container IDs")
            for container in self.detection_data['container_ids']:
                self.logger.debug(f"🆔 Container: {container['id']} ({container['type']}) from {container['source']}")

    #Sync loading suggests tracking is prioritized over user experience. Could correlate to aggressive tracking
    async def _analyze_loading_pattern(self):
        """Analyze GTM loading pattern (sync vs async)"""
        self.logger.debug("⚡ Analyzing GTM loading patterns...")
        
        # Check DOM elements for async/sync patterns
        for element in self.detection_data['dom_elements']:
            if element['type'] == 'script_src':
                content = element['content']
                if 'async' in content.lower():
                    self.detection_data['loading_pattern'] = 'async'
                    self.logger.debug("⚡ Async loading pattern detected")
                    break
                elif 'googletagmanager.com' in content:
                    # If GTM script found without async, likely sync
                    self.detection_data['loading_pattern'] = 'sync'
                    self.logger.debug("⚡ Sync loading pattern detected")
        
        # If no clear pattern detected but GTM found, default to async (most common)
        if (not self.detection_data['loading_pattern'] and 
            (self.detection_data['network_requests'] or 
             self.detection_data['dom_elements'] or 
             self.detection_data['javascript_objects'])):
            self.detection_data['loading_pattern'] = 'async'
            self.logger.debug("⚡ Defaulting to async loading pattern")

    def _generate_results(self, url: str) -> Dict[str, Any]:
        """Generate final detection results with enhanced tracking analysis"""
        
        # Determine if GTM is detected
        gtm_detected = bool(
            self.detection_data['network_requests'] or
            self.detection_data['dom_elements'] or
            self.detection_data['javascript_objects'] or
            self.detection_data['cookies'] or
            self.detection_data['console_logs'] or
            self.detection_data['data_attributes']
        )
        
        # Determine detection methods used
        detection_methods = []
        if self.detection_data['network_requests']:
            detection_methods.append('network')
        if self.detection_data['dom_elements']:
            detection_methods.append('dom')
        if self.detection_data['javascript_objects']:
            detection_methods.append('javascript')
        if self.detection_data['cookies']:
            detection_methods.append('cookies')
        if self.detection_data['console_logs']:
            detection_methods.append('console')
        if self.detection_data['data_attributes']:
            detection_methods.append('data_attributes')
        
        # ENHANCED: Extract tracking analysis from enhanced dataLayer and gtag detection
        tracking_analysis = {
            'has_ecommerce_tracking': False,
            'has_user_data_collection': False,
            'has_consent_implementation': False,
            'has_event_tracking': False,
            'has_personal_data_collection': False,
            'event_types_count': 0,
            'gtag_calls_summary': {}
        }
        
        # Analyze enhanced JavaScript objects for tracking patterns
        for js_obj in self.detection_data['javascript_objects']:
            if js_obj['name'] == 'dataLayer' and 'hasEcommerce' in js_obj:
                tracking_analysis['has_ecommerce_tracking'] = js_obj.get('hasEcommerce', False)
                tracking_analysis['has_user_data_collection'] = js_obj.get('hasUserData', False)
                tracking_analysis['has_consent_implementation'] = js_obj.get('hasConsentData', False)
                tracking_analysis['has_personal_data_collection'] = js_obj.get('hasPersonalData', False)
                tracking_analysis['event_types_count'] = len(js_obj.get('eventTypes', []))
                
            elif js_obj['name'] == 'enhanced_gtag_calls' and 'summary' in js_obj:
                tracking_analysis['has_event_tracking'] = js_obj['summary'].get('hasEventTracking', False)
                tracking_analysis['has_consent_implementation'] = js_obj['summary'].get('hasConsentCalls', False)
                tracking_analysis['gtag_calls_summary'] = js_obj['summary']
        
        # Calculate confidence score
        confidence_score = self._calculate_confidence_score()
        
        results = {
            'url': url,
            'timestamp': time.time(),
            'gtm_detected': gtm_detected,
            'confidence_score': confidence_score,
            'detection_methods': detection_methods,
            'container_ids': [c['id'] for c in self.detection_data['container_ids']],
            'container_types': list(set(c['type'] for c in self.detection_data['container_ids'])),
            'loading_pattern': self.detection_data['loading_pattern'],
            'timing': self.detection_data['timing'],
            'tracking_analysis': tracking_analysis, # NEW: Enhanced tracking analysis
            'details': {
                'network_requests_count': len(self.detection_data['network_requests']),
                'dom_elements_count': len(self.detection_data['dom_elements']),
                'javascript_objects_count': len(self.detection_data['javascript_objects']),
                'cookies_count': len(self.detection_data['cookies']),
                'console_logs_count': len(self.detection_data['console_logs']),
                'data_attributes_count': len(self.detection_data['data_attributes']),
                'container_ids_count': len(self.detection_data['container_ids'])
            },
            'raw_data': self.detection_data if self.debug_mode else None
        }
        
        return results

    def _calculate_confidence_score(self) -> float:
        """Calculate confidence score for GTM detection"""
        
        # Definitive evidence (GTM is definitely present)
        if (self.detection_data['network_requests'] or
            self.detection_data['dom_elements']):
            return 1.0
        
        # Some evidence (accumulate weights)
        score = 0.0
        
        if self.detection_data['javascript_objects']:
            score += 0.4  # JavaScript objects detected
        
        if self.detection_data['data_attributes']:
            score += 0.15  # Data attributes detected
        
        if self.detection_data['cookies']:
            score += 0.10  # GTM cookies detected
        
        # If we have some evidence, return the accumulated score
        if score > 0:
            return score
        
        # Weak evidence only (unreliable)
        if (self.detection_data['console_logs'] or
            self.detection_data['container_ids']):
            return 0.2  # Very low confidence, might be false positive
        
        # No evidence at all
        return 0.0

    def _generate_error_result(self, url: str, error: str) -> Dict[str, Any]:
        """Generate error result when detection fails"""
        return {
            'url': url,
            'timestamp': time.time(),
            'gtm_detected': False,
            'confidence_score': 0.0,
            'detection_methods': [],
            'container_ids': [],
            'container_types': [],
            'loading_pattern': None,
            'tracking_analysis': {
                'has_ecommerce_tracking': False,
                'has_user_data_collection': False,
                'has_consent_implementation': False,
                'has_event_tracking': False,
                'has_personal_data_collection': False,
                'event_types_count': 0,
                'gtag_calls_summary': {}
            },
            'error': error,
            'timing': self.detection_data['timing']
        }


# Test function for GTM detector
async def test_gtm_detector():
    """Test the GTM detector with known GTM sites"""
    from playwright.async_api import async_playwright
    
    print("🔍 Testing GTM Detection...")
    
    test_urls = [
        "https://neetcode.io",           # Known GTM site
        "https://example.com",           # Basic site, likely no GTM
    ]
    
    detector = GTMDetector(debug_mode=True)
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            for url in test_urls:
                print(f"\n{'='*60}")
                print(f"🌐 Analyzing GTM for: {url}")
                print(f"{'='*60}")
                
                result = await detector.analyze_website(page, url)
                print_gtm_summary(result)
            
            await browser.close()
            
    except Exception as e:
        print(f"❌ GTM detection test failed: {str(e)}")


def print_gtm_summary(result):
    """Print GTM detection summary"""
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
    
    if 'tracking_analysis' in result:
        tracking = result['tracking_analysis']
        print(f"   E-commerce Tracking: {'✅' if tracking.get('has_ecommerce_tracking') else '❌'}")
        print(f"   User Data Collection: {'✅' if tracking.get('has_user_data_collection') else '❌'}")
        print(f"   Consent Implementation: {'✅' if tracking.get('has_consent_implementation') else '❌'}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_gtm_detector())