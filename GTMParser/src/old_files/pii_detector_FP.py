#!/usr/bin/env python3
"""
Enhanced PII Detector - Personal Identifiable Information Detection Module
Detects PII leakage in network requests, URLs, cookies, storage, DOM content, and third-party transfers
"""

import re
import time
import logging
import json
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urlparse, parse_qs, unquote
from playwright.async_api import Page, Response


class PIIDetector:
    """Enhanced PII detection with comprehensive coverage"""
    
    def __init__(self, debug_mode: bool = True):
        self.debug_mode = debug_mode
        self.logger = self._setup_logging()
        self.main_domain = None  # Set during analysis for third-party detection
        
        # PII detection patterns with validation
        self.pii_patterns = {
            # Authentication-Related PII
            'email': {
                'params': [r'email', r'e', r'user_email', r'customer_email', r'mail'],
                'regex': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                'validation': self._validate_email
            },
            'username': {
                'params': [r'user', r'username', r'user_name', r'login', r'userid', r'user_id'],
                'regex': r'^[a-zA-Z0-9._-]{3,30}$',
                'validation': self._validate_username
            },
            'password': {
                'params': [r'password', r'pwd', r'pass', r'passwd', r'user_password'],
                'regex': r'.{6,}',  # Any string 6+ chars
                'validation': self._validate_password
            },
            'token': {
                'params': [r'token', r'sessionid', r'session_id', r'auth', r'sid', r'access_token', r'auth_token'],
                'regex': r'^[A-Za-z0-9+/=._-]{16,}$',
                'validation': self._validate_token
            },
            'api_key': {
                'params': [r'apikey', r'api_key', r'key', r'client_secret', r'secret', r'client_id'],
                'regex': r'^[A-Za-z0-9._-]{16,}$',
                'validation': self._validate_api_key
            },
            'jwt': {
                'params': [r'jwt', r'bearer', r'authorization'],
                'regex': r'^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]*$',
                'validation': self._validate_jwt
            },
            
            # Personal Information
            'full_name': {
                'params': [r'name', r'full_name', r'fullname', r'customer_name', r'user_name'],
                'regex': r'^[A-Za-z\s]{2,50}$',
                'validation': self._validate_name
            },
            'first_name': {
                'params': [r'first', r'first_name', r'firstname', r'fname'],
                'regex': r'^[A-Za-z]{2,30}$',
                'validation': self._validate_name
            },
            'last_name': {
                'params': [r'last', r'last_name', r'lastname', r'lname', r'surname'],
                'regex': r'^[A-Za-z]{2,30}$',
                'validation': self._validate_name
            },
            'phone': {
                'params': [r'phone', r'mobile', r'tel', r'telephone', r'cell', r'phone_number'],
                'regex': r'^\+?[1-9]\d{1,14}$',
                'validation': self._validate_phone
            },
            'address': {
                'params': [r'address', r'street', r'addr', r'street_address', r'address1'],
                'regex': r'^[A-Za-z0-9\s,.-]{5,100}$',
                'validation': self._validate_address
            },
            'postal_code': {
                'params': [r'zip', r'postal', r'postal_code', r'zipcode', r'postcode'],
                'regex': r'^[A-Za-z0-9\s-]{3,10}$',
                'validation': self._validate_postal
            },
            'city': {
                'params': [r'city', r'town', r'locality'],
                'regex': r'^[A-Za-z\s.-]{2,50}$',
                'validation': self._validate_city
            },
            'state': {
                'params': [r'state', r'province', r'region', r'county'],
                'regex': r'^[A-Za-z\s.-]{2,50}$',
                'validation': self._validate_state
            },
            'date_of_birth': {
                'params': [r'dob', r'birth', r'birth_date', r'birthdate', r'date_of_birth'],
                'regex': r'^\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}|\d{2}-\d{2}-\d{4}$',
                'validation': self._validate_date
            },
            'ssn': {
                'params': [r'ssn', r'social', r'social_security', r'nid', r'national_id'],
                'regex': r'^\d{3}-\d{2}-\d{4}|\d{9}$',
                'validation': self._validate_ssn
            },
            
            # Financial Information
            'credit_card': {
                'params': [r'card', r'cc', r'credit', r'credit_card', r'card_number'],
                'regex': r'^\d{13,19}$',
                'validation': self._validate_credit_card
            },
            'bank_account': {
                'params': [r'account', r'bank_account', r'account_number', r'routing'],
                'regex': r'^\d{8,17}$',
                'validation': self._validate_bank_account
            },
            'iban': {
                'params': [r'iban', r'international_account'],
                'regex': r'^[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}$',
                'validation': self._validate_iban
            },
            
            # E-commerce Specific
            'customer_id': {
                'params': [r'customer_id', r'cust_id', r'customer', r'client_id'],
                'regex': r'^[A-Za-z0-9_-]{3,50}$',
                'validation': self._validate_customer_id
            },
            'order_id': {
                'params': [r'order_id', r'order', r'transaction_id', r'purchase_id'],
                'regex': r'^[A-Za-z0-9_-]{3,50}$',
                'validation': self._validate_order_id
            },
            
            # Device/Metadata
            'ip_address': {
                'params': [r'ip', r'ip_address', r'client_ip', r'remote_ip'],
                'regex': r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$',
                'validation': self._validate_ip
            },
            'mac_address': {
                'params': [r'mac', r'mac_address', r'device_mac'],
                'regex': r'^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$',
                'validation': self._validate_mac
            },
            'device_id': {
                'params': [r'deviceid', r'device_id', r'uid', r'unique_id', r'device_uuid'],
                'regex': r'^[A-Za-z0-9_-]{8,}$',
                'validation': self._validate_device_id
            }
        }
        
        # Known third-party domains that commonly receive PII
        self.known_third_party_domains = {
            # Analytics
            'google-analytics.com', 'googletagmanager.com', 'doubleclick.net',
            'facebook.com', 'facebook.net', 'fbcdn.net',
            'twitter.com', 'linkedin.com', 'pinterest.com',
            # Ad networks
            'googlesyndication.com', 'adsystem.amazon.com',
            # Other tracking
            'hotjar.com', 'fullstory.com', 'loggly.com', 'mixpanel.com'
        }
        
        # Storage for detection results
        self.reset_detection_data()
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('PIIDetector')
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
            'url_parameters': [],
            'request_headers': [],
            'request_bodies': [],
            'response_headers': [],
            'response_bodies': [],  # NEW
            'cookies': [],
            'local_storage': [],
            'session_storage': [],
            'html_content': [],     # NEW
            'dom_variables': [],    # NEW
            'timing': {
                'detection_start': None,
                'detection_duration': None
            },
            'summary': {
                'total_pii_found': 0,
                'pii_types_found': set(),
                'high_risk_pii': 0,
                'medium_risk_pii': 0,
                'low_risk_pii': 0,
                'third_party_leaks': 0  # NEW
            }
        }
    
    # Validation methods for different PII types
    def _validate_email(self, value: str) -> bool:
        """Validate email format"""
        if not value or '@' not in value:
            return False
        parts = value.split('@')
        if len(parts) != 2:
            return False
        local, domain = parts
        if len(local) < 1 or len(domain) < 3:
            return False
        if '.' not in domain:
            return False
        return True
    
    def _validate_username(self, value: str) -> bool:
        """Validate username format"""
        if not value or len(value) < 3 or len(value) > 30:
            return False
        # Should not be just 'user' or 'username'
        if value.lower() in ['user', 'username', 'login', 'userid']:
            return False
        return True
    
    def _validate_password(self, value: str) -> bool:
        """Validate password (basic check)"""
        if not value or len(value) < 6:
            return False
        # Should not be just 'password' or 'pass'
        if value.lower() in ['password', 'pass', 'pwd', 'passwd']:
            return False
        return True
    
    def _validate_token(self, value: str) -> bool:
        """Validate token format"""
        if not value or len(value) < 16:
            return False
        # Should not be just parameter name
        if value.lower() in ['token', 'sessionid', 'auth', 'sid']:
            return False
        return True
    
    def _validate_api_key(self, value: str) -> bool:
        """Validate API key format"""
        if not value or len(value) < 16:
            return False
        if value.lower() in ['apikey', 'key', 'secret']:
            return False
        return True
    
    def _validate_jwt(self, value: str) -> bool:
        """Validate JWT token format"""
        if not value:
            return False
        parts = value.split('.')
        if len(parts) != 3:
            return False
        # Each part should be base64-like
        for part in parts:
            if not re.match(r'^[A-Za-z0-9_-]+$', part):
                return False
        return True
    
    def _validate_name(self, value: str) -> bool:
        """Validate name format"""
        if not value or len(value) < 2:
            return False
        # Should not be just parameter name
        if value.lower() in ['name', 'first', 'last', 'firstname', 'lastname']:
            return False
        # Should contain letters and spaces only
        if not re.match(r'^[A-Za-z\s]+$', value):
            return False
        return True
    
    def _validate_phone(self, value: str) -> bool:
        """Validate phone number format"""
        if not value:
            return False
        # Remove common formatting
        clean_phone = re.sub(r'[^\d+]', '', value)
        if len(clean_phone) < 10 or len(clean_phone) > 15:
            return False
        # Should not be placeholder
        if value.lower() in ['phone', 'mobile', 'telephone']:
            return False
        return True
    
    def _validate_address(self, value: str) -> bool:
        """Validate address format"""
        if not value or len(value) < 5:
            return False
        if value.lower() in ['address', 'street', 'addr']:
            return False
        return True
    
    def _validate_postal(self, value: str) -> bool:
        """Validate postal code format"""
        if not value or len(value) < 3:
            return False
        if value.lower() in ['zip', 'postal', 'zipcode']:
            return False
        return True
    
    def _validate_city(self, value: str) -> bool:
        """Validate city name"""
        if not value or len(value) < 2:
            return False
        if value.lower() in ['city', 'town']:
            return False
        return True
    
    def _validate_state(self, value: str) -> bool:
        """Validate state/province name"""
        if not value or len(value) < 2:
            return False
        if value.lower() in ['state', 'province', 'region']:
            return False
        return True
    
    def _validate_date(self, value: str) -> bool:
        """Validate date format"""
        if not value:
            return False
        # Check common date formats
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',    # YYYY-MM-DD
            r'^\d{2}/\d{2}/\d{4}$',    # MM/DD/YYYY
            r'^\d{2}-\d{2}-\d{4}$',    # MM-DD-YYYY
        ]
        return any(re.match(pattern, value) for pattern in date_patterns)
    
    def _validate_ssn(self, value: str) -> bool:
        """Validate SSN format"""
        if not value:
            return False
        # Remove formatting
        clean_ssn = re.sub(r'[^\d]', '', value)
        if len(clean_ssn) != 9:
            return False
        # Should not be all zeros or test patterns
        if clean_ssn == '000000000' or clean_ssn == '123456789':
            return False
        return True
    
    def _validate_credit_card(self, value: str) -> bool:
        """Validate credit card using Luhn algorithm"""
        if not value:
            return False
        # Remove spaces and dashes
        clean_card = re.sub(r'[^\d]', '', value)
        if len(clean_card) < 13 or len(clean_card) > 19:
            return False
        
        # Luhn algorithm validation
        def luhn_check(card_num):
            def digits_of(n):
                return [int(d) for d in str(n)]
            digits = digits_of(card_num)
            odd_digits = digits[-1::-2]
            even_digits = digits[-2::-2]
            checksum = sum(odd_digits)
            for d in even_digits:
                checksum += sum(digits_of(d*2))
            return checksum % 10 == 0
        
        return luhn_check(clean_card)
    
    def _validate_bank_account(self, value: str) -> bool:
        """Validate bank account number"""
        if not value or len(value) < 8:
            return False
        if value.lower() in ['account', 'bank_account']:
            return False
        return True
    
    def _validate_iban(self, value: str) -> bool:
        """Validate IBAN format"""
        if not value or len(value) < 15:
            return False
        # Basic IBAN format check
        return re.match(r'^[A-Z]{2}\d{2}[A-Z0-9]+$', value.upper()) is not None
    
    def _validate_customer_id(self, value: str) -> bool:
        """Validate customer ID"""
        if not value or len(value) < 3:
            return False
        if value.lower() in ['customer_id', 'customer', 'client_id']:
            return False
        return True
    
    def _validate_order_id(self, value: str) -> bool:
        """Validate order ID"""
        if not value or len(value) < 3:
            return False
        if value.lower() in ['order_id', 'order', 'transaction_id']:
            return False
        return True
    
    def _validate_ip(self, value: str) -> bool:
        """Validate IP address"""
        if not value:
            return False
        parts = value.split('.')
        if len(parts) != 4:
            return False
        try:
            for part in parts:
                if not 0 <= int(part) <= 255:
                    return False
            return True
        except ValueError:
            return False
    
    def _validate_mac(self, value: str) -> bool:
        """Validate MAC address"""
        if not value:
            return False
        # Standard MAC format
        return re.match(r'^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$', value) is not None
    
    def _validate_device_id(self, value: str) -> bool:
        """Validate device ID"""
        if not value or len(value) < 8:
            return False
        if value.lower() in ['deviceid', 'device_id', 'uid']:
            return False
        return True

    # NEW: Third-party domain detection methods
    def _is_third_party_domain(self, url: str) -> bool:
        """Check if URL is from a third-party domain"""
        if not self.main_domain:
            return False
            
        try:
            parsed_url = urlparse(url)
            parsed_main = urlparse(self.main_domain)
            
            # Extract base domain (remove subdomains)
            def get_base_domain(domain):
                if not domain:
                    return ""
                parts = domain.split('.')
                if len(parts) >= 2:
                    return '.'.join(parts[-2:])
                return domain
            
            url_domain = get_base_domain(parsed_url.netloc)
            main_base_domain = get_base_domain(parsed_main.netloc)
            
            return url_domain != main_base_domain
            
        except Exception:
            return False
    
    def _is_known_third_party_tracker(self, url: str) -> bool:
        """Check if URL belongs to known tracking/analytics services"""
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.lower()
            
            # Check against known third-party domains
            for known_domain in self.known_third_party_domains:
                if known_domain in domain:
                    return True
            return False
            
        except Exception:
            return False

    async def analyze_website(self, page: Page, url: str) -> Dict[str, Any]:
        """
        Enhanced PII analysis function for a website
        
        Args:
            page: Playwright page object
            url: Website URL to analyze
            
        Returns:
            Dictionary with complete PII detection results
        """
        self.logger.info(f"🔍 Starting enhanced PII analysis for: {url}")
        start_time = time.time()
        self.reset_detection_data()
        self.detection_data['timing']['detection_start'] = start_time
        
        # Set main domain for third-party detection
        self.main_domain = url
        
        # Setup network monitoring BEFORE navigation
        await self._setup_network_monitoring(page)
        await self._setup_response_monitoring(page)  # NEW
        
        try:
            # Navigate to the website
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            # Wait for dynamic content and requests
            await page.wait_for_timeout(3000)
            
            # Perform all PII detection methods
            await self._analyze_url_parameters(url)
            await self._analyze_cookies(page)
            await self._analyze_storage(page)
            
            # NEW: Enhanced analysis methods
            await self._analyze_html_content(page)
            await self._analyze_dom_variables(page)
            
            # Calculate detection duration
            end_time = time.time()
            self.detection_data['timing']['detection_duration'] = end_time - start_time
            
            # Generate final results
            results = self._generate_results(url)
            self.logger.info(f"✅ Enhanced PII analysis complete for {url}")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Error analyzing PII for {url}: {str(e)}")
            return self._generate_error_result(url, str(e))
    
    async def _setup_network_monitoring(self, page: Page):
        """Setup network request/response monitoring for PII"""
        def handle_request(request):
            try:
                # Analyze request URL and headers
                self._analyze_request_url(request.url)
                self._analyze_request_headers(dict(request.headers))
                
                # Analyze POST data if available
                if request.method == 'POST' and hasattr(request, 'post_data'):
                    post_data = request.post_data
                    if post_data:
                        self._analyze_request_body(post_data, request.url)
                        
            except Exception as e:
                self.logger.debug(f"Error processing request: {e}")
        
        def handle_response(response):
            try:
                # Analyze response headers
                self._analyze_response_headers(dict(response.headers))
                
            except Exception as e:
                self.logger.debug(f"Error processing response: {e}")
        
        page.on('request', handle_request)
        page.on('response', handle_response)
    
    # NEW: Response body monitoring
    async def _setup_response_monitoring(self, page: Page):
        """Setup response body analysis for PII"""
        async def handle_response(response):
            try:
                # Only analyze JSON responses to avoid large files
                content_type = response.headers.get('content-type', '')
                if 'application/json' in content_type:
                    try:
                        response_text = await response.text()
                        if response_text and len(response_text) < 50000:  # Limit size
                            # Try to parse as JSON
                            json_data = json.loads(response_text)
                            
                            # Recursively scan JSON for PII
                            self._scan_json_for_pii(json_data, response.url, 'response_body')
                            
                    except Exception as e:
                        self.logger.debug(f"Error parsing response body: {e}")
                        
            except Exception as e:
                self.logger.debug(f"Error analyzing response: {e}")
        
        page.on('response', handle_response)
    
    # NEW: HTML content analysis
    async def _analyze_html_content(self, page: Page):
        """Analyze HTML content for PII in DOM"""
        self.logger.debug("🔍 Analyzing HTML content for PII...")
        
        try:
            # Get full HTML content
            html_content = await page.content()
            
            # Extract text content (remove HTML tags but keep structure)
            import re
            text_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL)
            text_content = re.sub(r'<style[^>]*>.*?</style>', '', text_content, flags=re.DOTALL)
            text_content = re.sub(r'<[^>]+>', ' ', text_content)
            
            # Scan text content for PII patterns
            for pii_type, pattern_info in self.pii_patterns.items():
                matches = re.finditer(pattern_info['regex'], text_content)
                for match in matches:
                    value = match.group(0).strip()
                    if pattern_info['validation'](value):
                        pii_detection = {
                            'pii_type': pii_type,
                            'param_name': 'html_content',
                            'param_value': value[:50] + "..." if len(value) > 50 else value,
                            'source': 'html_dom',
                            'risk_level': self._classify_risk_level(pii_type),
                            'timestamp': time.time(),
                            'validation_passed': True,
                            'third_party_leak': False  # HTML content is on main domain
                        }
                        
                        self.detection_data['html_content'].append(pii_detection)
                        self._update_summary(pii_detection)
                        
                        self.logger.debug(f"🚨 PII in HTML: {pii_type} = {value[:20]}...")
                        
        except Exception as e:
            self.logger.error(f"❌ Error analyzing HTML content: {str(e)}")
    
    # NEW: DOM variables analysis
    async def _analyze_dom_variables(self, page: Page):
        """Analyze DOM variables and window properties for PII"""
        self.logger.debug("🌐 Analyzing DOM variables for PII...")
        
        try:
            # Common global variable patterns
            global_vars_script = """
            () => {
                const globals = {};
                
                // Check common patterns
                const patterns = [
                    'USER', 'user', 'currentUser', 'userData',
                    '__INITIAL_STATE__', '__INITIAL_DATA__', '__NEXT_DATA__',
                    'userInfo', 'profile', 'account', 'customer',
                    'config', 'appConfig', 'pageData'
                ];
                
                patterns.forEach(pattern => {
                    if (window[pattern]) {
                        try {
                            // Convert to JSON string to handle complex objects
                            globals[pattern] = JSON.parse(JSON.stringify(window[pattern]));
                        } catch (e) {
                            // If can't serialize, just get string representation
                            globals[pattern] = String(window[pattern]);
                        }
                    }
                });
                
                return globals;
            }
            """
            
            global_vars = await page.evaluate(global_vars_script)
            
            # Analyze each global variable
            for var_name, var_value in global_vars.items():
                self._scan_object_for_pii(var_value, var_name, 'dom_variable')
                        
        except Exception as e:
            self.logger.error(f"❌ Error analyzing DOM variables: {str(e)}")
    
    # NEW: Recursive object scanning
    def _scan_object_for_pii(self, data, parent_key: str, source_type: str):
        """Recursively scan object/dict for PII"""
        try:
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, (str, int)):
                        pii_found = self._detect_pii_in_param(key, str(value), source_type)
                        if pii_found:
                            for pii in pii_found:
                                pii['parent_key'] = parent_key
                            self.detection_data['dom_variables'].extend(pii_found)
                    elif isinstance(value, (dict, list)):
                        self._scan_object_for_pii(value, f"{parent_key}.{key}", source_type)
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    if isinstance(item, (dict, list)):
                        self._scan_object_for_pii(item, f"{parent_key}[{i}]", source_type)
                    elif isinstance(item, (str, int)):
                        # Check list items against patterns
                        for pii_type, pattern_info in self.pii_patterns.items():
                            if pattern_info['validation'](str(item)):
                                if re.match(pattern_info['regex'], str(item)):
                                    pii_detection = {
                                        'pii_type': pii_type,
                                        'param_name': f"{parent_key}[{i}]",
                                        'param_value': str(item)[:50] + "..." if len(str(item)) > 50 else str(item),
                                        'source': source_type,
                                        'risk_level': self._classify_risk_level(pii_type),
                                        'timestamp': time.time(),
                                        'validation_passed': True,
                                        'third_party_leak': False,
                                        'parent_key': parent_key
                                    }
                                    self.detection_data['dom_variables'].append(pii_detection)
                                    self._update_summary(pii_detection)
                                    break
        except Exception as e:
            self.logger.debug(f"Error scanning object for PII: {e}")
    
    # NEW: JSON response scanning
    def _scan_json_for_pii(self, data, source_url: str, source_type: str):
        """Recursively scan JSON data for PII"""
        try:
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, (str, int)):
                        pii_found = self._detect_pii_in_param(key, str(value), source_type)
                        if pii_found:
                            # Add source URL context and check for third-party
                            for pii in pii_found:
                                pii['source_url'] = source_url
                                pii['third_party_leak'] = self._is_third_party_domain(source_url)
                                if pii['third_party_leak']:
                                    pii['risk_level'] = 'high'  # Escalate risk for third-party
                                    pii['third_party_domain'] = urlparse(source_url).netloc
                                    self.detection_data['summary']['third_party_leaks'] += 1
                                    self.logger.warning(f"🚨 PII sent to third-party: {pii['pii_type']} -> {pii.get('third_party_domain', 'unknown')}")
                            
                            self.detection_data['response_bodies'].extend(pii_found)
                    elif isinstance(value, (dict, list)):
                        self._scan_json_for_pii(value, source_url, source_type)
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, (dict, list)):
                        self._scan_json_for_pii(item, source_url, source_type)
        except Exception as e:
            self.logger.debug(f"Error scanning JSON for PII: {e}")
    
    def _analyze_request_url(self, url: str):
        """Analyze URL for PII in parameters"""
        try:
            parsed_url = urlparse(url)
            if parsed_url.query:
                params = parse_qs(parsed_url.query)
                for param_name, param_values in params.items():
                    for param_value in param_values:
                        decoded_value = unquote(param_value)
                        pii_found = self._detect_pii_in_param(param_name, decoded_value, 'url_parameter')
                        if pii_found:
                            # Check for third-party leaks
                            for pii in pii_found:
                                pii['source_url'] = url
                                pii['third_party_leak'] = self._is_third_party_domain(url)
                                if pii['third_party_leak']:
                                    pii['risk_level'] = 'high'
                                    pii['third_party_domain'] = urlparse(url).netloc
                                    self.detection_data['summary']['third_party_leaks'] += 1
                                    self.logger.warning(f"🚨 PII in URL to third-party: {pii['pii_type']} -> {pii.get('third_party_domain', 'unknown')}")
                            
                            self.detection_data['url_parameters'].extend(pii_found)
                            
        except Exception as e:
            self.logger.debug(f"Error analyzing URL parameters: {e}")
    
    def _analyze_request_headers(self, headers: Dict[str, str]):
        """Analyze request headers for PII"""
        try:
            for header_name, header_value in headers.items():
                pii_found = self._detect_pii_in_param(header_name, header_value, 'request_header')
                if pii_found:
                    self.detection_data['request_headers'].extend(pii_found)
                    
        except Exception as e:
            self.logger.debug(f"Error analyzing request headers: {e}")
    
    def _analyze_request_body(self, body: str, request_url: str):
        """Analyze request body for PII"""
        try:
            # Try to parse as JSON
            try:
                json_data = json.loads(body)
                if isinstance(json_data, dict):
                    for key, value in json_data.items():
                        if isinstance(value, (str, int)):
                            pii_found = self._detect_pii_in_param(key, str(value), 'request_body_json')
                            if pii_found:
                                # Check for third-party leaks
                                for pii in pii_found:
                                    pii['source_url'] = request_url
                                    pii['third_party_leak'] = self._is_third_party_domain(request_url)
                                    if pii['third_party_leak']:
                                        pii['risk_level'] = 'high'
                                        pii['third_party_domain'] = urlparse(request_url).netloc
                                        self.detection_data['summary']['third_party_leaks'] += 1
                                        self.logger.warning(f"🚨 PII in request body to third-party: {pii['pii_type']} -> {pii.get('third_party_domain', 'unknown')}")
                                
                                self.detection_data['request_bodies'].extend(pii_found)
            except json.JSONDecodeError:
                # Try to parse as form data
                if '=' in body:
                    pairs = body.split('&')
                    for pair in pairs:
                        if '=' in pair:
                            key, value = pair.split('=', 1)
                            decoded_value = unquote(value)
                            pii_found = self._detect_pii_in_param(key, decoded_value, 'request_body_form')
                            if pii_found:
                                # Check for third-party leaks
                                for pii in pii_found:
                                    pii['source_url'] = request_url
                                    pii['third_party_leak'] = self._is_third_party_domain(request_url)
                                    if pii['third_party_leak']:
                                        pii['risk_level'] = 'high'
                                        pii['third_party_domain'] = urlparse(request_url).netloc
                                        self.detection_data['summary']['third_party_leaks'] += 1
                                        self.logger.warning(f"🚨 PII in form data to third-party: {pii['pii_type']} -> {pii.get('third_party_domain', 'unknown')}")
                                
                                self.detection_data['request_bodies'].extend(pii_found)
                                
        except Exception as e:
            self.logger.debug(f"Error analyzing request body: {e}")
    
    def _analyze_response_headers(self, headers: Dict[str, str]):
        """Analyze response headers for PII"""
        try:
            for header_name, header_value in headers.items():
                pii_found = self._detect_pii_in_param(header_name, header_value, 'response_header')
                if pii_found:
                    self.detection_data['response_headers'].extend(pii_found)
                    
        except Exception as e:
            self.logger.debug(f"Error analyzing response headers: {e}")
    
    async def _analyze_url_parameters(self, url: str):
        """Analyze main URL for PII in parameters"""
        self.logger.debug("🔍 Analyzing URL parameters for PII...")
        self._analyze_request_url(url)
    
    async def _analyze_cookies(self, page: Page):
        """Analyze cookies for PII"""
        self.logger.debug("🍪 Analyzing cookies for PII...")
        
        try:
            cookies = await page.context.cookies()
            
            for cookie in cookies:
                # Analyze cookie name and value
                pii_found_name = self._detect_pii_in_param(cookie['name'], cookie['name'], 'cookie_name')
                pii_found_value = self._detect_pii_in_param(cookie['name'], cookie['value'], 'cookie_value')
                
                if pii_found_name:
                    self.detection_data['cookies'].extend(pii_found_name)
                if pii_found_value:
                    self.detection_data['cookies'].extend(pii_found_value)
                    
        except Exception as e:
            self.logger.error(f"❌ Error analyzing cookies: {str(e)}")
    
    async def _analyze_storage(self, page: Page):
        """Analyze localStorage and sessionStorage for PII"""
        self.logger.debug("💾 Analyzing browser storage for PII...")
        
        try:
            # Analyze localStorage
            local_storage_script = """
            () => {
                const storage = {};
                for (let i = 0; i < localStorage.length; i++) {
                    const key = localStorage.key(i);
                    const value = localStorage.getItem(key);
                    storage[key] = value;
                }
                return storage;
            }
            """
            
            local_storage = await page.evaluate(local_storage_script)
            
            for key, value in local_storage.items():
                if value:
                    pii_found = self._detect_pii_in_param(key, str(value), 'local_storage')
                    if pii_found:
                        self.detection_data['local_storage'].extend(pii_found)
            
            # Analyze sessionStorage
            session_storage_script = """
            () => {
                const storage = {};
                for (let i = 0; i < sessionStorage.length; i++) {
                    const key = sessionStorage.key(i);
                    const value = sessionStorage.getItem(key);
                    storage[key] = value;
                }
                return storage;
            }
            """
            
            session_storage = await page.evaluate(session_storage_script)
            
            for key, value in session_storage.items():
                if value:
                    pii_found = self._detect_pii_in_param(key, str(value), 'session_storage')
                    if pii_found:
                        self.detection_data['session_storage'].extend(pii_found)
                        
        except Exception as e:
            self.logger.error(f"❌ Error analyzing storage: {str(e)}")
    
    def _detect_pii_in_param(self, param_name: str, param_value: str, source: str) -> List[Dict[str, Any]]:
        """
        Detect PII in a parameter name/value pair
        
        Returns:
            List of PII detection results
        """
        pii_detected = []
        
        if not param_value or len(param_value.strip()) == 0:
            return pii_detected
        
        param_name_lower = param_name.lower()
        param_value_stripped = param_value.strip()
        
        # Check each PII pattern
        for pii_type, pattern_info in self.pii_patterns.items():
            # Check if parameter name matches PII parameter patterns
            name_match = False
            for param_pattern in pattern_info['params']:
                if re.search(param_pattern, param_name_lower):
                    name_match = True
                    break
            
            # Check if parameter value matches PII regex pattern
            value_match = re.match(pattern_info['regex'], param_value_stripped)
            
            # If both name and value match, validate the PII
            if name_match and value_match:
                if pattern_info['validation'](param_value_stripped):
                    risk_level = self._classify_risk_level(pii_type)
                    
                    pii_detection = {
                        'pii_type': pii_type,
                        'param_name': param_name,
                        'param_value': param_value_stripped[:50] + "..." if len(param_value_stripped) > 50 else param_value_stripped,
                        'source': source,
                        'risk_level': risk_level,
                        'timestamp': time.time(),
                        'validation_passed': True,
                        'third_party_leak': False  # Default, will be updated by calling functions
                    }
                    
                    pii_detected.append(pii_detection)
                    self._update_summary(pii_detection)
                    
                    self.logger.debug(f"🚨 PII detected: {pii_type} in {source} - {param_name}={param_value_stripped[:20]}...")
        
        return pii_detected
    
    def _update_summary(self, pii_detection: Dict[str, Any]):
        """Update detection summary with new PII finding"""
        self.detection_data['summary']['total_pii_found'] += 1
        self.detection_data['summary']['pii_types_found'].add(pii_detection['pii_type'])
        
        risk_level = pii_detection['risk_level']
        if risk_level == 'high':
            self.detection_data['summary']['high_risk_pii'] += 1
        elif risk_level == 'medium':
            self.detection_data['summary']['medium_risk_pii'] += 1
        else:
            self.detection_data['summary']['low_risk_pii'] += 1
    
    def _classify_risk_level(self, pii_type: str) -> str:
        """Classify PII risk level"""
        high_risk = ['ssn', 'credit_card', 'bank_account', 'iban', 'password', 'api_key', 'jwt']
        medium_risk = ['email', 'phone', 'full_name', 'address', 'date_of_birth', 'customer_id']
        
        if pii_type in high_risk:
            return 'high'
        elif pii_type in medium_risk:
            return 'medium'
        else:
            return 'low'
    
    def _generate_results(self, url: str) -> Dict[str, Any]:
        """Generate final PII detection results"""
        
        # Convert set to list for JSON serialization
        pii_types_found = list(self.detection_data['summary']['pii_types_found'])
        
        # Calculate totals by source
        source_summary = {}
        all_detections = (
            self.detection_data['url_parameters'] +
            self.detection_data['request_headers'] +
            self.detection_data['request_bodies'] +
            self.detection_data['response_headers'] +
            self.detection_data['response_bodies'] +
            self.detection_data['cookies'] +
            self.detection_data['local_storage'] +
            self.detection_data['session_storage'] +
            self.detection_data['html_content'] +
            self.detection_data['dom_variables']
        )
        
        for detection in all_detections:
            source = detection['source']
            if source not in source_summary:
                source_summary[source] = 0
            source_summary[source] += 1
        
        # Calculate third-party leak summary
        third_party_summary = {}
        for detection in all_detections:
            if detection.get('third_party_leak', False):
                domain = detection.get('third_party_domain', 'unknown')
                if domain not in third_party_summary:
                    third_party_summary[domain] = []
                third_party_summary[domain].append(detection['pii_type'])
        
        results = {
            'url': url,
            'timestamp': time.time(),
            'pii_detected': self.detection_data['summary']['total_pii_found'] > 0,
            'total_pii_instances': self.detection_data['summary']['total_pii_found'],
            'unique_pii_types': len(pii_types_found),
            'pii_types_found': pii_types_found,
            'risk_distribution': {
                'high_risk': self.detection_data['summary']['high_risk_pii'],
                'medium_risk': self.detection_data['summary']['medium_risk_pii'],
                'low_risk': self.detection_data['summary']['low_risk_pii']
            },
            'third_party_analysis': {  # NEW
                'total_third_party_leaks': self.detection_data['summary']['third_party_leaks'],
                'third_party_domains': third_party_summary,
                'has_third_party_leaks': len(third_party_summary) > 0
            },
            'source_distribution': source_summary,
            'timing': self.detection_data['timing'],
            'details': {
                'url_parameters_count': len(self.detection_data['url_parameters']),
                'request_headers_count': len(self.detection_data['request_headers']),
                'request_bodies_count': len(self.detection_data['request_bodies']),
                'response_headers_count': len(self.detection_data['response_headers']),
                'response_bodies_count': len(self.detection_data['response_bodies']),  # NEW
                'cookies_count': len(self.detection_data['cookies']),
                'local_storage_count': len(self.detection_data['local_storage']),
                'session_storage_count': len(self.detection_data['session_storage']),
                'html_content_count': len(self.detection_data['html_content']),  # NEW
                'dom_variables_count': len(self.detection_data['dom_variables'])  # NEW
            },
            'raw_data': self.detection_data if self.debug_mode else None
        }
        
        return results
    
    def _generate_error_result(self, url: str, error: str) -> Dict[str, Any]:
        """Generate error result when PII detection fails"""
        return {
            'url': url,
            'timestamp': time.time(),
            'pii_detected': False,
            'total_pii_instances': 0,
            'unique_pii_types': 0,
            'pii_types_found': [],
            'risk_distribution': {
                'high_risk': 0,
                'medium_risk': 0,
                'low_risk': 0
            },
            'third_party_analysis': {
                'total_third_party_leaks': 0,
                'third_party_domains': {},
                'has_third_party_leaks': False
            },
            'source_distribution': {},
            'error': error,
            'timing': self.detection_data['timing']
        }


def print_enhanced_pii_summary(result):
    """Print enhanced PII detection summary"""
    url = result['url']
    detected = result['pii_detected']
    total_instances = result['total_pii_instances']
    unique_types = result['unique_pii_types']
    third_party_leaks = result['third_party_analysis']['total_third_party_leaks']
    
    print(f"📊 Enhanced PII Results for {url}:")
    print(f"   PII Detected: {'✅ YES' if detected else '❌ NO'}")
    print(f"   Total PII Instances: {total_instances}")
    print(f"   Unique PII Types: {unique_types}")
    print(f"   🚨 Third-Party Leaks: {third_party_leaks}")
    
    if result['pii_types_found']:
        print(f"   PII Types: {', '.join(result['pii_types_found'])}")
    
    risk_dist = result['risk_distribution']
    print(f"   Risk Distribution: High={risk_dist['high_risk']}, Medium={risk_dist['medium_risk']}, Low={risk_dist['low_risk']}")
    
    # Show third-party leak details
    if result['third_party_analysis']['has_third_party_leaks']:
        print(f"   🚨 Third-Party Domains:")
        for domain, pii_types in result['third_party_analysis']['third_party_domains'].items():
            print(f"      - {domain}: {', '.join(set(pii_types))}")
    
    if 'timing' in result and result['timing'].get('detection_duration'):
        duration = result['timing']['detection_duration']
        print(f"   Analysis Time: {duration:.2f} seconds")


# Test function for enhanced PII detector
async def test_enhanced_pii_detector():
    """Test the enhanced PII detector with known URLs"""
    from playwright.async_api import async_playwright
    
    print("🔍 Testing Enhanced PII Detection...")
    
    test_urls = [
        "https://httpbin.org/get?email=test@example.com&user_id=12345",  # Test URL with PII
        "https://example.com",  # Basic site
    ]
    
    detector = PIIDetector(debug_mode=True)
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            for url in test_urls:
                print(f"\n{'='*60}")
                print(f"🌐 Enhanced PII Analysis for: {url}")
                print(f"{'='*60}")
                
                result = await detector.analyze_website(page, url)
                print_enhanced_pii_summary(result)
            
            await browser.close()
            
    except Exception as e:
        print(f"❌ Enhanced PII detection test failed: {str(e)}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_enhanced_pii_detector())