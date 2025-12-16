#!/usr/bin/env python3
"""
Progress Manager for Large-Scale GTM Analysis
Handles resumable analysis, batch processing, and incremental CSV saving
Prevents data corruption and memory overflow
"""

import json
import os
import time
import csv
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
import logging


class ProgressManager:
    """Manages progress tracking and resumable analysis for large-scale GTM detection"""
    
    def __init__(self, session_name: str = None, debug_mode: bool = True):
        self.debug_mode = debug_mode
        self.logger = self._setup_logging()
        
        # Generate session name if not provided
        if session_name is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            session_name = f"gtm_analysis_{timestamp}"
        
        self.session_name = session_name
        
        # Progress file paths
        self.progress_dir = Path("/app/output/progress")
        self.progress_file = self.progress_dir / f"{session_name}_progress.json"
        self.csv_file = Path("/app/output/csv") / f"{session_name}.csv"
        
        # Ensure directories exist
        self.progress_dir.mkdir(parents=True, exist_ok=True)
        self.csv_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Progress tracking data
        self.completed_urls: Set[str] = set()
        self.failed_urls: Set[str] = set()
        self.current_batch: int = 0
        self.total_batches: int = 0
        self.batch_size: int = 100
        self.start_time: Optional[float] = None
        self.last_save_time: float = time.time()
        
        # CSV headers (must match main.py format)
        self.csv_fieldnames = [
            'url', 'gtm_detected', 'consent_mode', 'gtm_events', 
            'third_party_trackers', 'third_party_domains_count', 'third_party_domains_list',
            'trackerdb_patterns_count', 'trackerdb_data_source',
            'status', 'google_urls_count', 'analysis_time', 'timestamp', 'raw_urls'
        ]
        
        self.logger.info(f"📁 Progress Manager initialized for session: {session_name}")
        self.logger.info(f"📄 CSV file: {self.csv_file}")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('ProgressManager')
        logger.setLevel(logging.DEBUG if self.debug_mode else logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def load_progress(self) -> bool:
        """
        Load existing progress from file
        
        Returns:
            bool: True if progress was loaded, False if starting fresh
        """
        try:
            if self.progress_file.exists():
                self.logger.info(f"📥 Loading existing progress: {self.progress_file}")
                
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                self.completed_urls = set(data.get('completed_urls', []))
                self.failed_urls = set(data.get('failed_urls', []))
                self.current_batch = data.get('current_batch', 0)
                self.total_batches = data.get('total_batches', 0)
                self.batch_size = data.get('batch_size', 100)
                self.start_time = data.get('start_time')
                
                self.logger.info(f"✅ Progress loaded: {len(self.completed_urls)} completed, {len(self.failed_urls)} failed")
                self.logger.info(f" Batch progress: {self.current_batch}/{self.total_batches}")
                
                return True
                
        except Exception as e:
            self.logger.error(f"❌ Error loading progress: {e}")
            self.logger.info("🔄 Starting fresh analysis")
        
        return False
    
    def save_progress(self) -> bool:
        """
        Save current progress to file
        
        Returns:
            bool: True if saved successfully, False otherwise
        """
        try:
            progress_data = {
                'session_name': self.session_name,
                'completed_urls': list(self.completed_urls),
                'failed_urls': list(self.failed_urls),
                'current_batch': self.current_batch,
                'total_batches': self.total_batches,
                'batch_size': self.batch_size,
                'start_time': self.start_time,
                'last_updated': time.time(),
                'total_completed': len(self.completed_urls),
                'total_failed': len(self.failed_urls),
                'completion_percentage': (len(self.completed_urls) + len(self.failed_urls)) / max(1, self.total_batches * self.batch_size) * 100
            }
            
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, indent=2)
            
            self.last_save_time = time.time()
            self.logger.debug(f"💾 Progress saved: {len(self.completed_urls)} completed")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error saving progress: {e}")
            return False
    
    def initialize_session(self, all_urls: List[str], batch_size: int = 100) -> Dict[str, Any]:
        """
        Initialize a new analysis session or resume existing one
        
        Args:
            all_urls: Complete list of URLs to analyze
            batch_size: Number of URLs per batch
            
        Returns:
            Dictionary with session info and URLs to process
        """
        self.batch_size = batch_size
        self.start_time = time.time() if self.start_time is None else self.start_time
        
        # Load existing progress if available
        resumed = self.load_progress()
        
        # Calculate total batches
        self.total_batches = (len(all_urls) + batch_size - 1) // batch_size
        
        # Filter out already completed/failed URLs
        remaining_urls = [url for url in all_urls 
                         if url not in self.completed_urls and url not in self.failed_urls]
        
        # Initialize CSV file if starting fresh
        if not resumed:
            self._initialize_csv()
        else:
            self._validate_csv()
        
        session_info = {
            'session_name': self.session_name,
            'total_urls': len(all_urls),
            'remaining_urls': len(remaining_urls),
            'completed_urls': len(self.completed_urls),
            'failed_urls': len(self.failed_urls),
            'total_batches': self.total_batches,
            'current_batch': self.current_batch,
            'batch_size': self.batch_size,
            'resumed': resumed,
            'urls_to_process': remaining_urls,
            'csv_file': str(self.csv_file)
        }
        
        # Save initial progress
        self.save_progress()
        
        if resumed:
            self.logger.info(f"🔄 Resuming session: {len(remaining_urls)} URLs remaining")
        else:
            self.logger.info(f"🚀 Starting new session: {len(all_urls)} URLs to process")
        
        return session_info
    
    def _initialize_csv(self) -> bool:
        """Initialize CSV file with headers"""
        try:
            self.logger.info(f"📄 Initializing CSV file: {self.csv_file}")
            
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_fieldnames)
                writer.writeheader()
            
            self.logger.info(f"✅ CSV file initialized with headers")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error initializing CSV: {e}")
            return False
    
    def _validate_csv(self) -> bool:
        """Validate existing CSV file has correct headers"""
        try:
            if not self.csv_file.exists():
                self.logger.warning(f"⚠️ CSV file missing, creating new one")
                return self._initialize_csv()
            
            with open(self.csv_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                existing_headers = reader.fieldnames
                
                if set(existing_headers) != set(self.csv_fieldnames):
                    self.logger.error(f"❌ CSV headers mismatch!")
                    self.logger.error(f"Expected: {self.csv_fieldnames}")
                    self.logger.error(f"Found: {existing_headers}")
                    raise ValueError("CSV schema mismatch - cannot resume")
            
            self.logger.info(f"✅ CSV file validated")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error validating CSV: {e}")
            return False
    
    def save_batch_results(self, batch_results: List[Dict[str, Any]]) -> bool:
        """
        Save batch results to CSV using atomic append operation
        
        Args:
            batch_results: List of analysis results to save
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        if not batch_results:
            self.logger.warning("⚠️ No batch results to save")
            return True
        
        try:
            # Create temporary file for atomic operation
            temp_file = self.csv_file.with_suffix('.tmp')
            
            self.logger.debug(f"💾 Saving {len(batch_results)} results to CSV...")
            
            # Write batch results to temp file
            with open(temp_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_fieldnames)
                
                for result in batch_results:
                    csv_result = self._convert_result_to_csv(result)
                    writer.writerow(csv_result)
            
            # Atomic append: read temp file and append to main CSV
            with open(temp_file, 'r', encoding='utf-8') as temp_reader:
                with open(self.csv_file, 'a', newline='', encoding='utf-8') as main_csv:
                    main_csv.write(temp_reader.read())
            
            # Clean up temp file
            temp_file.unlink()
            
            self.logger.info(f"✅ Saved {len(batch_results)} results to CSV")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error saving batch results: {e}")
            
            # Clean up temp file on error
            if temp_file.exists():
                temp_file.unlink()
            
            return False
    
    def _convert_result_to_csv(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Convert analysis result to CSV format (matches main.py logic)"""
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
        
        return csv_result
    
    def get_next_batch(self, remaining_urls: List[str]) -> Optional[List[str]]:
        """
        Get the next batch of URLs to process
        
        Args:
            remaining_urls: List of URLs still to be processed
            
        Returns:
            List of URLs for next batch, or None if all done
        """
        if not remaining_urls:
            self.logger.info("✅ All URLs processed!")
            return None
        
        # Calculate batch start/end
        start_idx = 0
        end_idx = min(self.batch_size, len(remaining_urls))
        
        batch_urls = remaining_urls[start_idx:end_idx]
        
        self.current_batch += 1
        
        self.logger.info(f" Batch {self.current_batch}/{self.total_batches}: {len(batch_urls)} URLs")
        
        return batch_urls
    
    def mark_batch_completed(self, batch_urls: List[str], batch_results: List[Dict[str, Any]]) -> bool:
        """
        Mark a batch as completed and save results
        
        Args:
            batch_urls: URLs that were processed
            batch_results: Analysis results for the batch
            
        Returns:
            bool: True if batch was saved successfully
        """
        # Save results to CSV first
        if not self.save_batch_results(batch_results):
            self.logger.error(f"❌ Failed to save batch {self.current_batch} to CSV")
            return False
        
        # Mark URLs as completed/failed
        for i, url in enumerate(batch_urls):
            if i < len(batch_results):
                result = batch_results[i]
                if result.get('status') == 'success':
                    self.completed_urls.add(url)
                else:
                    self.failed_urls.add(url)
            else:
                # URL was not processed (shouldn't happen)
                self.failed_urls.add(url)
        
        # Save progress
        self.save_progress()
        
        self.logger.info(f"✅ Batch {self.current_batch} completed and saved")
        return True
    
    def mark_completed(self, url: str, result: Dict[str, Any]) -> None:
        """Mark a URL as successfully completed"""
        self.completed_urls.add(url)
        if url in self.failed_urls:
            self.failed_urls.remove(url)
        self.logger.debug(f"✅ Completed: {url}")
    
    def mark_failed(self, url: str, error: str) -> None:
        """Mark a URL as failed"""
        self.failed_urls.add(url)
        if url in self.completed_urls:
            self.completed_urls.remove(url)
        self.logger.warning(f"❌ Failed: {url} - {error}")
    
    def is_completed(self, url: str) -> bool:
        """Check if a URL has already been completed"""
        return url in self.completed_urls
    
    def is_failed(self, url: str) -> bool:
        """Check if a URL has already failed"""
        return url in self.failed_urls
    
    def should_save_progress(self, force: bool = False) -> bool:
        """Check if progress should be saved (every 30 seconds or when forced)"""
        if force:
            return True
        return (time.time() - self.last_save_time) > 30  # Save every 30 seconds
    
    def get_session_stats(self) -> Dict[str, Any]:
        """Get current session statistics"""
        current_time = time.time()
        elapsed_time = current_time - self.start_time if self.start_time else 0
        
        total_processed = len(self.completed_urls) + len(self.failed_urls)
        total_expected = self.total_batches * self.batch_size
        
        completion_percentage = (total_processed / max(1, total_expected)) * 100
        
        # Estimate remaining time
        if total_processed > 0 and elapsed_time > 0:
            avg_time_per_url = elapsed_time / total_processed
            remaining_urls = total_expected - total_processed
            estimated_remaining_time = remaining_urls * avg_time_per_url
        else:
            estimated_remaining_time = 0
        
        return {
            'session_name': self.session_name,
            'elapsed_time_hours': elapsed_time / 3600,
            'total_processed': total_processed,
            'completed': len(self.completed_urls),
            'failed': len(self.failed_urls),
            'success_rate': (len(self.completed_urls) / max(1, total_processed)) * 100,
            'completion_percentage': completion_percentage,
            'estimated_remaining_hours': estimated_remaining_time / 3600,
            'current_batch': self.current_batch,
            'total_batches': self.total_batches,
            'batch_size': self.batch_size,
            'csv_file': str(self.csv_file)
        }
    
    def print_session_summary(self) -> None:
        """Print a formatted session summary"""
        stats = self.get_session_stats()
        
        print(f"\n{'='*80}")
        print(f" SESSION SUMMARY: {stats['session_name']}")
        print(f"{'='*80}")
        print(f"  Elapsed Time: {stats['elapsed_time_hours']:.1f} hours")
        print(f" Progress: {stats['completion_percentage']:.1f}% complete")
        print(f"✅ Completed: {stats['completed']} URLs")
        print(f"❌ Failed: {stats['failed']} URLs")
        print(f" Success Rate: {stats['success_rate']:.1f}%")
        print(f" Batch: {stats['current_batch']}/{stats['total_batches']}")
        print(f"📄 CSV File: {stats['csv_file']}")
        
        if stats['estimated_remaining_hours'] > 0:
            print(f"⏳ Estimated Remaining: {stats['estimated_remaining_hours']:.1f} hours")
        
        print(f"{'='*80}")
    
    def export_failed_urls(self, output_file: str = None) -> str:
        """Export failed URLs to a text file for retry"""
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"/app/output/failed_urls_{self.session_name}_{timestamp}.txt"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                for url in sorted(self.failed_urls):
                    f.write(f"{url}\n")
            
            self.logger.info(f"📝 Exported {len(self.failed_urls)} failed URLs to: {output_file}")
            return output_file
            
        except Exception as e:
            self.logger.error(f"❌ Error exporting failed URLs: {e}")
            return ""
    
    def cleanup_old_sessions(self, days_old: int = 7) -> int:
        """Clean up old progress files"""
        deleted_count = 0
        cutoff_time = time.time() - (days_old * 24 * 60 * 60)
        
        try:
            for progress_file in self.progress_dir.glob("*_progress.json"):
                if progress_file.stat().st_mtime < cutoff_time:
                    progress_file.unlink()
                    deleted_count += 1
                    self.logger.debug(f" Deleted old progress file: {progress_file.name}")
            
            if deleted_count > 0:
                self.logger.info(f" Cleaned up {deleted_count} old progress files")
        
        except Exception as e:
            self.logger.error(f" Error cleaning up old sessions: {e}")
        
        return deleted_count