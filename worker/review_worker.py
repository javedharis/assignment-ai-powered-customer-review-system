#!/usr/bin/env python3
"""
Review Processing Worker

A reliable worker that processes reviews from Redis queue with retry mechanism.
"""

import sys
import os
import json
import time
import logging
from typing import Dict, Any

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from logic.review_processor import ReviewProcessor
from logic.reliable_redis_queue import ReliableRedisQueue


class ReviewWorker:
    
    def __init__(self, max_retries: int = 3, retry_delay: int = 5):
        self.processor = ReviewProcessor()
        self.queue = ReliableRedisQueue()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def start(self):
        """Start the worker to process reviews from queue."""
        self.logger.info("🚀 Starting Review Worker...")
        self.logger.info(f"   Max retries: {self.max_retries}")
        self.logger.info(f"   Retry delay: {self.retry_delay}s")
        
        if not self.queue.is_connected():
            self.logger.error("❌ Cannot connect to Redis. Exiting.")
            return
        
        self.logger.info("✅ Connected to Redis queue")
        self.logger.info("👂 Listening for reviews to process...")
        
        try:
            while True:
                self._process_next_review()
                time.sleep(1)  # Brief pause between checks
                
        except KeyboardInterrupt:
            self.logger.info("🛑 Worker stopped by user")
        except Exception as e:
            self.logger.error(f"💥 Worker crashed: {e}")
    
    def _process_next_review(self):
        """Process the next review from the queue."""
        try:
            # Get next review from queue
            result = self.queue.dequeue_for_processing('worker')
            if not result:
                return  # No messages in queue
            
            message_id, review_data = result
            
            review_id = review_data.get('review_id', 'unknown')
            
            self.logger.info(f"📝 Processing review: {review_id}")
            
            # Process the review with retry logic
            result = self._process_with_retries(review_data)
            
            if result['status'] == 'success':
                self.logger.info(f"✅ Successfully processed: {review_id}")
                # Acknowledge successful processing
                self.queue.acknowledge_message(message_id)
            else:
                self.logger.error(f"❌ Failed to process: {review_id} - {result.get('error')}")
                # Handle failed processing
                self.queue.nack_message(message_id, result.get('error', 'Processing failed'))
                
        except Exception as e:
            self.logger.error(f"💥 Error processing review: {e}")
    
    def _process_with_retries(self, review_data: Dict) -> Dict:
        """Process review with built-in retry mechanism."""
        review_id = review_data.get('review_id')
        
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"🔄 Attempt {attempt}/{self.max_retries} for {review_id}")
                
                # Process the review
                result = self.processor.process_review_complete(review_data)
                
                if result['status'] == 'success':
                    if attempt > 1:
                        self.logger.info(f"✅ Succeeded on attempt {attempt} for {review_id}")
                    return result
                else:
                    # Processing failed, log and continue to retry
                    self.logger.warning(f"⚠️  Attempt {attempt} failed for {review_id}: {result.get('error')}")
                    
                    if attempt < self.max_retries:
                        self.logger.info(f"⏳ Waiting {self.retry_delay}s before retry...")
                        time.sleep(self.retry_delay)
                        continue
                    else:
                        self.logger.error(f"❌ All {self.max_retries} attempts failed for {review_id}")
                        return result
                        
            except Exception as e:
                error_msg = str(e)
                self.logger.error(f"💥 Exception on attempt {attempt} for {review_id}: {error_msg}")
                
                if attempt < self.max_retries:
                    self.logger.info(f"⏳ Waiting {self.retry_delay}s before retry...")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    return {
                        'status': 'failed',
                        'review_id': review_id,
                        'error': error_msg
                    }
        
        # This shouldn't be reached, but just in case
        return {
            'status': 'failed',
            'review_id': review_id,
            'error': 'Maximum retries exceeded'
        }
    
    def get_status(self) -> Dict:
        """Get worker status information."""
        try:
            stats = self.queue.get_queue_stats()
            processing_summary = self.processor.get_processing_summary()
            
            return {
                'status': 'running',
                'queue_stats': stats,
                'max_retries': self.max_retries,
                'retry_delay': self.retry_delay,
                'processing_summary': processing_summary
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def process_failed_reviews(self):
        """Manually process reviews that are marked as failed in database."""
        self.logger.info("🔄 Processing failed reviews from database...")
        
        failed_reviews = self.processor.get_failed_reviews()
        self.logger.info(f"📊 Found {len(failed_reviews)} failed reviews")
        
        success_count = 0
        for review_status in failed_reviews:
            review_id = review_status.get('review_id')
            
            try:
                self.logger.info(f"🔄 Retrying failed review: {review_id}")
                result = self.processor.retry_failed_review(review_id, self.max_retries)
                
                if result['status'] == 'success':
                    success_count += 1
                    self.logger.info(f"✅ Successfully retried: {review_id}")
                else:
                    self.logger.error(f"❌ Retry failed: {review_id} - {result.get('message', result.get('error'))}")
                    
            except Exception as e:
                self.logger.error(f"💥 Exception retrying {review_id}: {e}")
        
        self.logger.info(f"✅ Retry complete: {success_count}/{len(failed_reviews)} successful")


def main():
    """Main entry point for the worker."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Review Processing Worker')
    parser.add_argument('--max-retries', type=int, default=3,
                      help='Maximum number of retry attempts (default: 3)')
    parser.add_argument('--retry-delay', type=int, default=5,
                      help='Delay between retries in seconds (default: 5)')
    parser.add_argument('--process-failed', action='store_true',
                      help='Process failed reviews from database and exit')
    parser.add_argument('--status', action='store_true',
                      help='Show worker status and exit')
    
    args = parser.parse_args()
    
    worker = ReviewWorker(max_retries=args.max_retries, retry_delay=args.retry_delay)
    
    if args.status:
        status = worker.get_status()
        print(json.dumps(status, indent=2))
        return
    
    if args.process_failed:
        worker.process_failed_reviews()
        return
    
    # Start the worker
    worker.start()


if __name__ == '__main__':
    main()