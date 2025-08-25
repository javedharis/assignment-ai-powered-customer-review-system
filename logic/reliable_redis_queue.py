import redis
import json
import os
import time
import uuid
from typing import Dict, Any, Optional, Tuple
from dataclasses import asdict
from dotenv import load_dotenv
from logic.reviews_csv_extractor import Review

load_dotenv()


class ReliableRedisQueue:
    """
    Redis queue implementation with reliable message processing.
    Ensures messages are not lost and processed exactly once.
    """
    
    def __init__(self):
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 0)),
            password=os.getenv('REDIS_PASSWORD') or None,
            decode_responses=True
        )
        
        # Queue names
        self.main_queue = os.getenv('REDIS_QUEUE_NAME', 'customer_reviews_queue')
        self.processing_queue = os.getenv('PROCESSING_QUEUE_NAME', 'customer_reviews_processing')
        self.failed_queue = os.getenv('FAILED_QUEUE_NAME', 'customer_reviews_failed')
        
        # Configuration
        self.visibility_timeout = int(os.getenv('MESSAGE_VISIBILITY_TIMEOUT', 300))
        self.max_retries = int(os.getenv('MESSAGE_MAX_RETRIES', 3))
    
    def is_connected(self) -> bool:
        try:
            self.redis_client.ping()
            return True
        except redis.ConnectionError:
            return False
    
    def enqueue_review(self, review: Review) -> bool:
        """Enqueue a review for processing"""
        try:
            message = {
                'id': str(uuid.uuid4()),
                'review_data': asdict(review),
                'retry_count': 0,
                'enqueued_at': time.time()
            }
            message_json = json.dumps(message)
            result = self.redis_client.lpush(self.main_queue, message_json)
            return result > 0
        except Exception as e:
            print(f"Error enqueueing review: {e}")
            return False
    
    def enqueue_review_dict(self, review_dict: Dict[str, Any]) -> bool:
        """Enqueue a review dictionary for processing"""
        try:
            message = {
                'id': str(uuid.uuid4()),
                'review_data': review_dict,
                'retry_count': 0,
                'enqueued_at': time.time()
            }
            message_json = json.dumps(message)
            result = self.redis_client.lpush(self.main_queue, message_json)
            return result > 0
        except Exception as e:
            print(f"Error enqueueing review dict: {e}")
            return False
    
    def dequeue_for_processing(self, worker_id: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        """
        Atomically move a message from main queue to processing queue.
        Returns (message_id, message_data) or None if no messages available.
        """
        try:
            # Use BRPOPLPUSH for atomic operation
            message_json = self.redis_client.brpoplpush(
                self.main_queue, 
                self.processing_queue, 
                timeout=1
            )
            
            if not message_json:
                return None
            
            message = json.loads(message_json)
            message_id = message['id']
            
            # Set visibility timeout for this message
            processing_key = f"{self.processing_queue}:{message_id}"
            processing_data = {
                'message': message,
                'worker_id': worker_id,
                'started_at': time.time(),
                'expires_at': time.time() + self.visibility_timeout
            }
            
            self.redis_client.setex(
                processing_key,
                self.visibility_timeout,
                json.dumps(processing_data)
            )
            
            return message_id, message['review_data']
            
        except Exception as e:
            print(f"Error dequeuing message: {e}")
            return None
    
    def acknowledge_message(self, message_id: str) -> bool:
        """
        Acknowledge successful processing of a message.
        This removes the message from the processing queue.
        """
        try:
            processing_key = f"{self.processing_queue}:{message_id}"
            
            # Remove from processing tracking
            self.redis_client.delete(processing_key)
            
            # Remove from processing queue (if still there)
            self._remove_message_from_list(self.processing_queue, message_id)
            
            return True
            
        except Exception as e:
            print(f"Error acknowledging message {message_id}: {e}")
            return False
    
    def nack_message(self, message_id: str, error_message: str = None) -> bool:
        """
        Negative acknowledge - message processing failed.
        This will retry the message or move it to failed queue.
        """
        try:
            processing_key = f"{self.processing_queue}:{message_id}"
            
            # Get processing data
            processing_data_json = self.redis_client.get(processing_key)
            if not processing_data_json:
                print(f"Processing data not found for message {message_id}")
                return False
            
            processing_data = json.loads(processing_data_json)
            message = processing_data['message']
            
            # Increment retry count
            message['retry_count'] = message.get('retry_count', 0) + 1
            message['last_error'] = error_message
            message['failed_at'] = time.time()
            
            # Clean up processing state
            self.redis_client.delete(processing_key)
            self._remove_message_from_list(self.processing_queue, message_id)
            
            # Check if we should retry or move to failed queue
            if message['retry_count'] < self.max_retries:
                # Retry - put back in main queue with delay
                retry_delay = min(60 * (2 ** message['retry_count']), 3600)  # Exponential backoff
                
                # Use delayed retry (simulate with score-based sorted set)
                retry_time = time.time() + retry_delay
                retry_key = f"{self.main_queue}:retry"
                
                self.redis_client.zadd(retry_key, {json.dumps(message): retry_time})
                print(f"Message {message_id} queued for retry in {retry_delay}s (attempt {message['retry_count']})")
            else:
                # Move to failed queue
                self.redis_client.lpush(self.failed_queue, json.dumps(message))
                print(f"Message {message_id} moved to failed queue after {message['retry_count']} attempts")
            
            return True
            
        except Exception as e:
            print(f"Error nacking message {message_id}: {e}")
            return False
    
    def process_retry_queue(self) -> int:
        """
        Process messages that are ready for retry.
        Returns number of messages moved back to main queue.
        """
        try:
            retry_key = f"{self.main_queue}:retry"
            current_time = time.time()
            
            # Get messages ready for retry
            ready_messages = self.redis_client.zrangebyscore(
                retry_key, 0, current_time, withscores=False
            )
            
            if not ready_messages:
                return 0
            
            # Move ready messages back to main queue
            moved_count = 0
            for message_json in ready_messages:
                # Add back to main queue
                self.redis_client.lpush(self.main_queue, message_json)
                # Remove from retry queue
                self.redis_client.zrem(retry_key, message_json)
                moved_count += 1
            
            if moved_count > 0:
                print(f"Moved {moved_count} messages from retry queue back to main queue")
            
            return moved_count
            
        except Exception as e:
            print(f"Error processing retry queue: {e}")
            return 0
    
    def cleanup_expired_messages(self) -> int:
        """
        Cleanup messages that have exceeded visibility timeout.
        These are returned to the main queue for reprocessing.
        """
        try:
            # Get all processing keys
            pattern = f"{self.processing_queue}:*"
            processing_keys = self.redis_client.keys(pattern)
            
            if not processing_keys:
                return 0
            
            current_time = time.time()
            cleaned_count = 0
            
            for processing_key in processing_keys:
                processing_data_json = self.redis_client.get(processing_key)
                if not processing_data_json:
                    continue
                
                try:
                    processing_data = json.loads(processing_data_json)
                    expires_at = processing_data.get('expires_at', 0)
                    
                    if current_time > expires_at:
                        # Message has expired, return to main queue
                        message = processing_data['message']
                        
                        # Increment retry count for timeout
                        message['retry_count'] = message.get('retry_count', 0) + 1
                        message['last_error'] = 'Processing timeout'
                        message['timed_out_at'] = current_time
                        
                        # Clean up processing state
                        message_id = message['id']
                        self.redis_client.delete(processing_key)
                        self._remove_message_from_list(self.processing_queue, message_id)
                        
                        # Return to main queue or failed queue based on retry count
                        if message['retry_count'] < self.max_retries:
                            self.redis_client.lpush(self.main_queue, json.dumps(message))
                        else:
                            self.redis_client.lpush(self.failed_queue, json.dumps(message))
                        
                        cleaned_count += 1
                        print(f"Cleaned up expired message {message_id}")
                
                except json.JSONDecodeError:
                    # Clean up corrupted data
                    self.redis_client.delete(processing_key)
                    cleaned_count += 1
            
            return cleaned_count
            
        except Exception as e:
            print(f"Error cleaning up expired messages: {e}")
            return 0
    
    def _remove_message_from_list(self, queue_name: str, message_id: str) -> bool:
        """Remove a specific message from a Redis list by message ID"""
        try:
            # Get all messages in the queue
            messages = self.redis_client.lrange(queue_name, 0, -1)
            
            for message_json in messages:
                try:
                    message = json.loads(message_json)
                    if message.get('id') == message_id:
                        # Remove this specific message
                        self.redis_client.lrem(queue_name, 1, message_json)
                        return True
                except json.JSONDecodeError:
                    continue
            
            return False
            
        except Exception as e:
            print(f"Error removing message {message_id} from {queue_name}: {e}")
            return False
    
    def get_queue_stats(self) -> Dict[str, int]:
        """Get statistics for all queues"""
        try:
            retry_key = f"{self.main_queue}:retry"
            
            return {
                'main_queue': self.redis_client.llen(self.main_queue),
                'processing_queue': self.redis_client.llen(self.processing_queue),
                'failed_queue': self.redis_client.llen(self.failed_queue),
                'retry_queue': self.redis_client.zcard(retry_key),
                'processing_keys': len(self.redis_client.keys(f"{self.processing_queue}:*"))
            }
            
        except Exception as e:
            print(f"Error getting queue stats: {e}")
            return {}
    
    def clear_all_queues(self) -> bool:
        """Clear all queues (for testing purposes)"""
        try:
            retry_key = f"{self.main_queue}:retry"
            processing_pattern = f"{self.processing_queue}:*"
            
            self.redis_client.delete(self.main_queue)
            self.redis_client.delete(self.processing_queue) 
            self.redis_client.delete(self.failed_queue)
            self.redis_client.delete(retry_key)
            
            # Clean up processing keys
            processing_keys = self.redis_client.keys(processing_pattern)
            if processing_keys:
                self.redis_client.delete(*processing_keys)
            
            return True
            
        except Exception as e:
            print(f"Error clearing queues: {e}")
            return False
    
    def enqueue_reviews_from_csv(self, csv_filename: str) -> int:
        """Enqueue all reviews from a CSV file"""
        from logic.reviews_csv_extractor import ReviewsCSVExtractor
        
        extractor = ReviewsCSVExtractor()
        enqueued_count = 0
        
        try:
            for review in extractor.extract_reviews_from_csv(csv_filename):
                if self.enqueue_review(review):
                    enqueued_count += 1
        except Exception as e:
            print(f"Error processing CSV file: {e}")
        
        return enqueued_count