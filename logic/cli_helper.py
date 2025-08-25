from typing import Dict, Any
from logic.reliable_redis_queue import ReliableRedisQueue
from logic.reviews_csv_extractor import ReviewsCSVExtractor
from models.raw_review import RawReviewHelper
from models.review_status import ReviewStatusHelper
from models.structured_review import StructuredReviewHelper


class CLIHelper:
    
    def __init__(self):
        self.redis_queue = ReliableRedisQueue()
        self.csv_extractor = ReviewsCSVExtractor()
        self.raw_review_helper = RawReviewHelper()
        self.review_status_helper = ReviewStatusHelper()
        self.structured_review_helper = StructuredReviewHelper()
    
    def enqueue_all_reviews(self, csv_filename: str) -> Dict[str, Any]:
        try:
            if not self.redis_queue.is_connected():
                return {
                    'success': False,
                    'message': 'Failed to connect to Redis server',
                    'enqueued_count': 0
                }
            
            enqueued_count = self.redis_queue.enqueue_reviews_from_csv(csv_filename)
            
            if enqueued_count > 0:
                return {
                    'success': True,
                    'message': f'Successfully enqueued {enqueued_count} reviews from {csv_filename}',
                    'enqueued_count': enqueued_count
                }
            else:
                return {
                    'success': False,
                    'message': f'No reviews were enqueued from {csv_filename}',
                    'enqueued_count': 0
                }
                
        except FileNotFoundError as e:
            return {
                'success': False,
                'message': f'CSV file not found: {csv_filename}',
                'enqueued_count': 0
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error processing CSV file: {str(e)}',
                'enqueued_count': 0
            }
    
    def get_queue_status(self) -> Dict[str, Any]:
        try:
            if not self.redis_queue.is_connected():
                return {
                    'success': False,
                    'message': 'Failed to connect to Redis server',
                    'queue_length': 0,
                    'connected': False
                }
            
            stats = self.redis_queue.get_queue_stats()
            queue_length = stats.get('main_queue', 0)
            return {
                'success': True,
                'message': f'Queue contains {queue_length} reviews (main: {stats.get("main_queue", 0)}, processing: {stats.get("processing_queue", 0)}, retry: {stats.get("retry_queue", 0)}, failed: {stats.get("failed_queue", 0)})',
                'queue_length': queue_length,
                'queue_stats': stats,
                'connected': True
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': f'Error getting queue status: {str(e)}',
                'queue_length': 0,
                'connected': False
            }
    
    def clear_queue(self) -> Dict[str, Any]:
        try:
            if not self.redis_queue.is_connected():
                return {
                    'success': False,
                    'message': 'Failed to connect to Redis server'
                }
            
            if self.redis_queue.clear_all_queues():
                return {
                    'success': True,
                    'message': 'Queue cleared successfully'
                }
            else:
                return {
                    'success': False,
                    'message': 'Failed to clear queue'
                }
                
        except Exception as e:
            return {
                'success': False,
                'message': f'Error clearing queue: {str(e)}'
            }
    
    def process_single_review(self, review_id: str, date: str, rating: str, text: str) -> Dict[str, Any]:
        try:
            from logic.reviews_csv_extractor import Review
            
            if not self.redis_queue.is_connected():
                return {
                    'success': False,
                    'message': 'Failed to connect to Redis server'
                }
            
            review = Review(review_id=review_id, date=date, rating=rating, text=text)
            
            if self.redis_queue.enqueue_review(review):
                return {
                    'success': True,
                    'message': f'Successfully enqueued review {review_id}'
                }
            else:
                return {
                    'success': False,
                    'message': f'Failed to enqueue review {review_id}'
                }
                
        except Exception as e:
            return {
                'success': False,
                'message': f'Error processing review: {str(e)}'
            }
    
    def clear_database(self, password: str) -> Dict[str, Any]:
        """
        Clear all records from the database.
        Requires the correct password: 'YES_DELETE_IT'
        
        Args:
            password: Password to authorize database clearing
            
        Returns:
            Dictionary with operation result
        """
        REQUIRED_PASSWORD = "YES_DELETE_IT"
        
        if password != REQUIRED_PASSWORD:
            return {
                'success': False,
                'message': 'Invalid password. Database clearing operation aborted.'
            }
        
        try:
            deleted_counts = {'structured_reviews': 0, 'review_statuses': 0, 'raw_reviews': 0}
            
            # Delete structured reviews first (due to foreign key constraints)
            structured_reviews = self.structured_review_helper.get_all_structured_reviews()
            for review in structured_reviews:
                self.structured_review_helper.delete_structured_review(review.review_id)
                deleted_counts['structured_reviews'] += 1
            
            # Delete review statuses
            review_statuses = self.review_status_helper.get_all_review_statuses()
            for status in review_statuses:
                self.review_status_helper.delete_review_status(status.review_id)
                deleted_counts['review_statuses'] += 1
            
            # Delete raw reviews last
            raw_reviews = self.raw_review_helper.get_all_raw_reviews()
            for review in raw_reviews:
                self.raw_review_helper.delete_raw_review(review.review_id)
                deleted_counts['raw_reviews'] += 1
            
            total_deleted = sum(deleted_counts.values())
            
            return {
                'success': True,
                'message': f'Database cleared successfully. Deleted {total_deleted} records total.',
                'deleted_counts': deleted_counts
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': f'Error clearing database: {str(e)}'
            }