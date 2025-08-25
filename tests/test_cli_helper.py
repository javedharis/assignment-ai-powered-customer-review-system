import pytest
from unittest.mock import Mock, patch
from logic.cli_helper import CLIHelper
from logic.reviews_csv_extractor import Review


class TestCLIHelper:
    
    def setup_method(self):
        self.cli_helper = CLIHelper()
        self.mock_redis_queue = Mock()
        self.mock_csv_extractor = Mock()
        
        self.cli_helper.redis_queue = self.mock_redis_queue
        self.cli_helper.csv_extractor = self.mock_csv_extractor
    
    def test_enqueue_all_reviews_success(self):
        self.mock_redis_queue.is_connected.return_value = True
        self.mock_redis_queue.enqueue_reviews_from_csv.return_value = 5
        
        result = self.cli_helper.enqueue_all_reviews('test.csv')
        
        assert result['success'] == True
        assert result['enqueued_count'] == 5
        assert 'Successfully enqueued 5 reviews' in result['message']
        self.mock_redis_queue.enqueue_reviews_from_csv.assert_called_once_with('test.csv')
    
    def test_enqueue_all_reviews_redis_not_connected(self):
        self.mock_redis_queue.is_connected.return_value = False
        
        result = self.cli_helper.enqueue_all_reviews('test.csv')
        
        assert result['success'] == False
        assert result['enqueued_count'] == 0
        assert 'Failed to connect to Redis server' in result['message']
    
    def test_enqueue_all_reviews_no_reviews_enqueued(self):
        self.mock_redis_queue.is_connected.return_value = True
        self.mock_redis_queue.enqueue_reviews_from_csv.return_value = 0
        
        result = self.cli_helper.enqueue_all_reviews('empty.csv')
        
        assert result['success'] == False
        assert result['enqueued_count'] == 0
        assert 'No reviews were enqueued' in result['message']
    
    def test_enqueue_all_reviews_file_not_found(self):
        self.mock_redis_queue.is_connected.return_value = True
        self.mock_redis_queue.enqueue_reviews_from_csv.side_effect = FileNotFoundError()
        
        result = self.cli_helper.enqueue_all_reviews('nonexistent.csv')
        
        assert result['success'] == False
        assert result['enqueued_count'] == 0
        assert 'CSV file not found' in result['message']
    
    def test_enqueue_all_reviews_exception(self):
        self.mock_redis_queue.is_connected.return_value = True
        self.mock_redis_queue.enqueue_reviews_from_csv.side_effect = Exception('Test error')
        
        result = self.cli_helper.enqueue_all_reviews('test.csv')
        
        assert result['success'] == False
        assert result['enqueued_count'] == 0
        assert 'Error processing CSV file: Test error' in result['message']
    
    def test_get_queue_status_success(self):
        self.mock_redis_queue.is_connected.return_value = True
        self.mock_redis_queue.get_queue_stats.return_value = {'main_queue': 10, 'processing_queue': 2, 'retry_queue': 1, 'failed_queue': 0}
        
        result = self.cli_helper.get_queue_status()
        
        assert result['success'] == True
        assert result['queue_length'] == 10
        assert result['connected'] == True
        assert 'Queue contains 10 reviews' in result['message']
    
    def test_get_queue_status_redis_not_connected(self):
        self.mock_redis_queue.is_connected.return_value = False
        
        result = self.cli_helper.get_queue_status()
        
        assert result['success'] == False
        assert result['queue_length'] == 0
        assert result['connected'] == False
        assert 'Failed to connect to Redis server' in result['message']
    
    def test_get_queue_status_exception(self):
        self.mock_redis_queue.is_connected.return_value = True
        self.mock_redis_queue.get_queue_stats.side_effect = Exception('Redis error')
        
        result = self.cli_helper.get_queue_status()
        
        assert result['success'] == False
        assert result['queue_length'] == 0
        assert result['connected'] == False
        assert 'Error getting queue status: Redis error' in result['message']
    
    def test_clear_queue_success(self):
        self.mock_redis_queue.is_connected.return_value = True
        self.mock_redis_queue.clear_all_queues.return_value = True
        
        result = self.cli_helper.clear_queue()
        
        assert result['success'] == True
        assert 'Queue cleared successfully' in result['message']
    
    def test_clear_queue_redis_not_connected(self):
        self.mock_redis_queue.is_connected.return_value = False
        
        result = self.cli_helper.clear_queue()
        
        assert result['success'] == False
        assert 'Failed to connect to Redis server' in result['message']
    
    def test_clear_queue_failed(self):
        self.mock_redis_queue.is_connected.return_value = True
        self.mock_redis_queue.clear_all_queues.return_value = False
        
        result = self.cli_helper.clear_queue()
        
        assert result['success'] == False
        assert 'Failed to clear queue' in result['message']
    
    def test_clear_queue_exception(self):
        self.mock_redis_queue.is_connected.return_value = True
        self.mock_redis_queue.clear_all_queues.side_effect = Exception('Clear error')
        
        result = self.cli_helper.clear_queue()
        
        assert result['success'] == False
        assert 'Error clearing queue: Clear error' in result['message']
    
    def test_process_single_review_success(self):
        self.mock_redis_queue.is_connected.return_value = True
        self.mock_redis_queue.enqueue_review.return_value = True
        
        result = self.cli_helper.process_single_review('R123', '2025-01-01', '★★★★☆ (4 stars)', 'Great product')
        
        assert result['success'] == True
        assert 'Successfully enqueued review R123' in result['message']
        self.mock_redis_queue.enqueue_review.assert_called_once()
    
    def test_process_single_review_redis_not_connected(self):
        self.mock_redis_queue.is_connected.return_value = False
        
        result = self.cli_helper.process_single_review('R123', '2025-01-01', '★★★★☆ (4 stars)', 'Great product')
        
        assert result['success'] == False
        assert 'Failed to connect to Redis server' in result['message']
    
    def test_process_single_review_enqueue_failed(self):
        self.mock_redis_queue.is_connected.return_value = True
        self.mock_redis_queue.enqueue_review.return_value = False
        
        result = self.cli_helper.process_single_review('R123', '2025-01-01', '★★★★☆ (4 stars)', 'Great product')
        
        assert result['success'] == False
        assert 'Failed to enqueue review R123' in result['message']
    
    def test_process_single_review_exception(self):
        self.mock_redis_queue.is_connected.return_value = True
        self.mock_redis_queue.enqueue_review.side_effect = Exception('Enqueue error')
        
        result = self.cli_helper.process_single_review('R123', '2025-01-01', '★★★★☆ (4 stars)', 'Great product')
        
        assert result['success'] == False
        assert 'Error processing review: Enqueue error' in result['message']
    
    @patch('logic.cli_helper.StructuredReviewHelper')
    @patch('logic.cli_helper.ReviewStatusHelper')
    @patch('logic.cli_helper.RawReviewHelper')
    def test_clear_database_success(self, mock_raw_helper_class, mock_status_helper_class, mock_structured_helper_class):
        # Setup mocks
        mock_raw_helper = Mock()
        mock_status_helper = Mock()
        mock_structured_helper = Mock()
        
        mock_raw_helper_class.return_value = mock_raw_helper
        mock_status_helper_class.return_value = mock_status_helper
        mock_structured_helper_class.return_value = mock_structured_helper
        
        # Setup mock data
        mock_structured_reviews = [Mock(review_id='R1'), Mock(review_id='R2')]
        mock_review_statuses = [Mock(review_id='R1'), Mock(review_id='R2')]
        mock_raw_reviews = [Mock(review_id='R1'), Mock(review_id='R2')]
        
        mock_structured_helper.get_all_structured_reviews.return_value = mock_structured_reviews
        mock_status_helper.get_all_review_statuses.return_value = mock_review_statuses
        mock_raw_helper.get_all_raw_reviews.return_value = mock_raw_reviews
        
        mock_structured_helper.delete_structured_review.return_value = True
        mock_status_helper.delete_review_status.return_value = True
        mock_raw_helper.delete_raw_review.return_value = True
        
        cli_helper = CLIHelper()
        result = cli_helper.clear_database("YES_DELETE_IT")
        
        # Verify calls
        mock_structured_helper.get_all_structured_reviews.assert_called_once()
        mock_status_helper.get_all_review_statuses.assert_called_once()
        mock_raw_helper.get_all_raw_reviews.assert_called_once()
        
        assert mock_structured_helper.delete_structured_review.call_count == 2
        assert mock_status_helper.delete_review_status.call_count == 2
        assert mock_raw_helper.delete_raw_review.call_count == 2
        
        # Verify result
        assert result['success'] == True
        assert 'Database cleared successfully' in result['message']
        assert 'Deleted 6 records total' in result['message']
        assert result['deleted_counts']['structured_reviews'] == 2
        assert result['deleted_counts']['review_statuses'] == 2
        assert result['deleted_counts']['raw_reviews'] == 2
    
    def test_clear_database_invalid_password(self):
        cli_helper = CLIHelper()
        result = cli_helper.clear_database("WRONG_PASSWORD")
        
        assert result['success'] == False
        assert result['message'] == 'Invalid password. Database clearing operation aborted.'
    
    def test_clear_database_empty_password(self):
        cli_helper = CLIHelper()
        result = cli_helper.clear_database("")
        
        assert result['success'] == False
        assert result['message'] == 'Invalid password. Database clearing operation aborted.'
    
    @patch('logic.cli_helper.StructuredReviewHelper')
    @patch('logic.cli_helper.ReviewStatusHelper')
    @patch('logic.cli_helper.RawReviewHelper')
    def test_clear_database_empty_database(self, mock_raw_helper_class, mock_status_helper_class, mock_structured_helper_class):
        # Setup mocks
        mock_raw_helper = Mock()
        mock_status_helper = Mock()
        mock_structured_helper = Mock()
        
        mock_raw_helper_class.return_value = mock_raw_helper
        mock_status_helper_class.return_value = mock_status_helper
        mock_structured_helper_class.return_value = mock_structured_helper
        
        # Setup empty database
        mock_structured_helper.get_all_structured_reviews.return_value = []
        mock_status_helper.get_all_review_statuses.return_value = []
        mock_raw_helper.get_all_raw_reviews.return_value = []
        
        cli_helper = CLIHelper()
        result = cli_helper.clear_database("YES_DELETE_IT")
        
        # Verify calls
        mock_structured_helper.get_all_structured_reviews.assert_called_once()
        mock_status_helper.get_all_review_statuses.assert_called_once()
        mock_raw_helper.get_all_raw_reviews.assert_called_once()
        
        # Verify no deletion calls were made
        mock_structured_helper.delete_structured_review.assert_not_called()
        mock_status_helper.delete_review_status.assert_not_called()
        mock_raw_helper.delete_raw_review.assert_not_called()
        
        # Verify result
        assert result['success'] == True
        assert 'Database cleared successfully' in result['message']
        assert 'Deleted 0 records total' in result['message']
        assert result['deleted_counts']['structured_reviews'] == 0
        assert result['deleted_counts']['review_statuses'] == 0
        assert result['deleted_counts']['raw_reviews'] == 0
    
    @patch('logic.cli_helper.StructuredReviewHelper')
    @patch('logic.cli_helper.ReviewStatusHelper') 
    @patch('logic.cli_helper.RawReviewHelper')
    def test_clear_database_exception(self, mock_raw_helper_class, mock_status_helper_class, mock_structured_helper_class):
        # Setup mocks
        mock_raw_helper = Mock()
        mock_status_helper = Mock()
        mock_structured_helper = Mock()
        
        mock_raw_helper_class.return_value = mock_raw_helper
        mock_status_helper_class.return_value = mock_status_helper
        mock_structured_helper_class.return_value = mock_structured_helper
        
        # Make get_all_structured_reviews raise an exception
        mock_structured_helper.get_all_structured_reviews.side_effect = Exception("Database connection failed")
        
        cli_helper = CLIHelper()
        result = cli_helper.clear_database("YES_DELETE_IT")
        
        # Verify result
        assert result['success'] == False
        assert 'Error clearing database' in result['message']
        assert 'Database connection failed' in result['message']