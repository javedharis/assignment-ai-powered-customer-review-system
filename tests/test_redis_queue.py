import pytest
import os
import tempfile
import csv
from unittest.mock import Mock, patch
from logic.reliable_redis_queue import ReliableRedisQueue
from logic.reviews_csv_extractor import Review


class TestReliableRedisQueue:
    
    def setup_method(self):
        with patch.dict(os.environ, {
            'REDIS_HOST': 'localhost',
            'REDIS_PORT': '6379',
            'REDIS_DB': '1',
            'REDIS_PASSWORD': '',
            'REDIS_QUEUE_NAME': 'test_queue'
        }):
            self.queue_helper = ReliableRedisQueue()
        
        self.mock_redis = Mock()
        self.queue_helper.redis_client = self.mock_redis
        self.queue_helper.main_queue = 'test_queue'
        
        self.sample_review = Review(
            review_id='R123',
            date='2025-01-01',
            rating='★★★★☆ (4 stars)',
            text='Great product!'
        )
    
    def test_init_with_env_variables(self):
        with patch.dict(os.environ, {
            'REDIS_HOST': 'test-host',
            'REDIS_PORT': '9999',
            'REDIS_DB': '2',
            'REDIS_PASSWORD': 'test-pass',
            'REDIS_QUEUE_NAME': 'custom_queue'
        }):
            with patch('redis.Redis') as mock_redis_class:
                queue_helper = ReliableRedisQueue()
                mock_redis_class.assert_called_once_with(
                    host='test-host',
                    port=9999,
                    db=2,
                    password='test-pass',
                    decode_responses=True
                )
                assert queue_helper.main_queue == 'custom_queue'
    
    def test_is_connected_success(self):
        self.mock_redis.ping.return_value = True
        assert self.queue_helper.is_connected() == True
        self.mock_redis.ping.assert_called_once()
    
    def test_is_connected_failure(self):
        import redis
        self.mock_redis.ping.side_effect = redis.ConnectionError()
        assert self.queue_helper.is_connected() == False
    
    def test_enqueue_review_success(self):
        self.mock_redis.lpush.return_value = 1
        result = self.queue_helper.enqueue_review(self.sample_review)
        
        assert result == True
        self.mock_redis.lpush.assert_called_once()
        args = self.mock_redis.lpush.call_args[0]
        assert args[0] == 'test_queue'
        assert 'R123' in args[1]
        assert 'Great product!' in args[1]
    
    def test_enqueue_review_failure(self):
        self.mock_redis.lpush.return_value = 0
        result = self.queue_helper.enqueue_review(self.sample_review)
        assert result == False
    
    def test_enqueue_review_dict_success(self):
        self.mock_redis.lpush.return_value = 1
        review_dict = {
            'review_id': 'R456',
            'date': '2025-01-02',
            'rating': '★★★☆☆ (3 stars)',
            'text': 'Good service'
        }
        
        result = self.queue_helper.enqueue_review_dict(review_dict)
        assert result == True
        self.mock_redis.lpush.assert_called_once()
    
    def test_get_queue_stats(self):
        self.mock_redis.llen.side_effect = [5, 2, 1]  # main, processing, failed
        self.mock_redis.zcard.return_value = 3  # retry
        self.mock_redis.keys.return_value = ['key1', 'key2']  # processing keys
        
        stats = self.queue_helper.get_queue_stats()
        
        assert stats['main_queue'] == 5
        assert stats['processing_queue'] == 2
        assert stats['failed_queue'] == 1
        assert stats['retry_queue'] == 3
        assert stats['processing_keys'] == 2
    
    def test_clear_all_queues_success(self):
        self.mock_redis.delete.return_value = 1
        self.mock_redis.keys.return_value = []
        
        result = self.queue_helper.clear_all_queues()
        assert result == True
        
        # Should delete main queues and retry queue
        expected_calls = [
            self.queue_helper.main_queue,
            self.queue_helper.processing_queue,
            self.queue_helper.failed_queue,
            f"{self.queue_helper.main_queue}:retry"
        ]
        
        for expected_call in expected_calls:
            assert any(call[0][0] == expected_call for call in self.mock_redis.delete.call_args_list)
    
    def test_enqueue_reviews_from_csv(self):
        temp_dir = tempfile.mkdtemp()
        csv_filename = 'test_reviews.csv'
        csv_path = os.path.join(temp_dir, csv_filename)
        
        sample_data = [
            ['review_id', 'date', 'rating', 'text'],
            ['R001', '2025-01-01', '★★★★☆ (4 stars)', 'Good product'],
            ['R002', '2025-01-02', '★★★☆☆ (3 stars)', 'Average service']
        ]
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(sample_data)
        
        with patch('logic.reviews_csv_extractor.ReviewsCSVExtractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_extractor_class.return_value = mock_extractor
            mock_extractor.extract_reviews_from_csv.return_value = [
                Review('R001', '2025-01-01', '★★★★☆ (4 stars)', 'Good product'),
                Review('R002', '2025-01-02', '★★★☆☆ (3 stars)', 'Average service')
            ]
            
            self.mock_redis.lpush.return_value = 1
            
            result = self.queue_helper.enqueue_reviews_from_csv(csv_filename)
            
            assert result == 2
            assert self.mock_redis.lpush.call_count == 2
        
        os.remove(csv_path)
        os.rmdir(temp_dir)