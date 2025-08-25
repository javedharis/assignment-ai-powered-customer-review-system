import pytest
import tempfile
import os
from datetime import datetime
from unittest.mock import patch
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.database import Base
from models.raw_review import RawReview, RawReviewHelper
from models.structured_review import StructuredReview, StructuredReviewHelper
from models.review_status import ReviewStatus, ReviewStatusHelper, ReviewStatusEnum


class TestDatabaseModels:
    
    def setup_method(self):
        # Create a unique temporary database for each test
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db_path = self.temp_db.name
        self.temp_db.close()
        
        # Create engine and session for this test
        self.engine = create_engine(f'sqlite:///{self.temp_db_path}', echo=False)
        Base.metadata.create_all(bind=self.engine)
        
        # Patch the helpers to use our test database
        self.session_patcher = patch('models.database.engine', self.engine)
        self.session_patcher.start()
        
        self.sessionlocal_patcher = patch('models.database.SessionLocal', sessionmaker(bind=self.engine))
        self.sessionlocal_patcher.start()
        
        # Create helpers
        self.raw_review_helper = RawReviewHelper()
        self.structured_review_helper = StructuredReviewHelper()
        self.review_status_helper = ReviewStatusHelper()
    
    def teardown_method(self):
        # Stop patches
        self.session_patcher.stop()
        self.sessionlocal_patcher.stop()
        
        # Clean up database
        try:
            os.unlink(self.temp_db_path)
        except FileNotFoundError:
            pass
    
    def test_raw_review_creation(self):
        review = self.raw_review_helper.create_raw_review(
            review_id="R001",
            date="2025-01-01",
            rating="★★★★☆ (4 stars)",
            text="Great product!"
        )
        
        assert review.review_id == "R001"
        assert review.date == "2025-01-01"
        assert review.rating == "★★★★☆ (4 stars)"
        assert review.text == "Great product!"
        assert review.created_at is not None
        assert review.updated_at is not None
    
    def test_raw_review_get_by_id(self):
        self.raw_review_helper.create_raw_review("R002", "2025-01-02", "★★★☆☆ (3 stars)", "Average service")
        
        review = self.raw_review_helper.get_raw_review_by_id("R002")
        
        assert review is not None
        assert review.review_id == "R002"
        assert review.text == "Average service"
    
    def test_raw_review_get_by_nonexistent_id(self):
        review = self.raw_review_helper.get_raw_review_by_id("NONEXISTENT")
        assert review is None
    
    def test_raw_review_update(self):
        self.raw_review_helper.create_raw_review("R003", "2025-01-03", "★★★★★ (5 stars)", "Excellent!")
        
        updated_review = self.raw_review_helper.update_raw_review("R003", text="Excellent service!")
        
        assert updated_review.text == "Excellent service!"
        assert updated_review.updated_at is not None
    
    def test_raw_review_delete(self):
        self.raw_review_helper.create_raw_review("R004", "2025-01-04", "★★☆☆☆ (2 stars)", "Poor quality")
        
        result = self.raw_review_helper.delete_raw_review("R004")
        assert result == True
        
        deleted_review = self.raw_review_helper.get_raw_review_by_id("R004")
        assert deleted_review is None
    
    def test_raw_review_bulk_create(self):
        reviews_data = [
            {"review_id": "R005", "date": "2025-01-05", "rating": "★★★★☆ (4 stars)", "text": "Good product"},
            {"review_id": "R006", "date": "2025-01-06", "rating": "★★★☆☆ (3 stars)", "text": "Average product"}
        ]
        
        count = self.raw_review_helper.bulk_create_raw_reviews(reviews_data)
        
        assert count == 2
        review1 = self.raw_review_helper.get_raw_review_by_id("R005")
        review2 = self.raw_review_helper.get_raw_review_by_id("R006")
        assert review1 is not None
        assert review2 is not None
    
    def test_raw_review_to_dict(self):
        review = self.raw_review_helper.create_raw_review("R007", "2025-01-07", "★★★★☆ (4 stars)", "Nice product")
        
        review_dict = review.to_dict()
        
        assert review_dict['review_id'] == "R007"
        assert review_dict['date'] == "2025-01-07"
        assert review_dict['rating'] == "★★★★☆ (4 stars)"
        assert review_dict['text'] == "Nice product"
        assert 'created_at' in review_dict
        assert 'updated_at' in review_dict
    
    def test_structured_review_creation(self):
        self.raw_review_helper.create_raw_review("R101", "2025-01-01", "★★★★☆ (4 stars)", "Great app!")
        
        structured_review = self.structured_review_helper.create_structured_review(
            review_id="R101",
            overall_sentiment="positive",
            sentiment_score=0.8,
            topics_mentioned="app quality",
            problems_identified=None,
            suggested_improvements=None,
            key_insights="User likes the app"
        )
        
        assert structured_review.review_id == "R101"
        assert structured_review.overall_sentiment == "positive"
        assert structured_review.sentiment_score == 0.8
        assert structured_review.topics_mentioned == "app quality"
        assert structured_review.key_insights == "User likes the app"
    
    def test_structured_review_get_by_sentiment(self):
        self.raw_review_helper.create_raw_review("R102", "2025-01-02", "★☆☆☆☆ (1 star)", "Terrible app")
        self.structured_review_helper.create_structured_review("R102", "negative", sentiment_score=0.2)
        
        negative_reviews = self.structured_review_helper.get_reviews_by_sentiment("negative")
        
        assert len(negative_reviews) == 1
        assert negative_reviews[0].review_id == "R102"
        assert negative_reviews[0].overall_sentiment == "negative"
    
    def test_structured_review_get_with_problems(self):
        self.raw_review_helper.create_raw_review("R103", "2025-01-03", "★★☆☆☆ (2 stars)", "App crashes")
        self.structured_review_helper.create_structured_review(
            "R103", "negative", problems_identified="App stability issues"
        )
        
        reviews_with_problems = self.structured_review_helper.get_reviews_with_problems()
        
        assert len(reviews_with_problems) == 1
        assert reviews_with_problems[0].problems_identified == "App stability issues"
    
    def test_review_status_creation(self):
        self.raw_review_helper.create_raw_review("R201", "2025-01-01", "★★★★☆ (4 stars)", "Good service")
        
        review_status = self.review_status_helper.create_review_status(
            review_id="R201",
            status=ReviewStatusEnum.IN_PROGRESS,
            processing_stage="sentiment_analysis"
        )
        
        assert review_status.review_id == "R201"
        assert review_status.status == ReviewStatusEnum.IN_PROGRESS
        assert review_status.processing_stage == "sentiment_analysis"
        assert review_status.processing_started_at is not None
    
    def test_review_status_update(self):
        self.raw_review_helper.create_raw_review("R202", "2025-01-02", "★★★☆☆ (3 stars)", "Average")
        self.review_status_helper.create_review_status("R202", ReviewStatusEnum.IN_PROGRESS)
        
        updated_status = self.review_status_helper.update_review_status(
            "R202", ReviewStatusEnum.COMPLETED, processing_stage="completed"
        )
        
        assert updated_status.status == ReviewStatusEnum.COMPLETED
        assert updated_status.processing_stage == "completed"
    
    def test_review_status_mark_as_completed(self):
        self.raw_review_helper.create_raw_review("R203", "2025-01-03", "★★★★★ (5 stars)", "Excellent")
        self.review_status_helper.create_review_status("R203", ReviewStatusEnum.IN_PROGRESS)
        
        completed_status = self.review_status_helper.mark_as_completed("R203", "120")
        
        assert completed_status.status == ReviewStatusEnum.COMPLETED
        assert completed_status.processing_duration_seconds == "120"
        assert completed_status.processing_completed_at is not None
    
    def test_review_status_mark_as_failed(self):
        self.raw_review_helper.create_raw_review("R204", "2025-01-04", "★★☆☆☆ (2 stars)", "Bad")
        self.review_status_helper.create_review_status("R204", ReviewStatusEnum.IN_PROGRESS)
        
        failed_status = self.review_status_helper.mark_as_failed("R204", "Processing timeout", "1")
        
        assert failed_status.status == ReviewStatusEnum.FAILED
        assert failed_status.error_message == "Processing timeout"
        assert failed_status.retry_count == "1"
    
    def test_review_status_increment_retry_count(self):
        self.raw_review_helper.create_raw_review("R205", "2025-01-05", "★★★☆☆ (3 stars)", "OK")
        self.review_status_helper.create_review_status("R205", ReviewStatusEnum.IN_PROGRESS)
        
        incremented_status = self.review_status_helper.increment_retry_count("R205")
        
        assert incremented_status.retry_count == "1"
        
        incremented_again = self.review_status_helper.increment_retry_count("R205")
        assert incremented_again.retry_count == "2"
    
    def test_review_status_get_by_status(self):
        self.raw_review_helper.create_raw_review("R206", "2025-01-06", "★★★★☆ (4 stars)", "Good")
        self.raw_review_helper.create_raw_review("R207", "2025-01-07", "★★★☆☆ (3 stars)", "Average")
        
        self.review_status_helper.create_review_status("R206", ReviewStatusEnum.COMPLETED)
        self.review_status_helper.create_review_status("R207", ReviewStatusEnum.IN_PROGRESS)
        
        completed_reviews = self.review_status_helper.get_reviews_by_status(ReviewStatusEnum.COMPLETED)
        in_progress_reviews = self.review_status_helper.get_reviews_by_status(ReviewStatusEnum.IN_PROGRESS)
        
        assert len(completed_reviews) == 1
        assert len(in_progress_reviews) == 1
        assert completed_reviews[0].review_id == "R206"
        assert in_progress_reviews[0].review_id == "R207"
    
    def test_review_status_to_dict(self):
        self.raw_review_helper.create_raw_review("R208", "2025-01-08", "★★★★☆ (4 stars)", "Nice")
        status = self.review_status_helper.create_review_status("R208", ReviewStatusEnum.IN_PROGRESS)
        
        status_dict = status.to_dict()
        
        assert status_dict['review_id'] == "R208"
        assert status_dict['status'] == "in-progress"
        assert 'processing_started_at' in status_dict
        assert 'created_at' in status_dict