import pytest
import os
import tempfile
import csv
from logic.reviews_csv_extractor import ReviewsCSVExtractor, Review


class TestReviewsCSVExtractor:
    
    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.extractor = ReviewsCSVExtractor(data_files_path=self.temp_dir)
        
        self.sample_data = [
            ['review_id', 'date', 'rating', 'text'],
            ['R12345', '2025-01-01', '★★★☆☆ (3 stars)', 'Great app but search needs improvement'],
            ['R67890', '2025-01-05', '★★★★★ (5 stars)', 'Fast delivery, excellent service'],
            ['R24680', '2025-01-07', '★☆☆☆☆ (1 star)', 'Order was late and incomplete']
        ]
        
        self.test_filename = 'test_reviews.csv'
        self.test_filepath = os.path.join(self.temp_dir, self.test_filename)
        
        with open(self.test_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(self.sample_data)
    
    def teardown_method(self):
        if os.path.exists(self.test_filepath):
            os.remove(self.test_filepath)
        os.rmdir(self.temp_dir)
    
    def test_fetch_csv_file_exists(self):
        result = self.extractor.fetch_csv_file(self.test_filename)
        assert result == self.test_filepath
    
    def test_fetch_csv_file_not_exists(self):
        with pytest.raises(FileNotFoundError):
            self.extractor.fetch_csv_file('nonexistent.csv')
    
    def test_extract_reviews_from_csv_generator(self):
        reviews_generator = self.extractor.extract_reviews_from_csv(self.test_filename)
        reviews = list(reviews_generator)
        
        assert len(reviews) == 3
        assert isinstance(reviews[0], Review)
        assert reviews[0].review_id == 'R12345'
        assert reviews[0].date == '2025-01-01'
        assert reviews[0].rating == '★★★☆☆ (3 stars)'
        assert reviews[0].text == 'Great app but search needs improvement'
    
    def test_extract_reviews_from_csv_is_generator(self):
        reviews_generator = self.extractor.extract_reviews_from_csv(self.test_filename)
        assert hasattr(reviews_generator, '__next__')
        assert hasattr(reviews_generator, '__iter__')
    
    def test_get_reviews_as_dict_generator(self):
        dict_generator = self.extractor.get_reviews_as_dict_generator(self.test_filename)
        dicts = list(dict_generator)
        
        assert len(dicts) == 3
        assert isinstance(dicts[0], dict)
        assert dicts[0]['review_id'] == 'R12345'
        assert dicts[0]['date'] == '2025-01-01'
        assert dicts[0]['rating'] == '★★★☆☆ (3 stars)'
        assert dicts[0]['text'] == 'Great app but search needs improvement'
    
    def test_get_reviews_as_dict_generator_is_generator(self):
        dict_generator = self.extractor.get_reviews_as_dict_generator(self.test_filename)
        assert hasattr(dict_generator, '__next__')
        assert hasattr(dict_generator, '__iter__')
    
    def test_review_to_dict(self):
        review = Review('R123', '2025-01-01', '★★★★☆ (4 stars)', 'Test review')
        result = review.to_dict()
        
        expected = {
            'review_id': 'R123',
            'date': '2025-01-01',
            'rating': '★★★★☆ (4 stars)',
            'text': 'Test review'
        }
        assert result == expected
    
    def test_empty_csv_fields_handled(self):
        empty_data = [
            ['review_id', 'date', 'rating', 'text'],
            ['', '', '', '']
        ]
        
        empty_filename = 'empty_test.csv'
        empty_filepath = os.path.join(self.temp_dir, empty_filename)
        
        with open(empty_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(empty_data)
        
        reviews = list(self.extractor.extract_reviews_from_csv(empty_filename))
        
        assert len(reviews) == 1
        assert reviews[0].review_id == ''
        assert reviews[0].date == ''
        assert reviews[0].rating == ''
        assert reviews[0].text == ''
        
        os.remove(empty_filepath)