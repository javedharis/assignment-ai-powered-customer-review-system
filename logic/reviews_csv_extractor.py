import csv
import os
from typing import Dict, Generator
from dataclasses import dataclass


@dataclass
class Review:
    review_id: str
    date: str
    rating: str
    text: str
    
    def to_dict(self) -> Dict[str, str]:
        return {
            'review_id': self.review_id,
            'date': self.date,
            'rating': self.rating,
            'text': self.text
        }


class ReviewsCSVExtractor:
    
    def __init__(self, data_files_path: str = "data_files"):
        self.data_files_path = data_files_path
    
    def fetch_csv_file(self, filename: str) -> str:
        file_path = os.path.join(self.data_files_path, filename)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        return file_path
    
    def extract_reviews_from_csv(self, filename: str) -> Generator[Review, None, None]:
        file_path = self.fetch_csv_file(filename)
        
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                review = Review(
                    review_id=row.get('review_id', ''),
                    date=row.get('date', ''),
                    rating=row.get('rating', ''),
                    text=row.get('text', '')
                )
                yield review
    
    def get_reviews_as_dict_generator(self, filename: str) -> Generator[Dict[str, str], None, None]:
        for review in self.extract_reviews_from_csv(filename):
            yield review.to_dict()