from sqlalchemy import Column, String, DateTime, Text, create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from models.database import Base, DatabaseManager


class RawReview(Base):
    __tablename__ = 'raw_reviews'
    
    review_id = Column(String(50), primary_key=True)
    date = Column(String(20), nullable=False)
    rating = Column(String(50), nullable=False)
    text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<RawReview(review_id='{self.review_id}', rating='{self.rating}')>"
    
    def to_dict(self):
        return {
            'review_id': self.review_id,
            'date': self.date,
            'rating': self.rating,
            'text': self.text,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class RawReviewHelper:
    
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    def create_raw_review(self, review_id: str, date: str, rating: str, text: str) -> RawReview:
        session = self.db_manager.get_session()
        try:
            raw_review = RawReview(
                review_id=review_id,
                date=date,
                rating=rating,
                text=text
            )
            session.add(raw_review)
            session.commit()
            session.refresh(raw_review)
            return raw_review
        finally:
            self.db_manager.close_session(session)
    
    def get_raw_review_by_id(self, review_id: str) -> RawReview:
        session = self.db_manager.get_session()
        try:
            return session.query(RawReview).filter(RawReview.review_id == review_id).first()
        finally:
            self.db_manager.close_session(session)
    
    def get_all_raw_reviews(self) -> list:
        session = self.db_manager.get_session()
        try:
            return session.query(RawReview).all()
        finally:
            self.db_manager.close_session(session)
    
    def update_raw_review(self, review_id: str, **kwargs) -> RawReview:
        session = self.db_manager.get_session()
        try:
            raw_review = session.query(RawReview).filter(RawReview.review_id == review_id).first()
            if raw_review:
                for key, value in kwargs.items():
                    if hasattr(raw_review, key):
                        setattr(raw_review, key, value)
                raw_review.updated_at = datetime.utcnow()
                session.commit()
                session.refresh(raw_review)
            return raw_review
        finally:
            self.db_manager.close_session(session)
    
    def delete_raw_review(self, review_id: str) -> bool:
        session = self.db_manager.get_session()
        try:
            raw_review = session.query(RawReview).filter(RawReview.review_id == review_id).first()
            if raw_review:
                session.delete(raw_review)
                session.commit()
                return True
            return False
        finally:
            self.db_manager.close_session(session)
    
    def bulk_create_raw_reviews(self, reviews_data: list) -> int:
        session = self.db_manager.get_session()
        try:
            raw_reviews = []
            for review_data in reviews_data:
                raw_review = RawReview(
                    review_id=review_data['review_id'],
                    date=review_data['date'],
                    rating=review_data['rating'],
                    text=review_data['text']
                )
                raw_reviews.append(raw_review)
            
            session.add_all(raw_reviews)
            session.commit()
            return len(raw_reviews)
        finally:
            self.db_manager.close_session(session)