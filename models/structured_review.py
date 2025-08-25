from sqlalchemy import Column, String, DateTime, Text, Float, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from models.database import Base, DatabaseManager


class StructuredReview(Base):
    __tablename__ = 'structured_reviews'
    
    review_id = Column(String(50), ForeignKey('raw_reviews.review_id'), primary_key=True)
    overall_sentiment = Column(String(20), nullable=False)
    sentiment_score = Column(Float, nullable=True)
    topics_mentioned = Column(Text, nullable=True)
    problems_identified = Column(Text, nullable=True)
    suggested_improvements = Column(Text, nullable=True)
    key_insights = Column(Text, nullable=True)
    processing_metadata = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    raw_review = relationship("RawReview", backref="structured_review")
    
    def __repr__(self):
        return f"<StructuredReview(review_id='{self.review_id}', sentiment='{self.overall_sentiment}')>"
    
    def to_dict(self):
        return {
            'review_id': self.review_id,
            'overall_sentiment': self.overall_sentiment,
            'sentiment_score': self.sentiment_score,
            'topics_mentioned': self.topics_mentioned,
            'problems_identified': self.problems_identified,
            'suggested_improvements': self.suggested_improvements,
            'key_insights': self.key_insights,
            'processing_metadata': self.processing_metadata,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class StructuredReviewHelper:
    
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    def create_structured_review(self, review_id: str, overall_sentiment: str, 
                                sentiment_score: float = None, topics_mentioned: str = None,
                                problems_identified: str = None, suggested_improvements: str = None,
                                key_insights: str = None, processing_metadata: str = None) -> StructuredReview:
        session = self.db_manager.get_session()
        try:
            structured_review = StructuredReview(
                review_id=review_id,
                overall_sentiment=overall_sentiment,
                sentiment_score=sentiment_score,
                topics_mentioned=topics_mentioned,
                problems_identified=problems_identified,
                suggested_improvements=suggested_improvements,
                key_insights=key_insights,
                processing_metadata=processing_metadata
            )
            session.add(structured_review)
            session.commit()
            session.refresh(structured_review)
            return structured_review
        finally:
            self.db_manager.close_session(session)
    
    def get_structured_review_by_id(self, review_id: str) -> StructuredReview:
        session = self.db_manager.get_session()
        try:
            return session.query(StructuredReview).filter(StructuredReview.review_id == review_id).first()
        finally:
            self.db_manager.close_session(session)
    
    def get_all_structured_reviews(self) -> list:
        session = self.db_manager.get_session()
        try:
            return session.query(StructuredReview).all()
        finally:
            self.db_manager.close_session(session)
    
    def get_reviews_by_sentiment(self, sentiment: str) -> list:
        session = self.db_manager.get_session()
        try:
            return session.query(StructuredReview).filter(StructuredReview.overall_sentiment == sentiment).all()
        finally:
            self.db_manager.close_session(session)
    
    def update_structured_review(self, review_id: str, **kwargs) -> StructuredReview:
        session = self.db_manager.get_session()
        try:
            structured_review = session.query(StructuredReview).filter(StructuredReview.review_id == review_id).first()
            if structured_review:
                for key, value in kwargs.items():
                    if hasattr(structured_review, key):
                        setattr(structured_review, key, value)
                structured_review.updated_at = datetime.utcnow()
                session.commit()
                session.refresh(structured_review)
            return structured_review
        finally:
            self.db_manager.close_session(session)
    
    def delete_structured_review(self, review_id: str) -> bool:
        session = self.db_manager.get_session()
        try:
            structured_review = session.query(StructuredReview).filter(StructuredReview.review_id == review_id).first()
            if structured_review:
                session.delete(structured_review)
                session.commit()
                return True
            return False
        finally:
            self.db_manager.close_session(session)
    
    def get_reviews_with_problems(self) -> list:
        session = self.db_manager.get_session()
        try:
            return session.query(StructuredReview).filter(StructuredReview.problems_identified.isnot(None)).all()
        finally:
            self.db_manager.close_session(session)
    
    def get_reviews_with_suggestions(self) -> list:
        session = self.db_manager.get_session()
        try:
            return session.query(StructuredReview).filter(StructuredReview.suggested_improvements.isnot(None)).all()
        finally:
            self.db_manager.close_session(session)