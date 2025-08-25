from sqlalchemy import Column, String, DateTime, Text, ForeignKey, Enum
from sqlalchemy.orm import relationship
from datetime import datetime
import enum
from models.database import Base, DatabaseManager


class ReviewStatusEnum(enum.Enum):
    IN_PROGRESS = "in-progress"
    COMPLETED = "completed"
    FAILED = "failed"


class ReviewStatus(Base):
    __tablename__ = 'review_statuses'
    
    review_id = Column(String(50), ForeignKey('raw_reviews.review_id'), primary_key=True)
    status = Column(Enum(ReviewStatusEnum), nullable=False, default=ReviewStatusEnum.IN_PROGRESS)
    processing_stage = Column(String(100), nullable=True)
    error_message = Column(Text, nullable=True)
    processing_started_at = Column(DateTime, default=datetime.utcnow)
    processing_completed_at = Column(DateTime, nullable=True)
    processing_duration_seconds = Column(String(20), nullable=True)
    retry_count = Column(String(10), default="0")
    processing_metadata = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    raw_review = relationship("RawReview", backref="review_status")
    
    def __repr__(self):
        return f"<ReviewStatus(review_id='{self.review_id}', status='{self.status.value}')>"
    
    def to_dict(self):
        return {
            'review_id': self.review_id,
            'status': self.status.value if self.status else None,
            'processing_stage': self.processing_stage,
            'error_message': self.error_message,
            'processing_started_at': self.processing_started_at.isoformat() if self.processing_started_at else None,
            'processing_completed_at': self.processing_completed_at.isoformat() if self.processing_completed_at else None,
            'processing_duration_seconds': self.processing_duration_seconds,
            'retry_count': self.retry_count,
            'processing_metadata': self.processing_metadata,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class ReviewStatusHelper:
    
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    def create_review_status(self, review_id: str, status: ReviewStatusEnum = ReviewStatusEnum.IN_PROGRESS,
                            processing_stage: str = None, processing_metadata: str = None) -> ReviewStatus:
        session = self.db_manager.get_session()
        try:
            review_status = ReviewStatus(
                review_id=review_id,
                status=status,
                processing_stage=processing_stage,
                processing_metadata=processing_metadata
            )
            session.add(review_status)
            session.commit()
            session.refresh(review_status)
            return review_status
        finally:
            self.db_manager.close_session(session)
    
    def get_review_status_by_id(self, review_id: str) -> ReviewStatus:
        session = self.db_manager.get_session()
        try:
            return session.query(ReviewStatus).filter(ReviewStatus.review_id == review_id).first()
        finally:
            self.db_manager.close_session(session)
    
    def get_all_review_statuses(self) -> list:
        session = self.db_manager.get_session()
        try:
            return session.query(ReviewStatus).all()
        finally:
            self.db_manager.close_session(session)
    
    def get_reviews_by_status(self, status: ReviewStatusEnum) -> list:
        session = self.db_manager.get_session()
        try:
            return session.query(ReviewStatus).filter(ReviewStatus.status == status).all()
        finally:
            self.db_manager.close_session(session)
    
    def update_review_status(self, review_id: str, status: ReviewStatusEnum, 
                            processing_stage: str = None, error_message: str = None,
                            processing_completed_at: datetime = None, 
                            processing_duration_seconds: str = None,
                            retry_count: str = None, processing_metadata: str = None) -> ReviewStatus:
        session = self.db_manager.get_session()
        try:
            review_status = session.query(ReviewStatus).filter(ReviewStatus.review_id == review_id).first()
            if review_status:
                review_status.status = status
                if processing_stage is not None:
                    review_status.processing_stage = processing_stage
                if error_message is not None:
                    review_status.error_message = error_message
                if processing_completed_at is not None:
                    review_status.processing_completed_at = processing_completed_at
                if processing_duration_seconds is not None:
                    review_status.processing_duration_seconds = processing_duration_seconds
                if retry_count is not None:
                    review_status.retry_count = retry_count
                if processing_metadata is not None:
                    review_status.processing_metadata = processing_metadata
                
                review_status.updated_at = datetime.utcnow()
                session.commit()
                session.refresh(review_status)
            return review_status
        finally:
            self.db_manager.close_session(session)
    
    def mark_as_completed(self, review_id: str, processing_duration_seconds: str = None,
                         processing_metadata: str = None) -> ReviewStatus:
        return self.update_review_status(
            review_id=review_id,
            status=ReviewStatusEnum.COMPLETED,
            processing_completed_at=datetime.utcnow(),
            processing_duration_seconds=processing_duration_seconds,
            processing_metadata=processing_metadata
        )
    
    def mark_as_failed(self, review_id: str, error_message: str, 
                      retry_count: str = None, processing_metadata: str = None) -> ReviewStatus:
        return self.update_review_status(
            review_id=review_id,
            status=ReviewStatusEnum.FAILED,
            error_message=error_message,
            retry_count=retry_count,
            processing_metadata=processing_metadata
        )
    
    def increment_retry_count(self, review_id: str) -> ReviewStatus:
        session = self.db_manager.get_session()
        try:
            review_status = session.query(ReviewStatus).filter(ReviewStatus.review_id == review_id).first()
            if review_status:
                current_retry = int(review_status.retry_count) if review_status.retry_count else 0
                review_status.retry_count = str(current_retry + 1)
                review_status.updated_at = datetime.utcnow()
                session.commit()
                session.refresh(review_status)
            return review_status
        finally:
            self.db_manager.close_session(session)
    
    def delete_review_status(self, review_id: str) -> bool:
        session = self.db_manager.get_session()
        try:
            review_status = session.query(ReviewStatus).filter(ReviewStatus.review_id == review_id).first()
            if review_status:
                session.delete(review_status)
                session.commit()
                return True
            return False
        finally:
            self.db_manager.close_session(session)