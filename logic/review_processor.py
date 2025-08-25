import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field

from models.raw_review import RawReviewHelper
from models.review_status import ReviewStatusHelper, ReviewStatusEnum
from models.structured_review import StructuredReviewHelper

load_dotenv()


class ReviewInsights(BaseModel):
    review_metadata: Dict[str, Any] = Field(description="Extracted metadata from the review")
    overall_sentiment: str = Field(description="Overall sentiment: positive, negative, or neutral")
    sentiment_score: float = Field(description="Sentiment score from -1.0 (very negative) to 1.0 (very positive)")
    topics_mentioned: List[str] = Field(description="List of main topics or categories mentioned")
    problems_identified: List[str] = Field(description="List of specific problems or issues identified")
    suggested_improvements: List[str] = Field(description="List of suggested improvements or solutions")
    key_phrases: List[str] = Field(description="Important phrases or keywords from the review")


class ReviewProcessor:
    
    def __init__(self):
        # Database helpers
        self.raw_review_helper = RawReviewHelper()
        self.review_status_helper = ReviewStatusHelper()
        self.structured_review_helper = StructuredReviewHelper()
        
        # AI processing setup
        self.deepseek_api_key = os.getenv('DEEPSEEK_API_KEY')
        self.deepseek_base_url = os.getenv('DEEPSEEK_BASE_URL', 'https://api.deepseek.com/v1')
        
        if not self.deepseek_api_key:
            raise ValueError("DEEPSEEK_API_KEY not found in environment variables")
        
        self.llm = ChatOpenAI(
            model="deepseek-chat",
            temperature=0.1,
            api_key=self.deepseek_api_key,
            base_url=self.deepseek_base_url
        )
        
        self.parser = PydanticOutputParser(pydantic_object=ReviewInsights)
        
        self.prompt_template = PromptTemplate(
            template="""
You are an AI expert in analyzing customer reviews for e-commerce platforms. Your task is to extract structured insights from customer reviews.

Please analyze the following customer review and extract the requested information:

Review Data:
- Review ID: {review_id}
- Date: {date}  
- Rating: {rating}
- Review Text: "{text}"

Instructions:
1. Extract review metadata (review_id, date, rating, text_length)
2. Determine the overall sentiment (positive, negative, neutral) and provide a sentiment score
3. Identify main topics mentioned (e.g., product quality, delivery, customer service, app functionality, pricing)
4. List specific problems or issues identified in the review
5. Extract any suggested improvements or solutions mentioned
6. Identify key phrases that capture the essence of the review

Be thorough but concise. Focus on actionable insights that would be valuable for product and operations teams.

{format_instructions}

Analysis:
""",
            input_variables=["review_id", "date", "rating", "text"],
            partial_variables={"format_instructions": self.parser.get_format_instructions()}
        )
    
    def process_review_complete(self, review_data: Dict) -> Dict:
        """
        Complete end-to-end review processing:
        1. Save raw review to database
        2. Create status tracking record
        3. Generate AI insights
        4. Save structured review
        5. Update status to completed
        
        Args:
            review_data: Dictionary with review_id, date, rating, text
            
        Returns:
            Dictionary with processing result
        """
        review_id = review_data.get('review_id')
        processing_start_time = datetime.utcnow()
        
        try:
            # Step 1: Save raw review and create status
            self._save_raw_review_and_create_status(review_data)
            
            # Step 2: Generate AI insights
            self._update_status(review_id, ReviewStatusEnum.IN_PROGRESS, "processing_insights")
            ai_result = self._generate_ai_insights(review_data)
            
            if not ai_result.get('success'):
                raise Exception(f"AI processing failed: {ai_result.get('error')}")
            
            # Step 3: Save structured review
            self._update_status(review_id, ReviewStatusEnum.IN_PROGRESS, "saving_structured_review")
            structured_data = self._convert_ai_result_to_structured(ai_result)
            self._save_structured_review(review_id, structured_data)
            
            # Step 4: Mark as completed
            processing_duration = self._calculate_duration(processing_start_time)
            self._mark_as_completed(review_id, processing_duration, structured_data)
            
            return {
                'status': 'success',
                'review_id': review_id,
                'processing_duration_seconds': processing_duration,
                'structured_data': structured_data
            }
            
        except Exception as e:
            error_message = str(e)
            self._mark_as_failed(review_id, error_message)
            return {
                'status': 'failed',
                'review_id': review_id,
                'error': error_message
            }
    
    def retry_failed_review(self, review_id: str, max_retries: int = 3) -> Dict:
        """
        Retry processing a failed review.
        
        Args:
            review_id: ID of the review to retry
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dictionary with retry result
        """
        # Get current status
        status = self.review_status_helper.get_review_status_by_id(review_id)
        if not status:
            return {'status': 'error', 'message': 'Review status not found'}
        
        # Check retry count
        current_retries = int(status.retry_count) if status.retry_count else 0
        if current_retries >= max_retries:
            return {
                'status': 'error', 
                'message': f'Maximum retries ({max_retries}) exceeded'
            }
        
        # Get raw review data
        raw_review = self.raw_review_helper.get_raw_review_by_id(review_id)
        if not raw_review:
            return {'status': 'error', 'message': 'Raw review not found'}
        
        # Increment retry count
        self.review_status_helper.increment_retry_count(review_id)
        
        # Process the review again
        review_data = raw_review.to_dict()
        return self.process_review_complete(review_data)
    
    def _save_raw_review_and_create_status(self, review_data: Dict) -> None:
        """Save raw review and create initial status record."""
        review_id = review_data.get('review_id')
        
        # Check if raw review already exists (for retries)
        existing_raw_review = self.raw_review_helper.get_raw_review_by_id(review_id)
        if not existing_raw_review:
            # Save raw review only if it doesn't exist
            self.raw_review_helper.create_raw_review(
                review_id=review_id,
                date=review_data.get('date'),
                rating=review_data.get('rating'),
                text=review_data.get('text')
            )
        
        # Check if status record already exists
        existing_status = self.review_status_helper.get_review_status_by_id(review_id)
        if not existing_status:
            # Create status record only if it doesn't exist
            self.review_status_helper.create_review_status(
                review_id=review_id,
                status=ReviewStatusEnum.IN_PROGRESS,
                processing_stage="raw_review_saved",
                processing_metadata=json.dumps({
                    "raw_review_created_at": datetime.utcnow().isoformat()
                })
            )
        else:
            # Update existing status to indicate retry
            self.review_status_helper.update_review_status(
                review_id=review_id,
                status=ReviewStatusEnum.IN_PROGRESS,
                processing_stage="retry_processing",
                processing_metadata=json.dumps({
                    "retry_started_at": datetime.utcnow().isoformat()
                })
            )
    
    def _update_status(self, review_id: str, status: ReviewStatusEnum, processing_stage: str, 
                      error_message: str = None, processing_metadata: Dict = None) -> None:
        """Update review processing status."""
        metadata_str = json.dumps(processing_metadata) if processing_metadata else None
        
        self.review_status_helper.update_review_status(
            review_id=review_id,
            status=status,
            processing_stage=processing_stage,
            error_message=error_message,
            processing_metadata=metadata_str
        )
    
    def _generate_ai_insights(self, review_data: Dict) -> Dict:
        """Generate AI insights from review text."""
        try:
            review_id = review_data.get('review_id')
            date = review_data.get('date')
            rating = review_data.get('rating')
            text = review_data.get('text', '')
            
            if not text.strip():
                return {
                    'success': False,
                    'error': 'Empty review text provided',
                    'review_id': review_id
                }
            
            # Prepare the prompt
            formatted_prompt = self.prompt_template.format(
                review_id=review_id,
                date=date,
                rating=rating,
                text=text
            )
            
            # Get AI analysis
            response = self.llm.invoke(formatted_prompt)
            
            # Parse the structured output
            insights = self.parser.parse(response.content)
            
            # Add text length to metadata
            insights.review_metadata['text_length'] = len(text)
            
            result = {
                'success': True,
                'review_id': review_id,
                'insights': {
                    'metadata': insights.review_metadata,
                    'overall_sentiment': insights.overall_sentiment,
                    'sentiment_score': insights.sentiment_score,
                    'topics_mentioned': insights.topics_mentioned,
                    'problems_identified': insights.problems_identified,
                    'suggested_improvements': insights.suggested_improvements,
                    'key_phrases': insights.key_phrases
                },
                'raw_response': response.content
            }
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'error_type': type(e).__name__,
                'review_id': review_data.get('review_id')
            }
    
    def _convert_ai_result_to_structured(self, ai_result: Dict) -> Dict:
        """Convert AI processing result to structured format."""
        insights = ai_result.get('insights', {})
        return {
            'overall_sentiment': insights.get('overall_sentiment'),
            'sentiment_score': insights.get('sentiment_score'),
            'topics_mentioned': insights.get('topics_mentioned', []),
            'problems_identified': insights.get('problems_identified', []),
            'suggested_improvements': insights.get('suggested_improvements', []),
            'key_insights': insights.get('key_phrases', [])
        }
    
    def _save_structured_review(self, review_id: str, structured_data: Dict) -> None:
        """Save structured review to database."""
        # Check if structured review already exists (for retries)
        existing_structured_review = self.structured_review_helper.get_structured_review_by_id(review_id)
        
        if existing_structured_review:
            # Update existing structured review
            self.structured_review_helper.update_structured_review(
                review_id=review_id,
                overall_sentiment=structured_data.get('overall_sentiment'),
                sentiment_score=structured_data.get('sentiment_score'),
                topics_mentioned=json.dumps(structured_data.get('topics_mentioned', [])),
                problems_identified=json.dumps(structured_data.get('problems_identified', [])),
                suggested_improvements=json.dumps(structured_data.get('suggested_improvements', [])),
                key_insights=json.dumps(structured_data.get('key_insights', [])),
                processing_metadata=json.dumps({
                    "processing_version": "1.0",
                    "updated_at": datetime.utcnow().isoformat()
                })
            )
        else:
            # Create new structured review
            self.structured_review_helper.create_structured_review(
                review_id=review_id,
                overall_sentiment=structured_data.get('overall_sentiment'),
                sentiment_score=structured_data.get('sentiment_score'),
                topics_mentioned=json.dumps(structured_data.get('topics_mentioned', [])),
                problems_identified=json.dumps(structured_data.get('problems_identified', [])),
                suggested_improvements=json.dumps(structured_data.get('suggested_improvements', [])),
                key_insights=json.dumps(structured_data.get('key_insights', [])),
                processing_metadata=json.dumps({
                    "processing_version": "1.0",
                    "structured_review_created_at": datetime.utcnow().isoformat()
                })
            )
    
    def _calculate_duration(self, start_time: datetime) -> str:
        """Calculate processing duration in seconds."""
        end_time = datetime.utcnow()
        duration = end_time - start_time
        return str(duration.total_seconds())
    
    def _mark_as_completed(self, review_id: str, processing_duration: str, structured_data: Dict) -> None:
        """Mark review processing as completed."""
        completion_metadata = {
            "completion_timestamp": datetime.utcnow().isoformat(),
            "insights_generated": True,
            "structured_review_created": True,
            "total_topics": len(structured_data.get('topics_mentioned', [])),
            "total_problems": len(structured_data.get('problems_identified', [])),
            "total_suggestions": len(structured_data.get('suggested_improvements', []))
        }
        
        self.review_status_helper.mark_as_completed(
            review_id=review_id,
            processing_duration_seconds=processing_duration,
            processing_metadata=json.dumps(completion_metadata)
        )
    
    def _mark_as_failed(self, review_id: str, error_message: str) -> None:
        """Mark review processing as failed."""
        failure_metadata = {
            "failure_timestamp": datetime.utcnow().isoformat(),
            "failure_stage": "processing",
            "error_type": type(Exception).__name__
        }
        
        self.review_status_helper.mark_as_failed(
            review_id=review_id,
            error_message=error_message,
            processing_metadata=json.dumps(failure_metadata)
        )
    
    # Status and monitoring methods
    def get_review_status(self, review_id: str) -> Optional[Dict]:
        """Get processing status of a specific review."""
        status = self.review_status_helper.get_review_status_by_id(review_id)
        return status.to_dict() if status else None
    
    def get_all_statuses(self) -> List[Dict]:
        """Get all review processing statuses."""
        statuses = self.review_status_helper.get_all_review_statuses()
        return [status.to_dict() for status in statuses]
    
    def get_reviews_by_status(self, status: ReviewStatusEnum) -> List[Dict]:
        """Get reviews by their processing status."""
        statuses = self.review_status_helper.get_reviews_by_status(status)
        return [status.to_dict() for status in statuses]
    
    def get_failed_reviews(self) -> List[Dict]:
        """Get all failed reviews."""
        return self.get_reviews_by_status(ReviewStatusEnum.FAILED)
    
    def get_processing_summary(self) -> Dict:
        """Get summary of all review processing statuses."""
        all_statuses = self.get_all_statuses()
        
        summary = {
            'total_reviews': len(all_statuses),
            'completed': 0,
            'in_progress': 0,
            'failed': 0
        }
        
        for status in all_statuses:
            status_value = status.get('status', '').lower()
            if status_value == 'completed':
                summary['completed'] += 1
            elif status_value == 'in-progress':
                summary['in_progress'] += 1
            elif status_value == 'failed':
                summary['failed'] += 1
        
        return summary