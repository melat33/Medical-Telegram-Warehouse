from datetime import datetime, date
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator, HttpUrl
from enum import Enum


# Enums
class ImageCategory(str, Enum):
    PROMOTIONAL = "promotional"
    PRODUCT_DISPLAY = "product_display"
    LIFESTYLE = "lifestyle"
    OTHER = "other"


class TimeGranularity(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class SortBy(str, Enum):
    VIEWS = "views"
    FORWARDS = "forwards"
    DATE = "date"
    RELEVANCE = "relevance"


class SortOrder(str, Enum):
    ASC = "asc"
    DESC = "desc"


# Base Schemas
class HealthCheck(BaseModel):
    """Health check response."""
    status: str
    timestamp: datetime
    database: bool
    redis: bool
    version: str


# Channel Schemas
class ChannelBase(BaseModel):
    """Base channel schema."""
    channel_name: str
    channel_type: str


class ChannelStats(ChannelBase):
    """Channel statistics."""
    total_posts: int
    total_views: int
    total_forwards: int
    avg_views: float
    avg_engagement: float
    first_post_date: Optional[datetime]
    last_post_date: Optional[datetime]


class ChannelActivity(BaseModel):
    """Channel activity data point."""
    period: str
    post_count: int
    avg_views: float
    avg_forwards: float
    total_views: int


class ChannelActivityResponse(BaseModel):
    """Channel activity response."""
    channel: str
    period_start: date
    period_end: date
    granularity: TimeGranularity
    total_posts: int
    total_views: int
    total_forwards: int
    avg_views: float
    avg_forwards: float
    activity_data: List[ChannelActivity]
    peak_hours: List[int]


# Product Schemas
class ProductMention(BaseModel):
    """Product mention statistics."""
    product_name: str
    mention_count: int
    unique_channels: int
    avg_sentiment: Optional[float] = None
    first_mentioned: Optional[datetime] = None
    last_mentioned: Optional[datetime] = None
    channels: List[str] = []


class TopProductsResponse(BaseModel):
    """Top products response."""
    timeframe: str
    channel: Optional[str] = None
    limit: int
    total_mentions: int
    products: List[ProductMention]


# Message Schemas
class MessageSearchResult(BaseModel):
    """Message search result."""
    message_id: int
    channel_name: str
    message_date: datetime
    message_text: str
    view_count: int
    forward_count: int
    has_image: bool
    extracted_products: Optional[List[str]] = []
    highlight: Optional[str] = None  # Highlighted search term


class MessageSearchResponse(BaseModel):
    """Message search response."""
    query: str
    total_results: int
    page: int
    limit: int
    sort_by: SortBy
    sort_order: SortOrder
    results: List[MessageSearchResult]


# Visual Content Schemas
class VisualContentStats(BaseModel):
    """Visual content statistics."""
    channel_name: str
    total_posts: int
    posts_with_images: int
    image_percentage: float
    category_distribution: Dict[str, int]
    avg_confidence: float
    top_objects: List[Dict[str, Any]]


class VisualContentResponse(BaseModel):
    """Visual content response."""
    period_start: date
    period_end: date
    channel_filter: Optional[str] = None
    total_analyzed_images: int
    channel_stats: List[VisualContentStats]
    overall_stats: Dict[str, Any]


# Analytics Schemas
class TrendAnalysis(BaseModel):
    """Trend analysis result."""
    metric: str
    current_value: float
    previous_value: float
    change_percentage: float
    trend: str  # up, down, stable


class AnalyticsResponse(BaseModel):
    """Analytics dashboard response."""
    timestamp: datetime
    overall_stats: Dict[str, Any]
    channel_performance: List[Dict[str, Any]]
    trending_products: List[ProductMention]
    visual_content_trends: Dict[str, Any]
    recommendations: List[str]


# Request Schemas
class DateRangeFilter(BaseModel):
    """Date range filter."""
    start_date: Optional[date] = None
    end_date: Optional[date] = None

    @validator('end_date')
    def validate_date_range(cls, v, values):
        if v and values.get('start_date') and v < values['start_date']:
            raise ValueError('end_date must be after start_date')
        return v


class PaginationParams(BaseModel):
    """Pagination parameters."""
    page: int = Field(default=1, ge=1)
    limit: int = Field(default=20, ge=1, le=100)

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.limit


class SearchQuery(BaseModel):
    """Search query parameters."""
    query: str = Field(..., min_length=1, max_length=100)
    channel: Optional[str] = None
    date_range: Optional[DateRangeFilter] = None
    sort_by: SortBy = SortBy.RELEVANCE
    sort_order: SortOrder = SortOrder.DESC


# Error Schemas
class ErrorResponse(BaseModel):
    """Error response."""
    detail: str
    error_code: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ValidationError(BaseModel):
    """Validation error."""
    field: str
    message: str
