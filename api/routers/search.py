from typing import Optional
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ..database import get_db
from ..schemas import (
    MessageSearchResponse,
    MessageSearchResult,
    SearchQuery,
    SortBy,
    SortOrder,
    DateRangeFilter,
    PaginationParams,
    ErrorResponse
)
from ..services.query_service import QueryService
from ..core.logger import logger

router = APIRouter(prefix="/search", tags=["search"])


@router.get(
    "/messages",
    response_model=MessageSearchResponse,
    summary="Search messages",
    description="Search for messages containing specific keywords",
    responses={
        200: {"description": "Successful response"},
        400: {"description": "Invalid parameters", "model": ErrorResponse},
        500: {"description": "Internal server error", "model": ErrorResponse}
    }
)
async def search_messages(
    query: str = Query(..., min_length=1, max_length=100, description="Search query"),
    channel: Optional[str] = Query(None, description="Filter by channel name"),
    start_date: Optional[date] = Query(None, description="Start date filter"),
    end_date: Optional[date] = Query(None, description="End date filter"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    sort_by: SortBy = Query(SortBy.RELEVANCE, description="Sort field"),
    sort_order: SortOrder = Query(SortOrder.DESC, description="Sort order"),
    db: Session = Depends(get_db)
):
    """Search messages with full-text search."""
    try:
        if len(query.strip()) < 2:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Search query must be at least 2 characters"
            )

        query_service = QueryService(db)

        # Create date range filter if provided
        date_range = None
        if start_date or end_date:
            date_range = DateRangeFilter(
                start_date=start_date,
                end_date=end_date
            )

        # Search messages
        results, total_count = query_service.search_messages(
            query=query,
            channel=channel,
            start_date=start_date,
            end_date=end_date,
            page=page,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order
        )

        return MessageSearchResponse(
            query=query,
            total_results=total_count,
            page=page,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order,
            results=results
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching messages: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.get(
    "/products",
    summary="Search products",
    description="Search for specific products across messages",
    responses={
        200: {"description": "Successful response"},
        500: {"description": "Internal server error", "model": ErrorResponse}
    }
)
async def search_products(
    product_name: str = Query(..., min_length=3, description="Product name to search"),
    channel: Optional[str] = Query(None, description="Filter by channel name"),
    min_price: Optional[float] = Query(None, ge=0, description="Minimum price"),
    max_price: Optional[float] = Query(None, ge=0, description="Maximum price"),
    db: Session = Depends(get_db)
):
    """Search for specific products."""
    try:
        # This would be implemented with more sophisticated product extraction
        # For now, use the message search with product name

        query_service = QueryService(db)
        results, total_count = query_service.search_messages(
            query=product_name,
            channel=channel,
            limit=50
        )

        # Extract price information from messages (basic implementation)
        for result in results:
            # Simple price extraction regex
            import re
            price_pattern = r'(\d+\.?\d*)\s*(birr|etb|€|\$|usd)'
            matches = re.findall(price_pattern, result['message_text'].lower())
            result['prices'] = [f"{m[0]} {m[1]}" for m in matches]

        return {
            "product": product_name,
            "total_mentions": total_count,
            "channels": list(set(r['channel_name'] for r in results)),
            "price_range": {
                "min": min_price,
                "max": max_price
            },
            "mentions": results[:10]  # Return top 10 mentions
        }

    except Exception as e:
        logger.error(f"Error searching products: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.get(
    "/channels",
    summary="Search channels",
    description="Search for channels by name or type",
    responses={
        200: {"description": "Successful response"},
        500: {"description": "Internal server error", "model": ErrorResponse}
    }
)
async def search_channels(
    name: Optional[str] = Query(None, description="Channel name search"),
    channel_type: Optional[str] = Query(None, description="Channel type filter"),
    min_posts: Optional[int] = Query(None, ge=0, description="Minimum number of posts"),
    db: Session = Depends(get_db)
):
    """Search for channels."""
    try:
        from ..models import DimChannel

        query = db.query(DimChannel)

        if name:
            query = query.filter(DimChannel.channel_name.ilike(f"%{name}%"))

        if channel_type:
            query = query.filter(DimChannel.channel_type == channel_type)

        if min_posts:
            query = query.filter(DimChannel.total_posts >= min_posts)

        channels = query.order_by(DimChannel.total_views.desc()).limit(50).all()

        return [
            {
                "channel_name": channel.channel_name,
                "channel_type": channel.channel_type,
                "total_posts": channel.total_posts,
                "total_views": channel.total_views,
                "avg_views": channel.avg_views,
                "first_post_date": channel.first_post_date,
                "last_post_date": channel.last_post_date
            }
            for channel in channels
        ]

    except Exception as e:
        logger.error(f"Error searching channels: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
