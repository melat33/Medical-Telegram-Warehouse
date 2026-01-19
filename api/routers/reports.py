from typing import Optional
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ..database import get_db
from ..schemas import (
    TopProductsResponse,
    VisualContentResponse,
    AnalyticsResponse,
    TimeGranularity,
    ErrorResponse
)
from ..services.query_service import QueryService
from ..core.logger import logger

router = APIRouter(prefix="/reports", tags=["reports"])


@router.get(
    "/top-products",
    response_model=TopProductsResponse,
    summary="Get top mentioned products",
    description="Returns the most frequently mentioned medical products across channels",
    responses={
        200: {"description": "Successful response"},
        400: {"description": "Invalid parameters", "model": ErrorResponse},
        500: {"description": "Internal server error", "model": ErrorResponse}
    }
)
async def get_top_products(
    limit: int = Query(10, ge=1, le=50, description="Number of top products to return"),
    timeframe: Optional[str] = Query(
        None,
        regex="^(day|week|month|all)$",
        description="Timeframe for analysis"
    ),
    channel: Optional[str] = Query(None, description="Filter by channel name"),
    db: Session = Depends(get_db)
):
    """Get top mentioned medical products."""
    try:
        query_service = QueryService(db)

        products = query_service.get_top_products(
            limit=limit,
            timeframe=timeframe,
            channel=channel
        )

        return TopProductsResponse(
            timeframe=timeframe or "all",
            channel=channel,
            limit=limit,
            total_mentions=sum(p["mention_count"] for p in products),
            products=products
        )

    except ValueError as e:
        logger.warning(f"Invalid request for top products: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error fetching top products: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.get(
    "/visual-content",
    response_model=VisualContentResponse,
    summary="Get visual content statistics",
    description="Returns statistics about image usage and object detection across channels",
    responses={
        200: {"description": "Successful response"},
        500: {"description": "Internal server error", "model": ErrorResponse}
    }
)
async def get_visual_content_stats(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    channel: Optional[str] = Query(None, description="Filter by channel name"),
    db: Session = Depends(get_db)
):
    """Get visual content statistics."""
    try:
        query_service = QueryService(db)

        stats = query_service.get_visual_content_stats(
            start_date=start_date,
            end_date=end_date,
            channel=channel
        )

        return VisualContentResponse(**stats)

    except Exception as e:
        logger.error(f"Error fetching visual content stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.get(
    "/analytics",
    response_model=AnalyticsResponse,
    summary="Get comprehensive analytics",
    description="Returns overall analytics dashboard with performance metrics and recommendations",
    responses={
        200: {"description": "Successful response"},
        500: {"description": "Internal server error", "model": ErrorResponse}
    }
)
async def get_analytics_dashboard(
    db: Session = Depends(get_db)
):
    """Get analytics dashboard."""
    try:
        query_service = QueryService(db)

        analytics = query_service.get_analytics_dashboard()

        return AnalyticsResponse(**analytics)

    except Exception as e:
        logger.error(f"Error fetching analytics dashboard: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.get(
    "/engagement-trends",
    summary="Get engagement trends",
    description="Returns engagement trends over time",
    responses={
        200: {"description": "Successful response"},
        500: {"description": "Internal server error", "model": ErrorResponse}
    }
)
async def get_engagement_trends(
    granularity: TimeGranularity = Query(TimeGranularity.DAILY, description="Time granularity"),
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Get engagement trends over time."""
    try:
        query_service = QueryService(db)

        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        # This would be implemented in QueryService
        # For now, return placeholder
        return {
            "granularity": granularity,
            "period_start": start_date,
            "period_end": end_date,
            "trends": []
        }

    except Exception as e:
        logger.error(f"Error fetching engagement trends: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
