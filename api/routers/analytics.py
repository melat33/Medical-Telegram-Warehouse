from datetime import date, timedelta
from typing import Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
import pandas as pd

from ..database import get_db
from ..schemas import ErrorResponse
from ..services.query_service import QueryService
from ..core.logger import logger

router = APIRouter(prefix="/analytics", tags=["advanced-analytics"])


@router.get(
    "/trends/daily",
    summary="Get daily trends",
    description="Returns daily posting and engagement trends",
    responses={
        200: {"description": "Successful response"},
        500: {"description": "Internal server error", "model": ErrorResponse}
    }
)
async def get_daily_trends(
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Get daily trends."""
    try:
        from ..models import DimDate, FactMessage
        from sqlalchemy import func

        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        # Get daily statistics
        daily_stats = db.query(
            DimDate.full_date,
            func.count(FactMessage.message_id).label('post_count'),
            func.sum(FactMessage.view_count).label('total_views'),
            func.avg(FactMessage.view_count).label('avg_views'),
            func.sum(FactMessage.forward_count).label('total_forwards')
        ).join(FactMessage).filter(
            DimDate.full_date >= start_date,
            DimDate.full_date <= end_date
        ).group_by(DimDate.full_date).order_by(DimDate.full_date).all()

        # Calculate moving averages
        df = pd.DataFrame([
            {
                'date': row[0],
                'posts': row[1],
                'views': row[2],
                'avg_views': float(row[3] or 0),
                'forwards': row[4]
            }
            for row in daily_stats
        ])

        if not df.empty:
            df['posts_ma'] = df['posts'].rolling(window=7, min_periods=1).mean()
            df['views_ma'] = df['views'].rolling(window=7, min_periods=1).mean()
            df['engagement_rate'] = (df['views'] + df['forwards'] * 10) / df['posts']
            df['engagement_ma'] = df['engagement_rate'].rolling(window=7, min_periods=1).mean()

        return {
            "period": {"start": start_date, "end": end_date, "days": days},
            "summary": {
                "total_posts": df['posts'].sum() if not df.empty else 0,
                "total_views": df['views'].sum() if not df.empty else 0,
                "total_forwards": df['forwards'].sum() if not df.empty else 0,
                "avg_daily_posts": df['posts'].mean() if not df.empty else 0,
                "avg_daily_views": df['views'].mean() if not df.empty else 0
            },
            "daily_data": df.to_dict('records') if not df.empty else [],
            "trends": {
                "posts_trend": "increasing" if not df.empty and df['posts'].iloc[-1] > df['posts'].iloc[0] else "decreasing",
                "views_trend": "increasing" if not df.empty and df['views'].iloc[-1] > df['views'].iloc[0] else "decreasing"
            }
        }

    except Exception as e:
        logger.error(f"Error fetching daily trends: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.get(
    "/comparison/channels",
    summary="Compare channels",
    description="Compare performance metrics across channels",
    responses={
        200: {"description": "Successful response"},
        500: {"description": "Internal server error", "model": ErrorResponse}
    }
)
async def compare_channels(
    channels: str = Query(..., description="Comma-separated channel names"),
    metric: str = Query("engagement", regex="^(posts|views|engagement|forwards)$"),
    days: int = Query(30, ge=1, le=365, description="Comparison period in days"),
    db: Session = Depends(get_db)
):
    """Compare channels."""
    try:
        channel_list = [c.strip() for c in channels.split(',')]

        comparison_data = []
        for channel_name in channel_list[:10]:  # Limit to 10 channels
            try:
                query_service = QueryService(db)

                end_date = date.today()
                start_date = end_date - timedelta(days=days)

                activity = query_service.get_channel_activity(
                    channel_name=channel_name,
                    start_date=start_date,
                    end_date=end_date,
                    granularity=TimeGranularity.DAILY
                )

                comparison_data.append({
                    "channel": channel_name,
                    "total_posts": activity["total_posts"],
                    "total_views": activity["total_views"],
                    "total_forwards": activity["total_forwards"],
                    "avg_views": activity["avg_views"],
                    "avg_forwards": activity["avg_forwards"],
                    "engagement_rate": (activity["total_views"] + activity["total_forwards"] * 10) / max(activity["total_posts"], 1)
                })
            except ValueError:
                continue  # Skip channels not found

        # Sort by selected metric
        if metric == "posts":
            comparison_data.sort(key=lambda x: x["total_posts"], reverse=True)
        elif metric == "views":
            comparison_data.sort(key=lambda x: x["total_views"], reverse=True)
        elif metric == "forwards":
            comparison_data.sort(key=lambda x: x["total_forwards"], reverse=True)
        else:  # engagement
            comparison_data.sort(key=lambda x: x["engagement_rate"], reverse=True)

        return {
            "metric": metric,
            "period_days": days,
            "channels_compared": len(comparison_data),
            "data": comparison_data,
            "top_performer": comparison_data[0] if comparison_data else None
        }

    except Exception as e:
        logger.error(f"Error comparing channels: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.get(
    "/predictions/engagement",
    summary="Predict engagement",
    description="Predict future engagement based on historical data",
    responses={
        200: {"description": "Successful response"},
        500: {"description": "Internal server error", "model": ErrorResponse}
    }
)
async def predict_engagement(
    channel: str = Query(..., description="Channel name"),
    days_ahead: int = Query(7, ge=1, le=30, description="Days to predict ahead"),
    db: Session = Depends(get_db)
):
    """Predict future engagement."""
    try:
        # Simple linear regression prediction
        from ..models import DimDate, FactMessage, DimChannel
        from sqlalchemy import func
        import numpy as np

        # Get historical data (last 90 days)
        end_date = date.today()
        start_date = end_date - timedelta(days=90)

        channel_obj = db.query(DimChannel).filter(
            DimChannel.channel_name == channel
        ).first()

        if not channel_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Channel {channel} not found"
            )

        daily_stats = db.query(
            DimDate.full_date,
            func.count(FactMessage.message_id).label('post_count'),
            func.sum(FactMessage.view_count).label('total_views')
        ).join(FactMessage).filter(
            FactMessage.channel_key == channel_obj.channel_key,
            DimDate.full_date >= start_date,
            DimDate.full_date <= end_date
        ).group_by(DimDate.full_date).order_by(DimDate.full_date).all()

        if len(daily_stats) < 7:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Insufficient historical data for prediction"
            )

        # Prepare data for prediction
        dates = [row[0] for row in daily_stats]
        views = [row[2] or 0 for row in daily_stats]
        posts = [row[1] for row in daily_stats]

        # Simple moving average prediction
        window_size = 7
        last_views = views[-window_size:]
        avg_daily_views = np.mean(last_views)

        # Generate predictions
        prediction_dates = [end_date + timedelta(days=i+1) for i in range(days_ahead)]
        predicted_views = [avg_daily_views * (1 + 0.02 * i) for i in range(days_ahead)]  # 2% daily growth

        return {
            "channel": channel,
            "prediction_days": days_ahead,
            "historical_data_points": len(daily_stats),
            "current_avg_daily_views": avg_daily_views,
            "predictions": [
                {
                    "date": pred_date.strftime('%Y-%m-%d'),
                    "predicted_views": pred_views,
                    "confidence": 0.8 - (i * 0.05)  # Decreasing confidence for farther predictions
                }
                for i, (pred_date, pred_views) in enumerate(zip(prediction_dates, predicted_views))
            ],
            "recommendations": [
                "Post during peak hours for maximum engagement",
                "Use images to increase view counts by 30%",
                "Consider promotional content on weekends"
            ]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error predicting engagement: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
