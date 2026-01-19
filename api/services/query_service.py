from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc, text, and_, or_, case
from sqlalchemy.sql import select
import re

from ..models import DimChannel, DimDate, FactMessage, FactImageDetection
from ..schemas import (
    TimeGranularity, SortBy, SortOrder,
    DateRangeFilter, SearchQuery, ImageCategory
)
from .cache_service import cached, CacheService
from ..core.logger import logger


class QueryService:
    """Service for executing complex queries."""

    def __init__(self, db: Session):
        self.db = db
        self.cache = CacheService()

    # Top Products Analysis
    @cached("top_products", ttl=600)  # 10 minutes cache
    def get_top_products(
        self,
        limit: int = 10,
        timeframe: Optional[str] = None,
        channel: Optional[str] = None,
        min_mentions: int = 2
    ) -> List[Dict[str, Any]]:
        """Get top mentioned products."""

        # Common medical product patterns
        product_patterns = [
            r'\b(paracetamol|panadol)\b',
            r'\b(amoxicillin|amox)\b',
            r'\b(cephalexin|keflex)\b',
            r'\b(metformin|glucophage)\b',
            r'\b(insulin)\b',
            r'\b(vitamin\s+[a-z]+)\b',
            r'\b(cream|ointment|gel)\b',
            r'\b(syrup|suspension)\b',
            r'\b(tablet|pill|capsule)\b',
            r'\b(injection|injectable)\b'
        ]

        # Build base query
        query = self.db.query(FactMessage)

        # Apply filters
        if timeframe:
            if timeframe == 'week':
                cutoff_date = datetime.utcnow() - timedelta(days=7)
            elif timeframe == 'month':
                cutoff_date = datetime.utcnow() - timedelta(days=30)
            else:
                cutoff_date = None

            if cutoff_date:
                query = query.join(DimDate).filter(DimDate.full_date >= cutoff_date)

        if channel:
            query = query.join(DimChannel).filter(DimChannel.channel_name == channel)

        # Extract products and count mentions
        product_counts = {}

        for message in query.all():
            if not message.message_text:
                continue

            text_lower = message.message_text.lower()

            for pattern in product_patterns:
                matches = re.findall(pattern, text_lower)
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[0]

                    product_counts[match] = product_counts.get(match, 0) + 1

        # Sort and limit results
        sorted_products = sorted(
            product_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:limit]

        # Format results
        results = []
        for product, count in sorted_products:
            if count >= min_mentions:
                results.append({
                    "product_name": product.title(),
                    "mention_count": count,
                    "unique_channels": 1,  # Would need channel grouping for accurate count
                    "first_mentioned": None,  # Would need timestamp tracking
                    "last_mentioned": None
                })

        return results

    # Channel Activity Analysis
    @cached("channel_activity", ttl=300)  # 5 minutes cache
    def get_channel_activity(
        self,
        channel_name: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        granularity: TimeGranularity = TimeGranularity.DAILY
    ) -> Dict[str, Any]:
        """Get channel activity over time."""

        if not start_date:
            start_date = date.today() - timedelta(days=30)
        if not end_date:
            end_date = date.today()

        # Get channel info
        channel = self.db.query(DimChannel).filter(
            DimChannel.channel_name == channel_name
        ).first()

        if not channel:
            raise ValueError(f"Channel {channel_name} not found")

        # Build activity query
        query = self.db.query(
            DimDate.full_date,
            func.count(FactMessage.message_id).label('post_count'),
            func.avg(FactMessage.view_count).label('avg_views'),
            func.avg(FactMessage.forward_count).label('avg_forwards'),
            func.sum(FactMessage.view_count).label('total_views')
        ).join(
            FactMessage, FactMessage.date_key == DimDate.date_key
        ).filter(
            FactMessage.channel_key == channel.channel_key,
            DimDate.full_date >= start_date,
            DimDate.full_date <= end_date
        )

        # Apply granularity
        if granularity == TimeGranularity.DAILY:
            query = query.group_by(DimDate.full_date).order_by(DimDate.full_date)
        elif granularity == TimeGranularity.WEEKLY:
            query = query.group_by(DimDate.year, DimDate.week_of_year).order_by(
                DimDate.year, DimDate.week_of_year
            )
        elif granularity == TimeGranularity.MONTHLY:
            query = query.group_by(DimDate.year, DimDate.month).order_by(
                DimDate.year, DimDate.month
            )

        activity_data = query.all()

        # Get peak hours
        peak_hours_query = text("""
            SELECT EXTRACT(HOUR FROM dm.full_date) as hour,
                   COUNT(*) as post_count
            FROM marts.fct_messages fm
            JOIN marts.dim_dates dm ON fm.date_key = dm.date_key
            WHERE fm.channel_key = :channel_key
            GROUP BY hour
            ORDER BY post_count DESC
            LIMIT 3
        """)

        peak_hours_result = self.db.execute(
            peak_hours_query,
            {"channel_key": channel.channel_key}
        ).fetchall()

        peak_hours = [int(row[0]) for row in peak_hours_result]

        # Format response
        formatted_data = []
        for row in activity_data:
            if granularity == TimeGranularity.DAILY:
                period = row[0].strftime('%Y-%m-%d')
            elif granularity == TimeGranularity.WEEKLY:
                period = f"Week {row[0].isocalendar()[1]}, {row[0].year}"
            elif granularity == TimeGranularity.MONTHLY:
                period = row[0].strftime('%Y-%m')

            formatted_data.append({
                "period": period,
                "post_count": row[1],
                "avg_views": float(row[2] or 0),
                "avg_forwards": float(row[3] or 0),
                "total_views": row[4] or 0
            })

        # Get totals
        totals = self.db.query(
            func.count(FactMessage.message_id).label('total_posts'),
            func.sum(FactMessage.view_count).label('total_views'),
            func.sum(FactMessage.forward_count).label('total_forwards'),
            func.avg(FactMessage.view_count).label('avg_views'),
            func.avg(FactMessage.forward_count).label('avg_forwards')
        ).filter(
            FactMessage.channel_key == channel.channel_key,
            FactMessage.date.has(DimDate.full_date >= start_date),
            FactMessage.date.has(DimDate.full_date <= end_date)
        ).first()

        return {
            "channel": channel_name,
            "period_start": start_date,
            "period_end": end_date,
            "granularity": granularity,
            "total_posts": totals[0] or 0,
            "total_views": totals[1] or 0,
            "total_forwards": totals[2] or 0,
            "avg_views": float(totals[3] or 0),
            "avg_forwards": float(totals[4] or 0),
            "activity_data": formatted_data,
            "peak_hours": peak_hours
        }

    # Message Search
    def search_messages(
        self,
        query: str,
        channel: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        page: int = 1,
        limit: int = 20,
        sort_by: SortBy = SortBy.RELEVANCE,
        sort_order: SortOrder = SortOrder.DESC
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Search messages with full-text search."""

        # Build base query
        sql_query = """
            SELECT
                fm.message_id,
                dc.channel_name,
                dd.full_date as message_date,
                fm.message_text,
                fm.view_count,
                fm.forward_count,
                fm.has_image,
                fm.extracted_products,
                ts_headline('english', fm.message_text,
                           plainto_tsquery('english', :query),
                           'StartSel=<mark>, StopSel=</mark>') as highlight
            FROM marts.fct_messages fm
            JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            WHERE fm.message_text IS NOT NULL
            AND to_tsvector('english', fm.message_text) @@ plainto_tsquery('english', :query)
        """

        params = {"query": query}

        # Add filters
        if channel:
            sql_query += " AND dc.channel_name = :channel"
            params["channel"] = channel

        if start_date:
            sql_query += " AND dd.full_date >= :start_date"
            params["start_date"] = start_date

        if end_date:
            sql_query += " AND dd.full_date <= :end_date"
            params["end_date"] = end_date

        # Add sorting
        if sort_by == SortBy.VIEWS:
            order_by = "fm.view_count"
        elif sort_by == SortBy.FORWARDS:
            order_by = "fm.forward_count"
        elif sort_by == SortBy.DATE:
            order_by = "dd.full_date"
        else:  SortBy.RELEVANCE
            # Use PostgreSQL ts_rank for relevance

            sql_query = sql_query.replace(
                "WHERE fm.message_text IS NOT NULL",
                """, ts_rank(to_tsvector('english', fm.message_text),
                           plainto_tsquery('english', :query)) as relevance
                WHERE fm.message_text IS NOT NULL"""
            )
            order_by = "relevance"

        sql_query += f" ORDER BY {order_by} {sort_order.value.upper()}"

        # Add pagination
        offset = (page - 1) * limit
        sql_query += " LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

        # Execute query
        results = self.db.execute(text(sql_query), params).fetchall()

        # Get total count
        count_query = """
            SELECT COUNT(*)
            FROM marts.fct_messages fm
            JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            WHERE fm.message_text IS NOT NULL
            AND to_tsvector('english', fm.message_text) @@ plainto_tsquery('english', :query)
        """

        if channel:
            count_query += " AND dc.channel_name = :channel"
        if start_date:
            count_query += " AND dd.full_date >= :start_date"
        if end_date:
            count_query += " AND dd.full_date <= :end_date"

        total_count = self.db.execute(
            text(count_query),
            {k: v for k, v in params.items() if k in ['query', 'channel', 'start_date', 'end_date']}
        ).scalar()

        # Format results
        formatted_results = []
        for row in results:
            formatted_results.append({
                "message_id": row[0],
                "channel_name": row[1],
                "message_date": row[2],
                "message_text": row[3],
                "view_count": row[4],
                "forward_count": row[5],
                "has_image": row[6],
                "extracted_products": row[7] or [],
                "highlight": row[8]
            })

        return formatted_results, total_count

    # Visual Content Analysis
    @cached("visual_content", ttl=600)
    def get_visual_content_stats(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        channel: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get visual content statistics."""

        if not start_date:
            start_date = date.today() - timedelta(days=30)
        if not end_date:
            end_date = date.today()

        # Base query for channel stats
        stats_query = self.db.query(
            DimChannel.channel_name,
            func.count(FactMessage.message_id).label('total_posts'),
            func.sum(case((FactMessage.has_image == True, 1), else_=0)).label('posts_with_images'),
            func.avg(FactImageDetection.confidence_score).label('avg_confidence')
        ).outerjoin(
            FactMessage, DimChannel.channel_key == FactMessage.channel_key
        ).outerjoin(
            FactImageDetection, FactMessage.message_id == FactImageDetection.message_id
        ).join(
            DimDate, FactMessage.date_key == DimDate.date_key
        ).filter(
            DimDate.full_date >= start_date,
            DimDate.full_date <= end_date
        )

        if channel:
            stats_query = stats_query.filter(DimChannel.channel_name == channel)

        stats_query = stats_query.group_by(DimChannel.channel_name)

        channel_stats = []
        total_analyzed_images = 0

        for row in stats_query.all():
            channel_name, total_posts, posts_with_images, avg_confidence = row

            # Get category distribution
            category_query = self.db.query(
                FactImageDetection.image_category,
                func.count(FactImageDetection.detection_id).label('count')
            ).join(
                FactMessage, FactImageDetection.message_id == FactMessage.message_id
            ).join(
                DimChannel, FactMessage.channel_key == DimChannel.channel_key
            ).join(
                DimDate, FactMessage.date_key == DimDate.date_key
            ).filter(
                DimChannel.channel_name == channel_name,
                DimDate.full_date >= start_date,
                DimDate.full_date <= end_date,
                FactImageDetection.image_category.isnot(None)
            ).group_by(FactImageDetection.image_category)

            category_dist = {row[0]: row[1] for row in category_query.all()}

            # Get top detected objects
            objects_query = text("""
                SELECT obj->>'name' as object_name, COUNT(*) as count
                FROM marts.fct_image_detections,
                     jsonb_array_elements(detected_objects) as obj
                JOIN marts.fct_messages ON fct_image_detections.message_id = fct_messages.message_id
                JOIN marts.dim_channels ON fct_messages.channel_key = dim_channels.channel_key
                JOIN marts.dim_dates ON fct_messages.date_key = dim_dates.date_key
                WHERE dim_channels.channel_name = :channel_name
                AND dim_dates.full_date >= :start_date
                AND dim_dates.full_date <= :end_date
                GROUP BY object_name
                ORDER BY count DESC
                LIMIT 5
            """)

            top_objects = self.db.execute(
                objects_query,
                {
                    "channel_name": channel_name,
                    "start_date": start_date,
                    "end_date": end_date
                }
            ).fetchall()

            total_analyzed_images += posts_with_images or 0

            channel_stats.append({
                "channel_name": channel_name,
                "total_posts": total_posts or 0,
                "posts_with_images": posts_with_images or 0,
                "image_percentage": (posts_with_images / total_posts * 100) if total_posts else 0,
                "category_distribution": category_dist,
                "avg_confidence": float(avg_confidence or 0),
                "top_objects": [
                    {"name": obj[0], "count": obj[1]}
                    for obj in top_objects
                ]
            })

        # Overall statistics
        overall_query = self.db.query(
            func.count(FactImageDetection.detection_id).label('total_detections'),
            func.avg(FactImageDetection.confidence_score).label('avg_confidence'),
            func.mode().within_group(asc(FactImageDetection.image_category)).label('most_common_category')
        ).join(
            FactMessage, FactImageDetection.message_id == FactMessage.message_id
        ).join(
            DimDate, FactMessage.date_key == DimDate.date_key
        ).filter(
            DimDate.full_date >= start_date,
            DimDate.full_date <= end_date
        ).first()

        overall_stats = {
            "total_analyzed_images": total_analyzed_images,
            "total_detections": overall_query[0] or 0,
            "avg_confidence": float(overall_query[1] or 0),
            "most_common_category": overall_query[2] or "N/A"
        }

        return {
            "period_start": start_date,
            "period_end": end_date,
            "channel_filter": channel,
            "total_analyzed_images": total_analyzed_images,
            "channel_stats": channel_stats,
            "overall_stats": overall_stats
        }

    # Analytics Dashboard
    @cached("analytics_dashboard", ttl=300)
    def get_analytics_dashboard(self) -> Dict[str, Any]:
        """Get comprehensive analytics dashboard."""

        # Overall statistics
        overall_stats = self.db.query(
            func.count(FactMessage.message_id).label('total_messages'),
            func.count(DimChannel.channel_key.distinct()).label('total_channels'),
            func.sum(FactMessage.view_count).label('total_views'),
            func.sum(FactMessage.forward_count).label('total_forwards'),
            func.avg(FactMessage.view_count).label('avg_views_per_message')
        ).join(DimChannel).first()

        # Channel performance ranking
        channel_perf = self.db.query(
            DimChannel.channel_name,
            DimChannel.channel_type,
            func.count(FactMessage.message_id).label('message_count'),
            func.sum(FactMessage.view_count).label('total_views'),
            func.avg(FactMessage.view_count).label('avg_views'),
            func.sum(FactMessage.forward_count).label('total_forwards'),
            (func.sum(FactMessage.view_count) / func.count(FactMessage.message_id)).label('engagement_rate')
        ).join(FactMessage).group_by(
            DimChannel.channel_name, DimChannel.channel_type
        ).order_by(desc('engagement_rate')).limit(10).all()

        # Trending products (last 7 days)
        week_ago = datetime.utcnow() - timedelta(days=7)

        trending_products = self.get_top_products(
            limit=5,
            timeframe='week',
            min_mentions=3
        )

        # Visual content trends
        visual_trends = self.db.query(
            func.date_trunc('day', DimDate.full_date).label('date'),
            func.count(FactImageDetection.detection_id).label('image_count'),
            func.avg(FactImageDetection.confidence_score).label('avg_confidence')
        ).join(FactMessage).join(DimDate).filter(
            DimDate.full_date >= week_ago
        ).group_by(func.date_trunc('day', DimDate.full_date)).order_by(
            func.date_trunc('day', DimDate.full_date)
        ).all()

        # Generate recommendations
        recommendations = []

        # Check for channels with low engagement
        low_engagement = self.db.query(DimChannel.channel_name).filter(
            DimChannel.avg_views < 100
        ).limit(3).all()

        if low_engagement:
            recommendations.append(
                f"Consider improving content strategy for channels: "
                f"{', '.join([ch[0] for ch in low_engagement])}"
            )

        # Check for image usage
        image_stats = self.db.query(
            func.avg(case((FactMessage.has_image == True, 1), else_=0)).label('image_ratio')
        ).scalar()

        if image_stats and image_stats < 0.3:
            recommendations.append(
                "Increase visual content: Only {:.1%} of posts contain images. "
                "Posts with images typically get 30% more engagement.".format(image_stats)
            )

        return {
            "timestamp": datetime.utcnow(),
            "overall_stats": {
                "total_messages": overall_stats[0] or 0,
                "total_channels": overall_stats[1] or 0,
                "total_views": overall_stats[2] or 0,
                "total_forwards": overall_stats[3] or 0,
                "avg_views_per_message": float(overall_stats[4] or 0)
            },
            "channel_performance": [
                {
                    "channel_name": row[0],
                    "channel_type": row[1],
                    "message_count": row[2],
                    "total_views": row[3],
                    "avg_views": float(row[4] or 0),
                    "total_forwards": row[5],
                    "engagement_rate": float(row[6] or 0)
                }
                for row in channel_perf
            ],
            "trending_products": trending_products,
            "visual_content_trends": [
                {
                    "date": row[0].date(),
                    "image_count": row[1],
                    "avg_confidence": float(row[2] or 0)
                }
                for row in visual_trends
            ],
            "recommendations": recommendations[:5]  # Top 5 recommendations
        }
