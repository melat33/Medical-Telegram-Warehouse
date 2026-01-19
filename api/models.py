from datetime import datetime
from typing import Optional, List
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float, ForeignKey, Text, BigInteger
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property

from .database import Base


class DimChannel(Base):
    """Channel dimension model."""
    __tablename__ = "dim_channels"
    __table_args__ = {"schema": "marts"}

    channel_key = Column(Integer, primary_key=True, index=True)
    channel_name = Column(String(255), unique=True, index=True, nullable=False)
    channel_type = Column(String(100), nullable=False)
    first_post_date = Column(DateTime)
    last_post_date = Column(DateTime)
    total_posts = Column(Integer, default=0)
    avg_views = Column(Float, default=0.0)
    total_views = Column(BigInteger, default=0)
    total_forwards = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    messages = relationship("FactMessage", back_populates="channel")
    image_detections = relationship("FactImageDetection", back_populates="channel")

    @hybrid_property
    def avg_engagement(self) -> float:
        """Calculate average engagement rate."""
        if self.total_posts > 0:
            return (self.total_views + self.total_forwards * 10) / self.total_posts
        return 0.0


class DimDate(Base):
    """Date dimension model."""
    __tablename__ = "dim_dates"
    __table_args__ = {"schema": "marts"}

    date_key = Column(Integer, primary_key=True, index=True)
    full_date = Column(DateTime, unique=True, index=True, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    day_name = Column(String(20), nullable=False)
    week_of_year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    month_name = Column(String(20), nullable=False)
    quarter = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    is_weekend = Column(Boolean, default=False)

    messages = relationship("FactMessage", back_populates="date")
    image_detections = relationship("FactImageDetection", back_populates="date")


class FactMessage(Base):
    """Message fact model."""
    __tablename__ = "fct_messages"
    __table_args__ = {"schema": "marts"}

    message_id = Column(BigInteger, primary_key=True, index=True)
    channel_key = Column(Integer, ForeignKey("marts.dim_channels.channel_key"), index=True, nullable=False)
    date_key = Column(Integer, ForeignKey("marts.dim_dates.date_key"), index=True, nullable=False)
    message_text = Column(Text)
    message_length = Column(Integer, default=0)
    view_count = Column(Integer, default=0)
    forward_count = Column(Integer, default=0)
    has_image = Column(Boolean, default=False)
    extracted_products = Column(JSONB)  # Store extracted products as JSON
    sentiment_score = Column(Float)  # Optional: for sentiment analysis
    created_at = Column(DateTime, default=datetime.utcnow)

    channel = relationship("DimChannel", back_populates="messages")
    date = relationship("DimDate", back_populates="messages")
    image_detections = relationship("FactImageDetection", back_populates="message")


class FactImageDetection(Base):
    """Image detection fact model."""
    __tablename__ = "fct_image_detections"
    __table_args__ = {"schema": "marts"}

    detection_id = Column(Integer, primary_key=True, index=True)
    message_id = Column(BigInteger, ForeignKey("marts.fct_messages.message_id"), index=True, nullable=False)
    channel_key = Column(Integer, ForeignKey("marts.dim_channels.channel_key"), index=True, nullable=False)
    date_key = Column(Integer, ForeignKey("marts.dim_dates.date_key"), index=True, nullable=False)
    detected_objects = Column(JSONB)  # Store all detected objects
    confidence_score = Column(Float)
    image_category = Column(String(50), index=True)  # promotional, product_display, etc.
    analysis_version = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)

    message = relationship("FactMessage", back_populates="image_detections")
    channel = relationship("DimChannel", back_populates="image_detections")
    date = relationship("DimDate", back_populates="image_detections")
