from typing import Generator, Optional
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.pool import QueuePool
import redis

from .core.config import settings
from .core.logger import logger


# Create SQLAlchemy engine with connection pooling
engine = create_engine(
    settings.DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=30,
    pool_pre_ping=True,
    echo=settings.DEBUG,
    connect_args={
        "connect_timeout": 10,
        "application_name": "medical_warehouse_api"
    }
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create base class for models
Base: DeclarativeMeta = declarative_base()


# Redis client for caching
def get_redis_client() -> redis.Redis:
    """Get Redis client."""
    try:
        client = redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_keepalive=True
        )
        # Test connection
        client.ping()
        logger.info("Redis connection established")
        return client
    except redis.ConnectionError as e:
        logger.error(f"Redis connection failed: {e}")
        # Return None or implement fallback strategy
        return None


redis_client = get_redis_client()


def get_db() -> Generator[Session, None, None]:
    """Get database session."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Database session error: {e}")
        raise
    finally:
        db.close()


@contextmanager
def db_session():
    """Context manager for database sessions."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()


def check_database_connection() -> bool:
    """Check if database is accessible."""
    try:
        with db_session() as db:
            db.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"Database connection check failed: {e}")
        return False
