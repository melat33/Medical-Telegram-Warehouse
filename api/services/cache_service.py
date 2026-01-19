import json
from typing import Any, Optional, Callable
from functools import wraps
import hashlib
import pickle

from redis import Redis
from .core.config import settings
from .core.logger import logger


class CacheService:
    """Redis cache service."""

    def __init__(self, redis_client: Optional[Redis] = None):
        self.redis = redis_client

    def is_available(self) -> bool:
        """Check if Redis is available."""
        return self.redis is not None

    def generate_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate cache key from function arguments."""
        key_parts = [prefix]

        # Add positional arguments
        for arg in args:
            key_parts.append(str(arg))

        # Add keyword arguments
        for key, value in sorted(kwargs.items()):
            key_parts.append(f"{key}:{value}")

        key_string = ":".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if not self.is_available():
            return None

        try:
            value = self.redis.get(key)
            if value:
                return pickle.loads(value)
        except Exception as e:
            logger.error(f"Cache get error: {e}")
        return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache."""
        if not self.is_available():
            return False

        try:
            if ttl is None:
                ttl = settings.CACHE_TTL

            self.redis.setex(
                key,
                ttl,
                pickle.dumps(value)
            )
            return True
        except Exception as e:
            logger.error(f"Cache set error: {e}")
            return False

    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        if not self.is_available():
            return False

        try:
            self.redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
            return False

    def clear_pattern(self, pattern: str) -> int:
        """Clear keys matching pattern."""
        if not self.is_available():
            return 0

        try:
            keys = self.redis.keys(pattern)
            if keys:
                return self.redis.delete(*keys)
            return 0
        except Exception as e:
            logger.error(f"Cache clear error: {e}")
            return 0


def cached(prefix: str, ttl: Optional[int] = None):
    """Decorator for caching function results."""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_service = CacheService()

            # Generate cache key
            key = cache_service.generate_key(prefix, *args, **kwargs)

            # Try to get from cache
            cached_result = cache_service.get(key)
            if cached_result is not None:
                logger.debug(f"Cache hit for {func.__name__}")
                return cached_result

            # Execute function
            result = func(*args, **kwargs)

            # Store in cache
            cache_service.set(key, result, ttl)

            return result
        return wrapper
    return decorator
