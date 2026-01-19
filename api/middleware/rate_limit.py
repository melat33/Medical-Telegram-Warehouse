import time
from typing import Dict, Tuple
from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from ..core.logger import logger
from ..services.cache_service import CacheService


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limiting middleware."""

    def __init__(self, app, limit: int = 60, window: int = 60):
        super().__init__(app)
        self.limit = limit
        self.window = window
        self.cache = CacheService()

    async def dispatch(self, request: Request, call_next):
        # Skip rate limiting for health checks
        if request.url.path in ["/health", "/metrics", "/"]:
            return await call_next(request)

        # Get client identifier
        client_ip = request.client.host
        endpoint = request.url.path

        # Generate rate limit key
        key = f"rate_limit:{client_ip}:{endpoint}"

        # Get current count
        current_time = int(time.time())
        window_key = current_time // self.window

        rate_key = f"{key}:{window_key}"

        # Check rate limit
        if self.cache.is_available():
            request_count = int(self.cache.get(rate_key) or 0)

            if request_count >= self.limit:
                logger.warning(f"Rate limit exceeded for {client_ip} on {endpoint}")
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Rate limit exceeded",
                    headers={
                        "X-RateLimit-Limit": str(self.limit),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str((window_key + 1) * self.window)
                    }
                )

            # Increment count
            self.cache.set(rate_key, request_count + 1, ttl=self.window * 2)

            # Add rate limit headers
            response = await call_next(request)
            response.headers["X-RateLimit-Limit"] = str(self.limit)
            response.headers["X-RateLimit-Remaining"] = str(self.limit - request_count - 1)
            response.headers["X-RateLimit-Reset"] = str((window_key + 1) * self.window)

            return response

        # If cache is not available, skip rate limiting
        logger.warning("Redis not available, skipping rate limiting")
        return await call_next(request)
