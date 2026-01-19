from typing import Generator
from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session

from .database import get_db
from .core.security import verify_token
from .core.config import settings
from .core.logger import logger


def get_query_service(db: Session = Depends(get_db)):
    """Get query service dependency."""
    from .services.query_service import QueryService
    return QueryService(db)


def require_api_key(x_api_key: str = None):
    """Require API key for protected endpoints."""
    # In production, you would validate against a database or environment variable
    if not settings.DEBUG and x_api_key != settings.SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )
    return True


def get_current_user(token_data: dict = Depends(verify_token)):
    """Get current user from token."""
    # This would validate user permissions
    return {
        "user_id": token_data.get("sub"),
        "username": token_data.get("username"),
        "permissions": token_data.get("permissions", [])
    }
