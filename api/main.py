from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
import time
from contextlib import asynccontextmanager

from .core.config import settings
from .core.logger import logger
from .database import check_database_connection, get_redis_client
from .routers import reports, channels, search, analytics
from .schemas import HealthCheck, ErrorResponse
from .middleware.rate_limit import RateLimitMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown events."""
    # Startup
    startup_time = time.time()
    logger.info("Starting Medical Telegram Warehouse API")

    # Check dependencies
    db_healthy = check_database_connection()
    redis_healthy = get_redis_client() is not None

    if not db_healthy:
        logger.critical("Database connection failed on startup")

    logger.info(f"Startup completed in {time.time() - startup_time:.2f}s")
    logger.info(f"Database: {'✓' if db_healthy else '✗'}")
    logger.info(f"Redis: {'✓' if redis_healthy else '✗'}")

    yield

    # Shutdown
    logger.info("Shutting down Medical Telegram Warehouse API")


# Create FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="""
    Medical Telegram Warehouse API provides analytical insights from Telegram medical channels.

    ## Features

    * **Top Products Analysis**: Find most mentioned medical products
    * **Channel Analytics**: Track posting activity and engagement
    * **Message Search**: Search through millions of messages
    * **Visual Content Analysis**: Insights from image detection
    * **Predictive Analytics**: Engagement predictions and trends

    ## Authentication

    This API uses API key authentication for production.
    """,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)


# Custom OpenAPI schema
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )

    # Add security scheme
    openapi_schema["components"]["securitySchemes"] = {
        "APIKeyHeader": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "API key for authentication"
        }
    }

    # Add tags metadata
    openapi_schema["tags"] = [
        {
            "name": "reports",
            "description": "Analytical reports and insights"
        },
        {
            "name": "channels",
            "description": "Channel-specific analytics"
        },
        {
            "name": "search",
            "description": "Search messages and products"
        },
        {
            "name": "advanced-analytics",
            "description": "Advanced analytics and predictions"
        },
        {
            "name": "system",
            "description": "System health and monitoring"
        }
    ]

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count", "X-Page", "X-Per-Page"]
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"]  # Configure properly for production
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

if settings.RATE_LIMIT_PER_MINUTE > 0:
    app.add_middleware(
        RateLimitMiddleware,
        limit=settings.RATE_LIMIT_PER_MINUTE,
        window=60
    )


# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    logger.warning(f"HTTP Exception: {exc.detail}", status_code=exc.status_code)
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            detail=exc.detail,
            error_code=exc.__class__.__name__
        ).dict()
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            detail="Internal server error",
            error_code="InternalServerError"
        ).dict()
    )


# Include routers
app.include_router(reports.router)
app.include_router(channels.router)
app.include_router(search.router)
app.include_router(analytics.router)


# Root endpoints
@app.get(
    "/",
    summary="API Root",
    description="Welcome to Medical Telegram Warehouse API",
    response_model=dict
)
async def root():
    """API root endpoint."""
    return {
        "name": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "description": "Analytical API for Telegram Medical Channels",
        "docs": "/docs",
        "health": "/health"
    }


@app.get(
    "/health",
    summary="Health Check",
    description="Check API health and dependencies",
    response_model=HealthCheck,
    tags=["system"]
)
async def health_check():
    """Health check endpoint."""
    db_healthy = check_database_connection()
    redis_healthy = get_redis_client() is not None

    overall_status = "healthy" if db_healthy else "degraded"

    return HealthCheck(
        status=overall_status,
        timestamp=datetime.utcnow(),
        database=db_healthy,
        redis=redis_healthy,
        version=settings.VERSION
    )


@app.get(
    "/metrics",
    summary="API Metrics",
    description="Get API usage metrics",
    tags=["system"]
)
async def get_metrics():
    """Get API metrics."""
    # This would integrate with Prometheus in production
    return {
        "timestamp": datetime.utcnow(),
        "endpoints": [
            {"path": "/api/v1/reports/top-products", "calls_today": 0},
            {"path": "/api/v1/channels/{channel}/activity", "calls_today": 0},
            {"path": "/api/v1/search/messages", "calls_today": 0}
        ],
        "performance": {
            "avg_response_time": 0.15,
            "error_rate": 0.01,
            "requests_per_minute": 10
        }
    }


@app.get(
    "/api/v1/status",
    summary="API Status",
    description="Get detailed API status",
    tags=["system"]
)
async def get_status():
    """Get detailed API status."""
    from sqlalchemy import text

    # Check database tables
    tables = []
    try:
        from .database import engine
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema IN ('raw', 'staging', 'marts')
                ORDER BY table_schema, table_name
            """))
            tables = [{"schema": row[0], "name": row[1]} for row in result]
    except Exception as e:
        logger.error(f"Error checking database tables: {e}")

    # Check dbt models
    dbt_status = "unknown"
    try:
        import subprocess
        result = subprocess.run(
            ["dbt", "test", "--project-dir", "medical_warehouse"],
            capture_output=True,
            text=True
        )
        dbt_status = "healthy" if result.returncode == 0 else "unhealthy"
    except Exception:
        dbt_status = "not_available"

    return {
        "timestamp": datetime.utcnow(),
        "api_version": settings.VERSION,
        "database": {
            "connected": check_database_connection(),
            "tables_found": len(tables),
            "schemas": list(set(t["schema"] for t in tables))
        },
        "cache": {
            "redis_available": get_redis_client() is not None
        },
        "dbt": {
            "status": dbt_status
        },
        "system": {
            "python_version": "3.11.0",
            "environment": "development" if settings.DEBUG else "production"
        }
    }


# Run the application
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level="info",
        access_log=True
    )
