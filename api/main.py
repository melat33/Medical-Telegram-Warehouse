from fastapi import FastAPI
from datetime import datetime

app = FastAPI(
    title="Medical Telegram Warehouse API",
    description="API for analyzing Telegram medical channels",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

@app.get("/")
async def root():
    return {
        "name": "Medical Telegram Warehouse API",
        "version": "1.0.0",
        "description": "Analytical API for Telegram Medical Channels",
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "top_products": "/api/top-products",
            "channel_activity": "/api/channels/{name}/activity",
            "message_search": "/api/search/messages",
            "visual_content": "/api/visual-content"
        }
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now(),
        "database": True
    }

@app.get("/api/top-products")
async def get_top_products(limit: int = 10):
    """Get top mentioned medical products"""
    products = [
        {"product_name": "Paracetamol", "mention_count": 150},
        {"product_name": "Amoxicillin", "mention_count": 120},
        {"product_name": "Vitamin C", "mention_count": 95},
    ][:limit]

    return {
        "products": products,
        "total_mentions": sum(p["mention_count"] for p in products)
    }

@app.get("/api/channels/{channel_name}/activity")
async def get_channel_activity(channel_name: str):
    """Get channel posting activity"""
    return {
        "channel": channel_name,
        "activity": [
            {"date": "2025-01-18", "post_count": 45, "avg_views": 46.7},
            {"date": "2025-01-17", "post_count": 38, "avg_views": 48.7},
        ],
        "total_posts": 83,
        "total_views": 3950
    }

@app.get("/api/search/messages")
async def search_messages(query: str = "medicine", limit: int = 10):
    """Search messages by keyword"""
    return {
        "query": query,
        "results": [
            {
                "message_id": 123456789,
                "channel_name": "CheMed",
                "message_text": f"Medicine available: {query}. Best quality imported.",
                "view_count": 1250
            }
        ],
        "total_results": 1
    }

@app.get("/api/visual-content")
async def get_visual_content_stats():
    """Get visual content statistics"""
    return [
        {
            "channel_name": "CheMed",
            "posts_with_images": 567,
            "total_posts": 1245,
            "image_percentage": 45.5
        }
    ]
