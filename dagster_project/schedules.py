"""
📅 Pipeline Scheduling Configuration
"""
from dagster import ScheduleDefinition
from .pipeline import medical_telegram_daily_pipeline

# Production schedule - runs daily at 2 AM Ethiopian time
daily_production_schedule = ScheduleDefinition(
    job=medical_telegram_daily_pipeline,
    cron_schedule="0 2 * * *",  # 2 AM daily
    execution_timezone="Africa/Addis_Ababa",
    description="Daily data refresh for Ethiopian medical market analysis"
)

# Development schedule - runs every hour
hourly_dev_schedule = ScheduleDefinition(
    job=medical_telegram_daily_pipeline,
    cron_schedule="0 * * * *",  # Every hour
    description="Hourly development schedule for testing"
)

# Quick test schedule - runs every 15 minutes
test_schedule = ScheduleDefinition(
    job=medical_telegram_daily_pipeline,
    cron_schedule="*/15 * * * *",  # Every 15 minutes
    description="Frequent testing during development"
)
