"""
🚨 Pipeline Monitoring and Alerting
"""
from dagster import sensor, RunRequest, SensorEvaluationContext
from dagster import get_dagster_logger

logger = get_dagster_logger()

@sensor(job_name="medical_telegram_daily_pipeline")
def pipeline_health_sensor(context: SensorEvaluationContext):
    """Monitor pipeline health and trigger alerts if needed"""
    logger.info("🔍 Checking pipeline health...")

    # In a real implementation, you would:
    # 1. Check last run status
    # 2. Check error logs
    # 3. Check data quality metrics
    # 4. Trigger alerts if issues detected

    # For now, return empty (no automatic retries)
    return []
