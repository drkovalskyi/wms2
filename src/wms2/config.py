from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "WMS2_"}

    # Database
    database_url: str = "postgresql+asyncpg://wms2:wms2pass@localhost:5432/wms2"
    db_pool_size: int = 10

    # Lifecycle Manager
    lifecycle_cycle_interval: int = 60

    # Admission Controller
    max_active_dags: int = 300

    # Per-status timeout thresholds (seconds)
    timeout_submitted: int = 3600
    timeout_queued: int = 86400 * 7
    timeout_pilot_running: int = 86400
    timeout_planning: int = 3600
    timeout_active: int = 86400 * 30
    timeout_stopping: int = 3600
    timeout_resubmitting: int = 600

    # API
    api_prefix: str = "/api/v1"

    # Logging
    log_level: str = "INFO"
    debug: bool = False
