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

    # External services
    reqmgr2_url: str = "https://cmsweb.cern.ch/reqmgr2"
    dbs_url: str = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
    rucio_url: str = "https://cms-rucio.cern.ch"
    rucio_account: str = "�wms2"

    # Agent identity
    agent_name: str = "wms2-agent"

    # HTCondor
    condor_host: str = ""  # collector address (e.g. "localhost:9618")
    schedd_name: str | None = None  # explicit schedd; auto-discovered if None

    # DAG submission
    submit_base_dir: str = "/data/wms2/submit"
    target_merged_size_kb: int = 4 * 1024 * 1024  # 4 GB in KB

    # X.509 certificates
    cert_file: str | None = None
    key_file: str | None = None

    # API
    api_prefix: str = "/api/v1"

    # Logging
    log_level: str = "INFO"
    debug: bool = False
