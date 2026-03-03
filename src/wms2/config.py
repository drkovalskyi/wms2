from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "WMS2_"}

    # Database
    database_url: str = "postgresql+asyncpg://wms2:wms2dev@localhost:5432/wms2"
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

    # CRIC
    cric_url: str = "https://cms-cric.cern.ch/"
    cric_sync_interval: int = 3600  # seconds between syncs; 0 = disabled

    # External services
    reqmgr2_url: str = "https://cmsweb.cern.ch/reqmgr2"
    configcache_url: str = "https://cmsweb.cern.ch/couchdb/reqmgr_config_cache"
    dbs_url: str = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
    rucio_url: str = "https://cms-rucio.cern.ch"
    rucio_account: str = "wms2"
    rucio_home: str = "/tmp/rucio"

    # Agent identity
    agent_name: str = "wms2-agent"

    # HTCondor
    condor_host: str = ""  # collector address (e.g. "localhost:9618")
    schedd_name: str | None = None  # explicit schedd; auto-discovered if None

    # DAG submission
    submit_base_dir: str = "/mnt/shared/tmp/wms2"
    target_merged_size_kb: int = 4 * 1024 * 1024  # 4 GB in KB

    # Pileup site filtering — restrict pileup file list to replicas at these RSEs.
    # When non-empty, only files with a disk replica at one of these RSEs are included.
    # When empty (default), any disk RSE is accepted (current behavior).
    # Example: "T2_CH_CERN,T1_US_FNAL_Disk"
    pileup_preferred_rses: str = ""

    # Work unit sizing
    jobs_per_work_unit: int = 8  # processing jobs per merge group
    first_round_work_units: int = 1  # work units for round 0 (pilot)
    work_units_per_round: int = 10  # work units per production round (adaptive, round 1+)
    max_jobs_per_work_unit: int = 100  # upper cap for output-size-based WU sizing

    # Job executables (override to /bin/true for local testing)
    processing_executable: str = "run_payload.sh"
    merge_executable: str = "run_merge.sh"
    cleanup_executable: str = "run_cleanup.sh"

    # Input file limit (0 = no limit, >0 = cap DBS file query)
    max_input_files: int = 0

    # X.509 certificates
    cert_file: str | None = None
    key_file: str | None = None
    ssl_ca_path: str = "/etc/grid-security/certificates"

    # Output staging — LFN→PFN mapping: PFN = local_pfn_prefix + LFN
    local_pfn_prefix: str = "/mnt/shared"

    # Pilot profiling (optional functional test)
    pilot_initial_events: int = 200
    pilot_step_timeout: int = 900       # 15 minutes per step

    # Error Handler
    error_hold_threshold: float = 0.20
    error_max_rescue_attempts: int = 3

    # Site Banning
    site_ban_duration_days: int = 7
    site_ban_promotion_threshold: int = 3
    site_ban_min_failures: int = 3
    site_ban_failure_ratio: float = 0.50

    # Priority
    default_pilot_priority: int = 5  # HTCondor job priority for round 0 (pilot)

    # Adaptive execution
    default_memory_per_core: int = 2000    # MB, floor for request_memory
    max_memory_per_core: int = 3000        # MB, ceiling for request_memory
    safety_margin: float = 0.20            # fractional margin on measured memory
    adaptive_mode: str = "per_step"        # per_step | job_split | pipeline_split

    # API
    api_prefix: str = "/api/v1"

    # Logging
    log_level: str = "INFO"
    debug: bool = False
