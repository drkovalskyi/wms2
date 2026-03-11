import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {
        "env_prefix": "WMS2_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }

    # Database
    database_url: str = ""  # Set via WMS2_DATABASE_URL env var or .env file
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
    consolidation_rse: str = ""  # target RSE for output consolidation (e.g. "T2_CH_CERN_Disk")

    # Agent identity
    agent_name: str = "wms2-agent"

    # User identity (for test/commissioning mode)
    accounting_group_user: str = ""     # HTCondor accounting_group_user
    test_lfn_user: str = ""            # LFN path component (e.g. "username.wms2")
    rucio_test_account: str = ""       # Rucio account for test mode

    # HTCondor
    condor_host: str = ""  # collector address (e.g. "localhost:9618")
    schedd_name: str | None = None  # explicit schedd; auto-discovered if None
    sec_token_directory: str = ""  # IDTOKEN dir for remote schedd auth
    extra_collectors: str = ""    # additional collector(s) for multi-pool, comma-separated

    # Remote schedd for global pool submission
    remote_schedd: str = ""         # e.g. "vocms047.cern.ch"

    # Remote schedd spool access (for monitoring DAGs on a remote schedd)
    # spool_mount: local sshfs mount of remote schedd's spool dir (read-only)
    # remote_spool_prefix: schedd's SPOOL path (for path translation)
    # When both are set, submit uses spool mode and monitoring reads via mount.
    spool_mount: str = ""           # e.g. "/mnt/remote_spool"
    remote_spool_prefix: str = ""   # e.g. "/data/srv/glidecondor/condor_local/spool"

    # Site whitelist — restrict job execution to these CMS sites.
    # Comma-separated. When non-empty, all jobs get a Requirements expression
    # allowing only listed sites. When empty (default), no restriction.
    # Example: "T2_CH_CERN"
    allowed_sites: str = ""

    # DAG submission
    submit_base_dir: str = "/mnt/shared/tmp/wms2"
    target_merged_size_kb: int = 4 * 1024 * 1024  # 4 GB in KB

    # Pileup site filtering — restrict pileup file list to replicas at these RSEs.
    # When non-empty, only files with a disk replica at one of these RSEs are included.
    # When empty (default), any disk RSE is accepted (current behavior).
    # Example: "T2_CH_CERN,T1_US_FNAL_Disk"
    pileup_preferred_rses: str = ""

    # Pileup Rucio query timeout (seconds). Large PREMIX datasets can take
    # minutes to enumerate via list_replicas.
    pileup_query_timeout: int = 300

    # Pileup remote read — when True (default), pileup file LFNs are prefixed
    # with root://cms-xrd-global.cern.ch/ so CMSSW reads them via the global
    # redirector (AAA).  When False, LFNs are left bare and resolved through
    # the site's trivialcatalog (storage.xml), requiring local replicas.
    pileup_remote_read: bool = True

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

    # X.509 proxy (for CMS grid services). Reads from X509_USER_PROXY if not set.
    x509_proxy: str | None = os.environ.get("X509_USER_PROXY")
    ssl_ca_path: str = "/etc/grid-security/certificates"

    # Output staging — LFN→PFN mapping
    # stageout_mode: "local" = filesystem copy (PFN = local_pfn_prefix + LFN)
    #                "test"  = grid stageout to /store/temp/, _Temp RSEs, user Rucio scope
    #                "production" = grid stageout to /store/, real RSEs, cms Rucio scope
    stageout_mode: str = "local"
    local_pfn_prefix: str = "/mnt/shared"

    # Pilot (round 0) — controls round 0 behavior
    # pilot_fraction: fraction of events_per_job for round 0 (0 = skip pilot, round 0 is normal)
    pilot_fraction: float = 0.01
    # pilot_throwaway: discard round 0 output after metrics extraction (events not counted)
    pilot_throwaway: bool = False
    # Legacy pilot runner settings (used by submit_pilot / _compute_pilot_config)
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
    merge_priority_boost: int = 10   # merge jobs get this much higher priority than proc

    # Tail WU escalation — bump remaining job priorities when most WUs are done
    tail_escalation_threshold: float = 0.9   # fraction of WUs done before escalating
    tail_escalation_priority: int = 10       # priority boost for remaining jobs

    # Wall time enforcement (periodic_remove)
    wall_time_safety_factor: float = 3.0     # multiplier on estimated wall time
    landing_wall_time_mins: int = 30         # fixed wall time for landing nodes
    merge_wall_time_mins_min: int = 240      # minimum wall time for merge nodes
    cleanup_wall_time_mins: int = 60         # fixed wall time for cleanup nodes

    # Periodic remove thresholds
    periodic_remove_held_timeout_sec: int = 420     # 7 min — kill jobs stuck in held
    periodic_remove_disk_limit_kb: int = 20971520   # 20 GB

    # Idle timeout tiers (seconds) — selected by job priority level
    idle_timeout_high_sec: int = 14400       # 4h for high-priority jobs
    idle_timeout_normal_sec: int = 43200     # 12h for normal-priority jobs
    idle_timeout_low_sec: int = 172800       # 48h for low-priority jobs

    # Adaptive execution
    default_memory_per_core: int = 2000    # MB, floor for request_memory
    max_memory_per_core: int = 3000        # MB, ceiling for request_memory
    safety_margin: float = 0.20            # fractional margin on measured memory
    min_request_cpus: int = 4              # floor for job splitting (avoids pool fragmentation)

    # API
    api_prefix: str = "/api/v1"

    # Logging
    log_level: str = "INFO"
    debug: bool = False
