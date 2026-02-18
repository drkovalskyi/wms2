"""Environment checks for each test level.

Each check function returns (ok: bool, detail: str).
These are used both by the environment test suite and by conftest.py
to gate higher-level tests.
"""

import os
import shutil
import subprocess
import sys


# ---------------------------------------------------------------------------
# Level 0 — Unit tests: Python, packages, database
# ---------------------------------------------------------------------------

def check_python_version(min_version=(3, 11)):
    """Python >= 3.11."""
    v = sys.version_info[:2]
    if v >= min_version:
        return True, f"Python {v[0]}.{v[1]}"
    return False, f"Python {v[0]}.{v[1]} < {min_version[0]}.{min_version[1]}"


def check_package_importable(package_name):
    """A Python package can be imported."""
    try:
        __import__(package_name)
        return True, f"{package_name} importable"
    except ImportError as exc:
        return False, f"{package_name}: {exc}"


LEVEL0_PACKAGES = [
    "fastapi",
    "sqlalchemy",
    "pydantic",
    "pydantic_settings",
    "httpx",
    "pytest",
    "alembic",
    "asyncpg",
]


def check_postgres_reachable(dsn="postgresql://wms2:wms2dev@localhost:5432/wms2"):
    """PostgreSQL accepts connections."""
    try:
        import psycopg2  # noqa: F811
        conn = psycopg2.connect(dsn, connect_timeout=5)
        conn.close()
        return True, "PostgreSQL reachable"
    except Exception:
        pass
    # Fallback: use pg_isready
    pg_isready = shutil.which("pg_isready")
    if pg_isready:
        r = subprocess.run(
            [pg_isready, "-h", "localhost", "-p", "5432", "-U", "wms2", "-d", "wms2"],
            capture_output=True, timeout=5,
        )
        if r.returncode == 0:
            return True, "PostgreSQL reachable (pg_isready)"
        return False, f"pg_isready exit {r.returncode}: {r.stderr.decode().strip()}"
    return False, "Cannot connect to PostgreSQL and pg_isready not found"


# ---------------------------------------------------------------------------
# Level 1 — Local real CMSSW workflow (no HTCondor)
# ---------------------------------------------------------------------------

def check_cvmfs_mounted():
    """CVMFS is mounted at /cvmfs/cms.cern.ch."""
    path = "/cvmfs/cms.cern.ch/cmsset_default.sh"
    if os.path.isfile(path):
        return True, "CVMFS mounted"
    return False, f"{path} not found"


def check_cmssw_release_available():
    """At least one CMSSW release directory exists."""
    base = "/cvmfs/cms.cern.ch"
    if not os.path.isdir(base):
        return False, "CVMFS not mounted"
    # Check for a known recent release
    for arch_dir in ("el8_amd64_gcc12", "el8_amd64_gcc11", "el9_amd64_gcc12"):
        cms_path = os.path.join(base, arch_dir, "cms", "cmssw")
        if os.path.isdir(cms_path):
            entries = os.listdir(cms_path)
            if entries:
                return True, f"{len(entries)} releases under {arch_dir}"
    return False, "No CMSSW releases found under known architectures"


def check_x509_proxy():
    """Valid X509 proxy exists and is not expired."""
    proxy_path = os.environ.get("X509_USER_PROXY", "")
    if not proxy_path:
        # Check common locations
        for candidate in ("/mnt/creds/x509up", f"/tmp/x509up_u{os.getuid()}"):
            if os.path.isfile(candidate):
                proxy_path = candidate
                break
    if not proxy_path or not os.path.isfile(proxy_path):
        return False, "No X509 proxy found (set X509_USER_PROXY)"
    # Check expiry via voms-proxy-info or openssl
    voms = shutil.which("voms-proxy-info")
    if voms:
        r = subprocess.run(
            [voms, "-file", proxy_path, "-timeleft"],
            capture_output=True, timeout=5,
        )
        if r.returncode == 0:
            timeleft = int(r.stdout.decode().strip())
            if timeleft > 0:
                hours = timeleft // 3600
                return True, f"X509 proxy valid ({hours}h remaining)"
            return False, "X509 proxy expired"
    # Fallback: file exists and is non-empty
    if os.path.getsize(proxy_path) > 0:
        return True, f"X509 proxy exists at {proxy_path} (expiry not checked)"
    return False, "X509 proxy file is empty"


def check_siteconf():
    """CMS site-local-config exists."""
    siteconf = os.environ.get("SITECONFIG_PATH", "/opt/cms/siteconf")
    cfg = os.path.join(siteconf, "JobConfig", "site-local-config.xml")
    if os.path.isfile(cfg):
        return True, f"siteconf at {siteconf}"
    return False, f"site-local-config.xml not found at {cfg}"


def check_apptainer():
    """apptainer (or singularity) is available."""
    for cmd in ("apptainer", "singularity"):
        path = shutil.which(cmd)
        if path:
            r = subprocess.run([path, "--version"], capture_output=True, timeout=5)
            ver = r.stdout.decode().strip()
            return True, f"{cmd} {ver}"
    return False, "Neither apptainer nor singularity found in PATH"


# ---------------------------------------------------------------------------
# Level 2 — Local HTCondor
# ---------------------------------------------------------------------------

def check_condor_binaries():
    """HTCondor CLI tools available."""
    missing = []
    for cmd in ("condor_status", "condor_submit", "condor_q", "condor_submit_dag"):
        if not shutil.which(cmd):
            missing.append(cmd)
    if missing:
        return False, f"Missing: {', '.join(missing)}"
    return True, "HTCondor CLI tools available"


def check_condor_schedd():
    """HTCondor schedd is reachable and accepting commands."""
    condor_status = shutil.which("condor_status")
    if not condor_status:
        return False, "condor_status not found"
    r = subprocess.run(
        [condor_status, "-schedd"],
        capture_output=True, timeout=10,
    )
    if r.returncode == 0 and r.stdout.decode().strip():
        return True, "Schedd reachable"
    return False, f"condor_status -schedd failed: {r.stderr.decode().strip()}"


def check_condor_slots():
    """HTCondor pool has available execution slots."""
    condor_status = shutil.which("condor_status")
    if not condor_status:
        return False, "condor_status not found"
    r = subprocess.run(
        [condor_status, "-total"],
        capture_output=True, timeout=10,
    )
    if r.returncode != 0:
        return False, f"condor_status -total failed: {r.stderr.decode().strip()}"
    output = r.stdout.decode()
    # Look for "Total" line with slot counts
    for line in output.splitlines():
        if "Total" in line:
            parts = line.split()
            # Format: Total  <slots> <owner> <claimed> <unclaimed> ...
            try:
                total_slots = int(parts[1])
                if total_slots > 0:
                    return True, f"{total_slots} slots in pool"
            except (IndexError, ValueError):
                pass
    return False, f"No execution slots found: {output.strip()}"


def check_condor_python_bindings():
    """HTCondor Python bindings (htcondor2) importable."""
    return check_package_importable("htcondor2")


# ---------------------------------------------------------------------------
# Level 3 — Production services
# ---------------------------------------------------------------------------

def check_reqmgr2_reachable(url="https://cmsweb.cern.ch/reqmgr2/data/info"):
    """ReqMgr2 API is reachable."""
    return _check_https_endpoint(url, "ReqMgr2")


def check_dbs_reachable(url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader/serverinfo"):
    """DBS API is reachable."""
    return _check_https_endpoint(url, "DBS")


def check_couchdb_reachable(url="https://cmsweb.cern.ch/couchdb/"):
    """CouchDB/ConfigCache is reachable."""
    return _check_https_endpoint(url, "CouchDB")


def _check_https_endpoint(url, name):
    """Check an HTTPS endpoint is reachable with X509 auth via curl.

    We use curl instead of httpx because Python's ssl module does not
    handle X509 proxy certificate chains correctly (sends only the proxy
    cert, not the full chain), resulting in HTTP 401 from CMS services.
    """
    curl = shutil.which("curl")
    if not curl:
        return False, "curl not found"
    proxy_path = os.environ.get("X509_USER_PROXY", "")
    if not proxy_path:
        for candidate in ("/mnt/creds/x509up", f"/tmp/x509up_u{os.getuid()}"):
            if os.path.isfile(candidate):
                proxy_path = candidate
                break
    ca_path = os.environ.get("WMS2_CA_BUNDLE", "")
    if not ca_path and os.path.isfile("/tmp/cern-ca-bundle.pem"):
        ca_path = "/tmp/cern-ca-bundle.pem"
    cmd = [curl, "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "10"]
    if proxy_path:
        cmd += ["--cert", proxy_path, "--key", proxy_path]
    if ca_path:
        cmd += ["--cacert", ca_path]
    cmd.append(url)
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=15)
        status = r.stdout.decode().strip()
        if r.returncode != 0:
            return False, f"{name}: curl exit {r.returncode}"
        if status.startswith("2") or status.startswith("3"):
            return True, f"{name} reachable (HTTP {status})"
        return False, f"{name} returned HTTP {status}"
    except Exception as exc:
        return False, f"{name}: {exc}"


# ---------------------------------------------------------------------------
# Aggregate runners
# ---------------------------------------------------------------------------

def run_level_checks(level):
    """Run all checks for a given level. Returns list of (name, ok, detail)."""
    checks = {
        0: [
            ("python_version", check_python_version),
            *[(f"package_{p}", lambda p=p: check_package_importable(p)) for p in LEVEL0_PACKAGES],
            ("postgres", check_postgres_reachable),
        ],
        1: [
            ("cvmfs", check_cvmfs_mounted),
            ("cmssw_releases", check_cmssw_release_available),
            ("x509_proxy", check_x509_proxy),
            ("siteconf", check_siteconf),
            ("apptainer", check_apptainer),
        ],
        2: [
            ("condor_binaries", check_condor_binaries),
            ("condor_schedd", check_condor_schedd),
            ("condor_slots", check_condor_slots),
            ("condor_python", check_condor_python_bindings),
        ],
        3: [
            ("reqmgr2", check_reqmgr2_reachable),
            ("dbs", check_dbs_reachable),
            ("couchdb", check_couchdb_reachable),
        ],
    }
    results = []
    for name, fn in checks.get(level, []):
        try:
            ok, detail = fn()
        except Exception as exc:
            ok, detail = False, str(exc)
        results.append((name, ok, detail))
    return results
