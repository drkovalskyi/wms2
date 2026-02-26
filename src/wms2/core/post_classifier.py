"""POST script error classification and collector script generation.

Provides:
- classify_error() — testable error classification logic
- generate_collector_script() — returns self-contained Python script string
  that the DAG Planner writes to {submit_dir}/wms2_post_collect.py

The collector script runs on the submit host after each job attempt, parses
FJR XML and HTCondor logs, classifies errors, and writes a JSON side file
({node_name}.post.json) for Level 2 consumption by the Error Handler.

Error taxonomy derived from WMAgent's WMExceptions.py + DefaultConfig.py,
adapted for WMS2's two-level error handling model (docs/error_handling.md).
"""

from __future__ import annotations

# ── Permanent (UNLESS-EXIT 42): won't fix on retry ──────────────
PERMANENT_CODES: set[int] = {
    # CMSSW config/software errors
    70,     # Invalid cmsRun arguments
    73,     # Failed to get Scram runtime
    7000,   # Exception from command line processing
    7001,   # Configuration file not found
    7002,   # Configuration file read error
    8001,   # Other CMS Exception (generic permanent)
    8006,   # ProductNotFound
    8009,   # Configuration error
    8023,   # MismatchedInputFiles
    8026,   # NotFound (generic missing dependency)
    8501,   # EventGenerationFailure
    50110,  # Executable not found
    50113,  # Executable bad arguments
    # Wallclock kills (job inherently too slow)
    50664,  # Killed by wrapper: too much wall clock
    # Scheduling impossibility
    71102,  # Can only run at Aborted site
    71104,  # Could not load job pickle
    71105,  # Empty job pickle
}

# ── Data errors (UNLESS-EXIT 42): bad input, exclude file ───────
DATA_CODES: set[int] = {
    8016,   # EventCorruption
    8021,   # FileReadError
    8027,   # FormatIncompatibility
    8028,   # FileOpenError with fallback
    8034,   # FileNameInconsistentWithGUID
}

# ── Infrastructure (retryable with cooloff): site/resource issues ─
INFRASTRUCTURE_CODES: set[int] = {
    # Memory kills — retryable with resource adjustment
    50660,  # Killed: too much RAM (PSS) → bump request_memory
    50661,  # Killed: too much VSize → bump request_memory
    8004,   # std::bad_alloc (memory exhaustion)
    8030,   # Exceeded max VSize
    8031,   # Exceeded max RSS
    # Site environment issues
    50,     # Required app version not found
    71,     # Failed to initiate Scram project
    72,     # Failed to enter Scram project dir
    74,     # Unable to untar sandbox
    81,     # No functioning CMSSW on worker node
    8020,   # FileOpenError (likely site error)
    8033,   # Could not write output file (disk)
    8035,   # UnavailableAccelerator
    # Site environment setup (10xxx range)
    10003,  # cmsset_default.sh failed
    10004,  # CMS_PATH not defined
    10005,  # CMS_PATH dir does not exist
    10006,  # scramv1 not found
    10007,  # CMSSW files corrupted
    10008,  # Scratch dir not found
    10009,  # Less than 5GB free
    10018,  # site-local-config.xml not found
    10032,  # Failed to source CMS environment
    10042,  # Unable to stage-in wrapper tarball
    10043,  # Unable to bootstrap WMCore libraries
    # Stageout failures — site storage issues
    60311,  # Local stageout failure (site plugin)
    60315,  # Stageout init error (TFC, SITECONF)
    60316,  # Failed to create directory on SE
    60321,  # Site issue: no space, SE down
}

# Memory-specific subset (trigger request_memory bump)
MEMORY_CODES: set[int] = {50660, 50661, 8004, 8030, 8031}


def classify_error(
    cmssw_exit_code: int,
    job_exit_code: int,
    error_message: str = "",
) -> dict:
    """Classify an error into category + retryable + action.

    Uses CMSSW exit code as primary signal, job exit code as fallback.

    Returns:
        {
            "category": str,        # "permanent", "data", "infrastructure",
                                    # "infrastructure_memory", "transient", "success"
            "retryable": bool,
            "bad_input_files": list, # populated only for "data" category
            "action": str,          # "permanent_failure", "retry", "retry_with_cooloff"
            "memory_exceeded": bool,
        }
    """
    if cmssw_exit_code == 0 and job_exit_code == 0:
        return {
            "category": "success",
            "retryable": False,
            "bad_input_files": [],
            "action": "none",
            "memory_exceeded": False,
        }

    # Use whichever code is non-zero; prefer CMSSW code if both are set
    code = cmssw_exit_code if cmssw_exit_code != 0 else job_exit_code

    if code in PERMANENT_CODES:
        return {
            "category": "permanent",
            "retryable": False,
            "bad_input_files": [],
            "action": "permanent_failure",
            "memory_exceeded": False,
        }

    if code in DATA_CODES:
        return {
            "category": "data",
            "retryable": False,
            "bad_input_files": [],  # filled by collector from FJR
            "action": "permanent_failure",
            "memory_exceeded": False,
        }

    if code in MEMORY_CODES:
        return {
            "category": "infrastructure_memory",
            "retryable": True,
            "bad_input_files": [],
            "action": "retry_with_cooloff",
            "memory_exceeded": True,
        }

    if code in INFRASTRUCTURE_CODES:
        return {
            "category": "infrastructure",
            "retryable": True,
            "bad_input_files": [],
            "action": "retry_with_cooloff",
            "memory_exceeded": False,
        }

    # Default: transient (unknown codes, signals, stageout timeouts)
    return {
        "category": "transient",
        "retryable": True,
        "bad_input_files": [],
        "action": "retry",
        "memory_exceeded": False,
    }


def generate_collector_script() -> str:
    """Return self-contained Python script (wms2_post_collect.py).

    The script:
    1. Parses FJR XML for CMSSW exit code, per-step status, input/output files,
       performance metrics
    2. Reads .err file tail (last 200 lines) for log_tail
    3. Reads HTCondor .log file for site (MATCH_GLIDEIN_CMSSite), wall time, memory
    4. Classifies the error (classification logic embedded inline)
    5. Writes {node_name}.post.json to the group directory
    6. Prints the classification category to stdout (consumed by post_script.sh)

    Called as: python3 wms2_post_collect.py <node> <exit_code> <retry> <max_retries>
    """
    return r'''#!/usr/bin/env python3
"""wms2_post_collect.py — POST script data collector (self-contained).

Called by post_script.sh after each job attempt. Parses FJR, HTCondor logs,
classifies errors, and writes {node_name}.post.json.

Usage: python3 wms2_post_collect.py <node_name> <exit_code> <retry_num> <max_retries>
"""

import glob
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

# ── Error classification tables (embedded from post_classifier.py) ────

PERMANENT_CODES = {
    70, 73, 7000, 7001, 7002, 8001, 8006, 8009, 8023, 8026, 8501,
    50110, 50113, 50664, 71102, 71104, 71105,
}
DATA_CODES = {8016, 8021, 8027, 8028, 8034}
INFRASTRUCTURE_CODES = {
    50660, 50661, 8004, 8030, 8031,
    50, 71, 72, 74, 81, 8020, 8033, 8035,
    10003, 10004, 10005, 10006, 10007, 10008, 10009, 10018, 10032, 10042, 10043,
    60311, 60315, 60316, 60321,
}
MEMORY_CODES = {50660, 50661, 8004, 8030, 8031}


def classify_error(cmssw_exit_code, job_exit_code, error_message=""):
    """Classify an error into category + retryable + action."""
    if cmssw_exit_code == 0 and job_exit_code == 0:
        return {
            "category": "success", "retryable": False,
            "bad_input_files": [], "action": "none", "memory_exceeded": False,
        }
    code = cmssw_exit_code if cmssw_exit_code != 0 else job_exit_code
    if code in PERMANENT_CODES:
        return {
            "category": "permanent", "retryable": False,
            "bad_input_files": [], "action": "permanent_failure", "memory_exceeded": False,
        }
    if code in DATA_CODES:
        return {
            "category": "data", "retryable": False,
            "bad_input_files": [], "action": "permanent_failure", "memory_exceeded": False,
        }
    if code in MEMORY_CODES:
        return {
            "category": "infrastructure_memory", "retryable": True,
            "bad_input_files": [], "action": "retry_with_cooloff", "memory_exceeded": True,
        }
    if code in INFRASTRUCTURE_CODES:
        return {
            "category": "infrastructure", "retryable": True,
            "bad_input_files": [], "action": "retry_with_cooloff", "memory_exceeded": False,
        }
    return {
        "category": "transient", "retryable": True,
        "bad_input_files": [], "action": "retry", "memory_exceeded": False,
    }


# ── FJR parsing ─────────────────────────────────────────────────

def parse_fjr(xml_path):
    """Parse a CMSSW FrameworkJobReport XML. Returns dict or None on failure."""
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except (ET.ParseError, FileNotFoundError, OSError):
        return None

    result = {
        "exit_code": 0,
        "steps": {},
        "error_message": "",
        "input_files": [],
        "output_files": [],
        "performance": {
            "peak_rss_mb": 0,
            "avg_event_time_sec": 0.0,
            "events_read": 0,
            "events_written": 0,
        },
    }

    # Exit code from FrameworkError
    for err in root.findall(".//FrameworkError"):
        ec = err.get("ExitStatus", "0")
        try:
            result["exit_code"] = int(ec)
        except ValueError:
            pass
        result["error_message"] = err.text or err.get("Type", "")
        break

    # Per-step status from ExitCode elements
    for step_elem in root.findall(".//Step"):
        step_name = step_elem.get("Name", "unknown")
        exit_code = 0
        status = 0
        for ec_elem in step_elem.findall(".//ExitCode"):
            try:
                exit_code = int(ec_elem.get("Value", "0"))
                status = 1 if exit_code != 0 else 0
            except ValueError:
                pass
        result["steps"][step_name] = {"status": status, "exit_code": exit_code}

    # Input files
    for inp in root.findall(".//InputFile"):
        lfn = inp.findtext("LFN", "")
        if lfn:
            result["input_files"].append(lfn)
        er = inp.findtext("EventsRead")
        if er is not None:
            try:
                result["performance"]["events_read"] += int(er)
            except ValueError:
                pass

    # GEN fallback — ProcessingSummary NumberEvents
    if result["performance"]["events_read"] == 0:
        for ps in root.findall(".//PerformanceSummary"):
            if ps.get("Metric") == "ProcessingSummary":
                for m in ps.findall("Metric"):
                    if m.get("Name") == "NumberEvents":
                        try:
                            result["performance"]["events_read"] = int(m.get("Value", 0))
                        except ValueError:
                            pass

    # Output files
    for f in root.findall(".//File"):
        pfn = f.findtext("PFN", "")
        total_events = f.findtext("TotalEvents", "0")
        try:
            result["performance"]["events_written"] += int(total_events)
        except ValueError:
            pass
        if pfn:
            result["output_files"].append(pfn)

    # Peak RSS (CMSSW reports PeakValueRss in MB)
    for ps in root.findall(".//PerformanceSummary"):
        if ps.get("Metric") == "ApplicationMemory":
            for m in ps.findall("Metric"):
                if m.get("Name") == "PeakValueRss":
                    try:
                        result["performance"]["peak_rss_mb"] = int(float(m.get("Value", 0)))
                    except ValueError:
                        pass

    # Wall time and avg event time
    wall_time = 0.0
    for ps in root.findall(".//PerformanceSummary"):
        if ps.get("Metric") == "Timing":
            for m in ps.findall("Metric"):
                if m.get("Name") == "TotalJobTime":
                    try:
                        wall_time = float(m.get("Value", 0))
                    except ValueError:
                        pass
    if wall_time > 0 and result["performance"]["events_read"] > 0:
        result["performance"]["avg_event_time_sec"] = round(
            wall_time / result["performance"]["events_read"], 3
        )

    return result


# ── HTCondor log parsing ────────────────────────────────────────

def parse_condor_log(log_path):
    """Parse HTCondor job log for site, wall time, memory usage."""
    info = {"site": "unknown", "wall_time_sec": 0, "memory_mb": 0}
    if not os.path.isfile(log_path):
        return info
    try:
        with open(log_path) as f:
            content = f.read()
    except OSError:
        return info

    # MATCH_GLIDEIN_CMSSite from job classad
    m = re.search(r'MATCH_GLIDEIN_CMSSite\s*=\s*"?([^"\s]+)"?', content)
    if m:
        info["site"] = m.group(1)

    # Memory usage (ResidentSetSize in KB → MB)
    for m in re.finditer(r'ResidentSetSize\s*=\s*(\d+)', content):
        try:
            mem_kb = int(m.group(1))
            info["memory_mb"] = max(info["memory_mb"], mem_kb // 1024)
        except ValueError:
            pass

    # Wall time from job events (approximate from terminate - execute timestamps)
    execute_times = re.findall(r'004 \(.*?\) (\d{2}/\d{2} \d{2}:\d{2}:\d{2})', content)
    terminate_times = re.findall(r'005 \(.*?\) (\d{2}/\d{2} \d{2}:\d{2}:\d{2})', content)
    if execute_times and terminate_times:
        try:
            fmt = "%m/%d %H:%M:%S"
            t_exec = datetime.strptime(execute_times[-1], fmt)
            t_term = datetime.strptime(terminate_times[-1], fmt)
            diff = (t_term - t_exec).total_seconds()
            if diff > 0:
                info["wall_time_sec"] = int(diff)
        except (ValueError, TypeError):
            pass

    return info


# ── Stderr tail ─────────────────────────────────────────────────

def read_err_tail(err_path, max_lines=200):
    """Read last max_lines of a file."""
    if not os.path.isfile(err_path):
        return ""
    try:
        with open(err_path) as f:
            lines = f.readlines()
        return "".join(lines[-max_lines:])
    except OSError:
        return ""


# ── Main entry point ────────────────────────────────────────────

def main():
    if len(sys.argv) < 5:
        print("Usage: wms2_post_collect.py <node_name> <exit_code> <retry_num> <max_retries>",
              file=sys.stderr)
        sys.exit(1)

    node_name = sys.argv[1]
    try:
        job_exit_code = int(sys.argv[2])
    except ValueError:
        job_exit_code = 1
    try:
        retry_num = int(sys.argv[3])
    except ValueError:
        retry_num = 0
    try:
        max_retries = int(sys.argv[4])
    except ValueError:
        max_retries = 3

    # Parse FJR — look for report_step*.xml or FrameworkJobReport*.xml
    cmssw_data = None
    fjr_files = sorted(glob.glob("report_step*.xml") + glob.glob("FrameworkJobReport*.xml"))
    for fjr_path in fjr_files:
        parsed = parse_fjr(fjr_path)
        if parsed is not None:
            cmssw_data = parsed
            break  # use first parseable FJR

    cmssw_exit_code = 0
    if cmssw_data:
        cmssw_exit_code = cmssw_data.get("exit_code", 0)

    # Parse HTCondor log
    condor_log = parse_condor_log(f"{node_name}.log")

    # Read stderr tail
    log_tail = read_err_tail(f"{node_name}.err")

    # Classify
    error_message = cmssw_data.get("error_message", "") if cmssw_data else ""
    classification = classify_error(cmssw_exit_code, job_exit_code, error_message)

    # For data errors, populate bad_input_files from FJR input files
    if classification["category"] == "data" and cmssw_data:
        classification["bad_input_files"] = cmssw_data.get("input_files", [])

    # Determine if this is the final attempt
    is_final = (
        job_exit_code == 0 or
        not classification["retryable"] or
        retry_num >= max_retries
    )

    # Build post.json
    post_data = {
        "node_name": node_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "attempt": retry_num,
        "max_retries": max_retries,
        "final": is_final,
        "job": {
            "exit_code": job_exit_code,
            "exit_signal": None,
            "hold_reason": None,
            "wall_time_sec": condor_log["wall_time_sec"],
            "cpu_time_sec": 0,
            "memory_mb": condor_log["memory_mb"],
            "site": condor_log["site"],
            "schedd": "",
        },
        "cmssw": {
            "exit_code": cmssw_exit_code,
            "steps": cmssw_data.get("steps", {}) if cmssw_data else {},
            "error_message": error_message,
            "input_files": cmssw_data.get("input_files", []) if cmssw_data else [],
            "output_files": cmssw_data.get("output_files", []) if cmssw_data else [],
            "performance": cmssw_data.get("performance", {}) if cmssw_data else {},
        },
        "classification": classification,
        "log_tail": log_tail[-10000:] if len(log_tail) > 10000 else log_tail,
    }

    # Write post.json
    post_json_path = f"{node_name}.post.json"
    try:
        with open(post_json_path, "w") as f:
            json.dump(post_data, f, indent=2)
    except OSError as e:
        print(f"Warning: failed to write {post_json_path}: {e}", file=sys.stderr)

    # Print classification category to stdout (consumed by post_script.sh)
    print(classification["category"])


if __name__ == "__main__":
    main()
'''
