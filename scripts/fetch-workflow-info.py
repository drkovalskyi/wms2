#!/usr/bin/env python3
"""Fetch workflow info from ReqMgr2 and print formatted markdown.

Standalone script — stdlib only (urllib + ssl), no pip dependencies.

Usage:
    python scripts/fetch-workflow-info.py REQUEST_NAME [REQUEST_NAME ...]
    python scripts/fetch-workflow-info.py --from-file request.json

Examples:
    # Fetch from ReqMgr2
    python scripts/fetch-workflow-info.py \\
        cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54 \\
        --proxy /mnt/creds/x509up

    # Use a local request.json (same format as ReqMgr2 response)
    python scripts/fetch-workflow-info.py \\
        --from-file /mnt/shared/work/wms2_real_test/request.json
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import urllib.request
from typing import Any

REQMGR2_BASE = "https://cmsweb.cern.ch/reqmgr2"
DEFAULT_CA = "/cvmfs/grid.cern.ch/etc/grid-security/certificates"


# ── ReqMgr2 fetch ────────────────────────────────────────────


def _make_ssl_context(
    proxy: str, ca_path: str | None
) -> ssl.SSLContext:
    """Build SSL context with X.509 proxy and optional CA bundle."""
    ca = ca_path or DEFAULT_CA
    if os.path.isdir(ca):
        ctx = ssl.create_default_context(capath=ca)
    elif os.path.isfile(ca):
        ctx = ssl.create_default_context(cafile=ca)
    else:
        ctx = ssl.create_default_context()
    ctx.load_cert_chain(proxy, proxy)
    return ctx


def fetch_request(
    request_name: str, ssl_ctx: ssl.SSLContext
) -> dict[str, Any]:
    """Fetch a single request from ReqMgr2 and unwrap the response."""
    url = f"{REQMGR2_BASE}/data/request/{request_name}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, context=ssl_ctx) as resp:
        data = json.loads(resp.read())
    # ReqMgr2 wraps: {"result": [{request_name: {fields}}]}
    results = data.get("result", [])
    if not results or not results[0]:
        raise ValueError(f"Request {request_name} not found in ReqMgr2")
    row = results[0]
    if request_name in row:
        return row[request_name]
    return next(iter(row.values()))


def load_from_file(path: str) -> dict[str, Any]:
    """Load request data from a local JSON file.

    Accepts either a raw request dict (has "RequestType" at top level)
    or a ReqMgr2-style wrapped response.
    """
    with open(path) as f:
        data = json.load(f)
    # Raw request dict
    if "RequestType" in data:
        return data
    # Wrapped ReqMgr2 response
    if "result" in data:
        row = data["result"][0]
        return next(iter(row.values()))
    return data


# ── Field extraction ──────────────────────────────────────────


def extract_info(req: dict[str, Any]) -> dict[str, Any]:
    """Extract the fields we care about from a request dict."""
    name = req.get("RequestName") or req.get("_id", "unknown")
    rtype = req.get("RequestType", "unknown")
    nsteps = req.get("StepChain") or req.get("TaskChain") or 0
    multicore = req.get("Multicore", 1)
    memory = req.get("Memory", 0)
    tpe = req.get("TimePerEvent", 0)
    spe = req.get("SizePerEvent", 0)

    # RequestNumEvents lives in Step1/Task1, not top-level
    step1 = req.get("Step1") or req.get("Task1") or {}
    num_events = step1.get("RequestNumEvents") or req.get("RequestNumEvents") or 0

    # FilterEfficiency from Step1 (top-level is often absent)
    filter_eff = float(
        req.get("FilterEfficiency")
        or step1.get("FilterEfficiency")
        or 1.0
    )
    if filter_eff <= 0:
        filter_eff = 1.0

    # CPU-days: TimePerEvent is per *generated* event, RequestNumEvents is
    # desired *output* events.  Total generated = num_events / filter_eff.
    # CPU time = wall_time * cores = (generated * TPE) * cores.
    cpu_days = (
        tpe * multicore * num_events / filter_eff / 86400
        if (tpe and num_events) else 0
    )

    # Per-step details
    steps = []
    for i in range(1, nsteps + 1):
        key = f"Step{i}" if "StepChain" in req else f"Task{i}"
        step = req.get(key, {})
        cmssw = step.get("CMSSWVersion", "")
        arch_raw = step.get("ScramArch", "")
        arch = arch_raw[0] if isinstance(arch_raw, list) else arch_raw
        keep = step.get("KeepOutput", True)
        step_name = step.get("StepName", f"step{i}")
        gt = step.get("GlobalTag", "")
        filt_eff = step.get("FilterEfficiency", 1)
        steps.append({
            "num": i,
            "name": step_name,
            "cmssw": cmssw,
            "arch": arch,
            "keep": keep,
            "global_tag": gt,
            "filter_efficiency": filt_eff,
        })

    # Output datasets and tiers
    output_datasets = req.get("OutputDatasets", [])
    tiers = [ds.rsplit("/", 1)[-1] for ds in output_datasets]

    # Detect GEN workflow (Step1 has no input)
    is_gen = False
    if rtype == "StepChain" and step1:
        if not step1.get("InputDataset") and not step1.get("InputFromOutputModule"):
            is_gen = True

    # Has LHE input?
    has_lhe = bool(step1.get("LheInputFiles"))

    return {
        "request_name": name,
        "request_type": rtype,
        "num_steps": nsteps,
        "multicore": multicore,
        "memory_mb": memory,
        "time_per_event": tpe,
        "size_per_event": spe,
        "filter_efficiency": filter_eff,
        "request_num_events": num_events,
        "cpu_days": cpu_days,
        "steps": steps,
        "output_datasets": output_datasets,
        "output_tiers": tiers,
        "is_gen": is_gen,
        "has_lhe": has_lhe,
    }


# ── Markdown formatting ──────────────────────────────────────


def _short_name(request_name: str) -> str:
    """Derive a short name from the request name.

    e.g. 'cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_...' → 'NPS'
    """
    # Strip cmsunified_task_ prefix
    name = request_name
    if name.startswith("cmsunified_task_"):
        name = name[len("cmsunified_task_"):]
    # Take the physics group prefix (before the first '-')
    parts = name.split("-", 1)
    return parts[0]


def format_markdown(info: dict[str, Any]) -> str:
    """Format extracted info as a markdown detail card."""
    lines: list[str] = []
    name = info["request_name"]
    short = _short_name(name)

    lines.append(f"### {short}")
    lines.append("")
    lines.append(f"**Request name**: `{name}`")
    lines.append("")

    # Summary line
    gen_label = " (GEN)" if info["is_gen"] else ""
    lhe_label = " + LHE" if info["has_lhe"] else ""
    lines.append(
        f"- **Type**: {info['request_type']}, "
        f"{info['num_steps']}-step{gen_label}{lhe_label}"
    )
    lines.append(f"- **Multicore**: {info['multicore']}")
    lines.append(f"- **Memory**: {info['memory_mb']} MB")
    lines.append(f"- **TimePerEvent**: {info['time_per_event']:.2f} s")
    lines.append(f"- **SizePerEvent**: {info['size_per_event']:.1f} KB")
    fe = info.get("filter_efficiency", 1.0)
    if fe < 1.0:
        lines.append(f"- **FilterEfficiency**: {fe:.6g}")
    if info["request_num_events"]:
        lines.append(f"- **RequestNumEvents**: {info['request_num_events']:,}")
    if info["cpu_days"]:
        lines.append(f"- **CPU-days**: {info['cpu_days']:,.1f}")
    lines.append(f"- **Output tiers**: {', '.join(info['output_tiers']) or 'N/A'}")
    lines.append("")

    # Step table
    lines.append("| Step | Name | CMSSW | Arch | Keep | GlobalTag |")
    lines.append("|------|------|-------|------|------|-----------|")
    for s in info["steps"]:
        keep_str = "yes" if s["keep"] else "no"
        gt_short = s["global_tag"][:30] + "..." if len(s["global_tag"]) > 33 else s["global_tag"]
        lines.append(
            f"| {s['num']} | {s['name']} | {s['cmssw']} | {s['arch']} "
            f"| {keep_str} | {gt_short} |"
        )
    lines.append("")

    # Output datasets
    if info["output_datasets"]:
        lines.append("**Output datasets**:")
        lines.append("```")
        for ds in info["output_datasets"]:
            lines.append(ds)
        lines.append("```")
        lines.append("")

    return "\n".join(lines)


def format_table_row(info: dict[str, Any]) -> str:
    """Format a single row for the quick-reference table."""
    short = _short_name(info["request_name"])
    cpu_days = f"{info['cpu_days']:,.1f}" if info["cpu_days"] else "—"
    tiers = ", ".join(info["output_tiers"]) or "—"
    return (
        f"| {short} | {info['request_type']} | {info['num_steps']} "
        f"| {info['multicore']} | {info['memory_mb']} "
        f"| {cpu_days} | {tiers} |"
    )


# ── CLI ───────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch workflow info from ReqMgr2 and print formatted markdown.",
    )
    parser.add_argument(
        "request_names",
        nargs="*",
        help="One or more ReqMgr2 request names to fetch",
    )
    parser.add_argument(
        "--from-file",
        action="append",
        default=[],
        dest="files",
        help="Load request data from local JSON file(s) instead of ReqMgr2",
    )
    parser.add_argument(
        "--proxy",
        default=None,
        help=f"X.509 proxy cert (default: /tmp/x509up_u$UID)",
    )
    parser.add_argument(
        "--ca-bundle",
        default=None,
        help=f"CA bundle for CERN Grid (default: {DEFAULT_CA})",
    )
    parser.add_argument(
        "--table-only",
        action="store_true",
        help="Print only the quick-reference table rows (no detail cards)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not args.request_names and not args.files:
        parser.error("Provide request name(s) or --from-file path(s)")

    infos: list[dict[str, Any]] = []

    # Load from local files
    for path in args.files:
        req = load_from_file(path)
        infos.append(extract_info(req))

    # Fetch from ReqMgr2
    if args.request_names:
        proxy = args.proxy
        if not proxy:
            default = f"/tmp/x509up_u{os.getuid()}"
            if os.path.exists(default):
                proxy = default
            else:
                parser.error(
                    f"No X.509 proxy found at {default}. Use --proxy."
                )
        ssl_ctx = _make_ssl_context(proxy, args.ca_bundle)
        for name in args.request_names:
            print(f"Fetching {name}...", file=sys.stderr)
            req = fetch_request(name, ssl_ctx)
            infos.append(extract_info(req))

    if not infos:
        print("No workflows to display.", file=sys.stderr)
        return

    # Output
    if args.table_only:
        print("| Name | Type | Steps | Cores | Memory (MB) | CPU-days | Output Tiers |")
        print("|------|------|-------|-------|-------------|----------|--------------|")
        for info in infos:
            print(format_table_row(info))
    else:
        # Full output: table + detail cards
        print("## Quick Reference\n")
        print("| Name | Type | Steps | Cores | Memory (MB) | CPU-days | Output Tiers |")
        print("|------|------|-------|-------|-------------|----------|--------------|")
        for info in infos:
            print(format_table_row(info))
        print("\n## Detail Cards\n")
        for info in infos:
            print(format_markdown(info))


if __name__ == "__main__":
    main()
