"""CLI entry point: python -m tests.matrix

Usage:
    python -m tests.matrix -l smoke              # run smoke set
    python -m tests.matrix -l 100.0,500.0        # run specific workflows
    python -m tests.matrix -l 500-530            # run ID range
    python -m tests.matrix -l full               # run everything
    python -m tests.matrix --list                # list all workflows + env status
    python -m tests.matrix --list -l integration # show what's in a set
    python -m tests.matrix --report              # re-render latest saved results
    python -m tests.matrix --report FILE         # re-render specific results file
    python -m tests.matrix --results             # list saved result files
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Ensure project is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.matrix.catalog import CATALOG
from tests.matrix.definitions import WorkflowDef
from tests.matrix.environment import detect_capabilities, detect_capabilities_detailed
from tests.matrix.reporter import print_catalog_list, print_summary
from tests.matrix.runner import (
    MatrixRunner,
    list_saved_results,
    load_results,
    save_results,
)
from tests.matrix.sets import get_set, list_sets


def _parse_workflow_list(spec: str) -> list[float]:
    """Parse a comma-separated list of IDs, ranges (500-530), or set names."""
    ids: list[float] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue

        # Named set?
        try:
            ids.extend(get_set(part))
            continue
        except KeyError:
            pass

        # Range?  e.g. "500-530"
        if "-" in part and not part.startswith("-"):
            lo_s, hi_s = part.split("-", 1)
            try:
                lo, hi = float(lo_s), float(hi_s)
                ids.extend(
                    wf_id for wf_id in sorted(CATALOG.keys()) if lo <= wf_id <= hi
                )
                continue
            except ValueError:
                pass

        # Single ID
        try:
            ids.append(float(part))
        except ValueError:
            print(f"WARNING: unrecognized workflow spec '{part}', skipping", file=sys.stderr)

    return ids


def _resolve_workflows(ids: list[float]) -> list[WorkflowDef]:
    """Look up WorkflowDefs from the catalog, warn on missing."""
    wfs = []
    seen = set()
    for wf_id in ids:
        if wf_id in seen:
            continue
        seen.add(wf_id)
        wf = CATALOG.get(wf_id)
        if wf is None:
            print(f"WARNING: workflow {wf_id} not in catalog, skipping", file=sys.stderr)
        else:
            wfs.append(wf)
    return wfs


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tests.matrix",
        description="WMS2 test matrix runner",
    )
    parser.add_argument(
        "-l", "--workflows",
        default="smoke",
        help="Comma-separated workflow IDs, ranges, or set names (default: smoke)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        dest="list_mode",
        help="List workflows instead of running them",
    )
    parser.add_argument(
        "--sets",
        action="store_true",
        help="List available predefined sets",
    )
    parser.add_argument(
        "--report",
        nargs="?",
        const="latest",
        metavar="FILE",
        help="Re-render report from saved results (default: latest)",
    )
    parser.add_argument(
        "--results",
        action="store_true",
        help="List saved result files",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    # Logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # --sets: list predefined sets
    if args.sets:
        print("\nAvailable sets:")
        for name in list_sets():
            ids = get_set(name)
            print(f"  {name:<15s} ({len(ids)} workflows): {ids}")
        print()
        return 0

    # --results: list saved result files
    if args.results:
        saved = list_saved_results()
        if not saved:
            print("No saved results.")
        else:
            print(f"\nSaved results ({len(saved)} files):")
            for p in saved:
                print(f"  {p.name}")
        print()
        return 0

    # --report: re-render from saved results
    if args.report is not None:
        if args.report == "latest":
            results = load_results()
        else:
            results = load_results(Path(args.report))
        print_summary(results)
        return 0

    # Resolve workflow list
    wf_ids = _parse_workflow_list(args.workflows)
    workflows = _resolve_workflows(wf_ids)

    if not workflows:
        print("No workflows matched.", file=sys.stderr)
        return 1

    # --list: show catalog table
    if args.list_mode:
        print("\nDetecting environment capabilities...")
        caps = detect_capabilities()
        caps_detail = detect_capabilities_detailed()
        for cap, (ok, detail) in sorted(caps_detail.items()):
            status = "OK" if ok else "MISSING"
            print(f"  {cap:<12s} [{status:>7s}] {detail}")
        print_catalog_list(workflows, caps=caps)
        return 0

    # Run
    print(f"\nWMS2 Test Matrix — {len(workflows)} workflow(s)")
    print(f"  Set: {args.workflows}")
    print()

    caps = detect_capabilities()
    runner = MatrixRunner(caps=caps)
    results = asyncio.run(runner.run_many(workflows))

    # Save results for later re-rendering
    save_results(results, tag=args.workflows)

    print_summary(results)

    # Exit code: 0 if all passed or skipped, 1 otherwise
    failed = any(r.status in ("failed", "error", "timeout") for r in results)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
