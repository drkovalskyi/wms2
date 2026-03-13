#!/usr/bin/env python3
"""Generate WMS2 architecture overview and request state machine diagram.

To move a box, change its (x, y) or (dx, dy) — arrows follow automatically.
To move the entire service group, change SVC_CENTER.
"""

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

# ── Font sizes (increase these for slides/posters) ───────────
FONT_TITLE = 22
FONT_BOX_LABEL = 14
FONT_BOX_SUBLABEL = 10
FONT_BANNER = 12
FONT_STATE = 12
FONT_TRANSITION = 9
FONT_LEGEND = 11

# ── Colors ────────────────────────────────────────────────────
C_LIFECYCLE = "#ff9f43"      # orange — lifecycle manager (hub)
C_WORKER = "#b3d4fc"         # blue — core worker components
C_INFRA = "#d4c5f9"          # lavender — infrastructure workers
C_EXTERNAL = "#c8a2c8"       # purple — external systems
C_CONDOR = "#f9d77e"         # gold — HTCondor/DAGMan
C_DB = "#b6e2b6"             # green — PostgreSQL
C_API = "#f5f5f5"            # light gray — API/UI entry points
C_SERVICE = "#fafafa"        # near-white — service boundary
C_ARROW = "#555555"
C_ADAPTIVE = "#ffe0b2"       # light orange — adaptive optimizer

# State machine colors
C_STATE_ACTIVE = "#b3d4fc"   # blue — active/in-progress states
C_STATE_TERMINAL = "#cccccc" # gray — terminal states
C_STATE_OPERATOR = "#f8b4b4" # salmon — operator-triggered states
C_STATE_ARROW = "#444444"

# ── Line thickness ────────────────────────────────────────────
LW_BOX = 3                # box border
LW_ARROW = 2              # regular arrows (LM ↔ workers, workers ↔ external)
LW_ARROW_ACCENT = 2       # emphasized arrows (→ HTCondor, service → DB)
LW_SERVICE_BORDER = 1.2     # dashed service boundary
LW_STATE_BOX = 0.8          # state machine boxes
LW_STATE_ARROW = 0.9        # state machine transitions
ARROW_HEAD = 20             # arrowhead scale (default ~10, increase for larger heads)
ARROW_SHRINK = 12           # gap between arrow tip and box edge (points)

# ── Layout constants ─────────────────────────────────────────
BOX_CORNER = 0.12           # FancyBboxPatch pad
STATE_W = 2.2               # state box width
STATE_H = 0.55              # state box height

# ── Service group ────────────────────────────────────────────
# Move SVC_CENTER to reposition the entire service boundary + all internal
# components as a single unit.  Internal boxes use (dx, dy) offsets from
# this center.

SVC_CENTER = (8.25, 7.25)   # center of the dashed service boundary
SVC_W = 15.9                # boundary width
SVC_H = 7.5                 # boundary height

# Internal boxes — offsets (dx, dy) from SVC_CENTER.
# Format: (dx, dy, w, h, label, sublabel, color, fontsize, bold)
SVC_BOXES = {
    # Central hub
    "lifecycle":  ( 0.00,  0.75, 4.0, 1.20, "Lifecycle Manager",    "state machine owner",       C_LIFECYCLE, 16,            True),
    # Left column — pre-processing workers
    "workflow":   (-5.45,  2.05, 3.0, 0.85, "Workflow Manager",     "fetch and normalize",       C_WORKER,   FONT_BOX_LABEL, False),
    "admission":  (-5.45,  0.75, 3.0, 0.85, "Admission Controller", "capacity gate",             C_WORKER,   FONT_BOX_LABEL, False),
    "planner":    (-5.45, -0.55, 3.0, 0.85, "DAG Planner",          "plan and submit",           C_WORKER,   FONT_BOX_LABEL, False),
    # Right column — runtime workers
    "monitor":    ( 5.45,  2.05, 3.0, 0.85, "DAG Monitor",          "poll HTCondor",             C_WORKER,   FONT_BOX_LABEL, False),
    "output":     ( 5.45, -1.85, 3.0, 0.85, "Output Manager",       "Rucio / DBS registration",  C_WORKER,   FONT_BOX_LABEL, False),
    "error":      ( 5.45, -0.55, 3.0, 0.85, "Error Handler",        "classify and rescue",       C_WORKER,   FONT_BOX_LABEL, False),
    "adaptive":   ( 5.45,  0.75, 3.0, 0.85, "Adaptive Optimizer",   "memory / CPU tuning",       C_ADAPTIVE, FONT_BOX_LABEL, False),
    # Infrastructure workers
    "sitemgr":    (-4.25, -2.75, 2.8, 0.85, "Site Manager",         "CRIC sync and bans",        C_INFRA,    FONT_BOX_LABEL, False),
    "metrics":    ( 1.75, -2.75, 3.0, 0.85, "Metrics Collector",    "monitoring cache",          C_INFRA,    FONT_BOX_LABEL, False),
}

# ── External boxes (absolute positions) ──────────────────────
# Format: (x, y, w, h, label, sublabel, color, fontsize, bold)
EXT_BOXES = {
    # Entry points
    "api":        ( 4.50, 13.50, 3.2, 0.75, "REST API",             "/api/v1/",                  C_API,      FONT_BOX_LABEL, False),
    "ui":         (12.00, 13.50, 3.2, 0.75, "Web UI",               "/ui/",                      C_API,      FONT_BOX_LABEL, False),
    # Data layer
    "postgres":   ( 8.25,  2.00, 3.5, 1.10, "PostgreSQL",           "requests / workflows / dags / sites / blocks", C_DB, 14, True),
    # External systems
    "reqmgr2":    ( 2.80, 12.00, 1.8, 0.75, "ReqMgr2",              "request specs",             C_EXTERNAL, FONT_BOX_LABEL, False),
    "cric":       ( 4.00,  2.20, 1.8, 0.75, "CRIC",                 "site configuration",        C_EXTERNAL, FONT_BOX_LABEL, False),
    "condor":     ( 8.25, 12.20, 3.5, 1.20, "HTCondor",             "DAGMan + schedd",           C_CONDOR,   14,            True),
    "dbs":        (12.50,  2.20, 1.8, 0.75, "DBS",                  "data catalog",              C_EXTERNAL, FONT_BOX_LABEL, False),
    "rucio":      (15.00,  2.20, 1.8, 0.75, "Rucio",                "data management",           C_EXTERNAL, FONT_BOX_LABEL, False),
}


def _resolve_boxes():
    """Merge SVC_BOXES (offset-based) and EXT_BOXES (absolute) into one dict.

    Returns dict mapping name → (x, y, w, h, label, sublabel, color, fontsize, bold)
    with SVC_BOXES offsets resolved to absolute coordinates.
    """
    cx, cy = SVC_CENTER
    boxes = {}
    for name, (dx, dy, *rest) in SVC_BOXES.items():
        boxes[name] = (cx + dx, cy + dy, *rest)
    boxes.update(EXT_BOXES)
    return boxes


# Resolved at import time so ARROWS can reference them.
BOXES = _resolve_boxes()


# ── Arrow definitions ────────────────────────────────────────
# Each arrow: (source, target, source_side, target_side, style)
#   sides: "top", "bottom", "left", "right",
#          "top-left", "top-right", "bottom-left", "bottom-right"
#   style: dict of optional overrides (color, lw, ls)

ARROWS = [
    # API/UI → Lifecycle Manager
    ("api",       "lifecycle", "bottom", "top",    dict(color="#999999", ls="--")),
    ("ui",        "lifecycle", "bottom", "top",    dict(color="#999999", ls="--")),
    # Lifecycle Manager → left workers
    ("lifecycle", "workflow",  "left",   "right",  {}),
    ("lifecycle", "admission", "left",   "right",  {}),
    ("lifecycle", "planner",   "left",   "right",  {}),
    # Lifecycle Manager → right workers
    ("lifecycle", "monitor",   "right",  "left",   {}),
    ("lifecycle", "output",    "right",  "left",   {}),
    ("lifecycle", "error",     "right",  "left",   {}),
    ("lifecycle", "adaptive",  "right",  "left",   {}),
    # Lifecycle Manager → infra workers
    ("lifecycle", "sitemgr",   "bottom", "top",    dict(color="#999999", ls="--")),
    ("lifecycle", "metrics",   "bottom", "top",    dict(color="#999999", ls="--")),
    # Workers → external systems
    ("workflow",  "reqmgr2",   "top", "bottom",    dict(color=C_EXTERNAL)),
    ("planner",   "condor",    "top-right", "bottom",  dict(color="#c8a020", lw=LW_ARROW_ACCENT)),
    ("monitor",   "condor",    "left",  "bottom", dict(color="#c8a020", lw=LW_ARROW_ACCENT)),
    ("output",    "dbs",       "bottom", "top",    dict(color=C_EXTERNAL)),
    ("output",    "rucio",     "bottom", "top",    dict(color=C_EXTERNAL)),
    ("sitemgr",   "cric",      "bottom", "top",    dict(color=C_EXTERNAL, ls="--")),
]


# ── State machine definitions ────────────────────────────────
# Each state: (x, y, color)
# To move a state, just change its (x, y) — arrows follow automatically.

STATES = {
    "NEW":           (2.5, 13.5, C_STATE_ACTIVE),
    "SUBMITTED":     (2.5, 12.2, C_STATE_ACTIVE),
    "QUEUED":        (2.5, 10.7, C_STATE_ACTIVE),
    "PLANNING":      (2.5,  9.4, C_STATE_ACTIVE),
    "ACTIVE":        (2.5,  7.9, C_STATE_ACTIVE),
    "COMPLETED":     (2.5,  3.0, C_STATE_TERMINAL),
    "PILOT_RUNNING": (6.2, 10.7, C_STATE_ACTIVE),
    "STOPPING":      (6.2,  7.9, C_STATE_OPERATOR),
    "RESUBMITTING":  (0.0,  6.3, C_STATE_ACTIVE),
    "HELD":          (6.2,  4.6, C_STATE_OPERATOR),
    "PAUSED":        (6.2,  6.3, C_STATE_OPERATOR),
    "FAILED":        (6.2,  3.0, C_STATE_TERMINAL),
    "ABORTED":       (2.5,  1.5, C_STATE_TERMINAL),
    "PARTIAL":       (6.2,  1.5, C_STATE_TERMINAL),
}

# Transitions: (source, target, side_from, side_to, label, extra_kwargs)
TRANSITIONS = [
    # Main flow
    ("NEW",           "SUBMITTED",     "bottom", "top",    "import",              {}),
    ("SUBMITTED",     "QUEUED",        "bottom", "top",    "spec fetched",        {}),
    ("QUEUED",        "PLANNING",      "bottom", "top",    "admitted",            {}),
    ("PLANNING",      "ACTIVE",        "bottom", "top",    "DAG submitted",       {}),
    ("ACTIVE",        "COMPLETED",     "bottom", "top",    "all events\nproduced", dict(label_offset=(0, 0.8))),
    # Pilot
    ("QUEUED",        "PILOT_RUNNING", "right",  "left",   "round 0",             {}),
    ("PILOT_RUNNING", "PLANNING",      "left",   "right",  "pilot done",          dict(label_offset=(0.2, 0.15))),
    # Operator stop
    ("ACTIVE",        "STOPPING",      "right",  "left",   "operator\nstop",      {}),
    ("STOPPING",      "PAUSED",        "bottom", "top",   "drained",             {}),
    # Rescue
    ("ACTIVE",        "RESUBMITTING",  "bottom",  "right",   "DAG failed",          dict(label_offset=(0.0, 0.15))),
    ("RESUBMITTING",  "QUEUED",        "top",   "left",  "rescue\nsubmitted",   dict(connectionstyle="arc3,rad=-0.25", label_offset=(-1.8, 0))),
    # Error threshold
    ("ACTIVE",        "HELD",          "bottom",  "left",   "error\nthreshold",    dict(label_offset=(0.0, 0.15), color="#cc3333")),
    # ("HELD",          "QUEUED",        "top",   "right",  "operator\nrelease",   dict(connectionstyle="arc3,rad=-0.35", label_offset=(-2.6, 0))),
    ("HELD",          "QUEUED",        "top-left",   "bottom-right",  "",                     {}),
    # Round loop
    # ("ACTIVE",        "QUEUED",        "left",   "left",   "round done\nmore work", dict(connectionstyle="arc3,rad=-0.4", label_offset=(-1.5, 0))),
    ("ACTIVE",        "QUEUED",        "left",   "left",   "", dict(connectionstyle="arc3,rad=-0.4", label_offset=(-1.5, 0))),
    # Terminal
    ("COMPLETED",     "ABORTED",       "bottom", "top",    "operator",            {}),
    ("HELD",          "FAILED",        "bottom", "top",    "max retries",         dict(color="#cc3333")),
    ("FAILED",        "PARTIAL",       "bottom", "top",    "",                    {}),
]


# ── Helper functions ─────────────────────────────────────────

def _box(ax, x, y, w, h, label, color, fontsize=FONT_BOX_LABEL,
         lw=LW_BOX, ls="-", sublabel=None, bold=False, zorder=3):
    """Draw a rounded box centered at (x, y)."""
    patch = FancyBboxPatch(
        (x - w / 2, y - h / 2), w, h,
        boxstyle=f"round,pad={BOX_CORNER}", fc=color, ec="black",
        lw=lw, ls=ls, zorder=zorder)
    ax.add_patch(patch)
    weight = "bold" if bold else "normal"
    if sublabel:
        ax.text(x, y + 0.15, label, ha="center", va="center",
                fontsize=fontsize, fontweight=weight, fontfamily="sans-serif",
                zorder=zorder + 1)
        ax.text(x, y - 0.18, sublabel, ha="center", va="center",
                fontsize=FONT_BOX_SUBLABEL, fontfamily="sans-serif",
                color="#555555", zorder=zorder + 1)
    else:
        ax.text(x, y, label, ha="center", va="center",
                fontsize=fontsize, fontweight=weight, fontfamily="sans-serif",
                zorder=zorder + 1)


def _edge_point(x, y, w, h, side):
    """Return the (px, py) on the edge of a box centered at (x, y).

    Sides: "top", "bottom", "left", "right",
           "top-left", "top-right", "bottom-left", "bottom-right".
    """
    hw, hh = w / 2, h / 2
    points = {
        "top":          (x,      y + hh),
        "bottom":       (x,      y - hh),
        "left":         (x - hw, y),
        "right":        (x + hw, y),
        "top-left":     (x - hw, y + hh),
        "top-right":    (x + hw, y + hh),
        "bottom-left":  (x - hw, y - hh),
        "bottom-right": (x + hw, y - hh),
    }
    if side not in points:
        raise ValueError(f"Unknown side: {side!r}  (valid: {', '.join(points)})")
    return points[side]


def _draw_arrow(ax, x1, y1, x2, y2, color=C_ARROW, lw=LW_ARROW, ls="-",
                zorder=5):
    """Draw an arrow from (x1,y1) to (x2,y2)."""
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", mutation_scale=ARROW_HEAD,
                                color=color, lw=lw, linestyle=ls,
                                shrinkA=ARROW_SHRINK, shrinkB=ARROW_SHRINK),
                zorder=zorder)


def _state_box(ax, x, y, label, color):
    """Draw a state box for the state machine diagram."""
    patch = FancyBboxPatch(
        (x - STATE_W / 2, y - STATE_H / 2), STATE_W, STATE_H,
        boxstyle="round,pad=0.08", fc=color, ec="black",
        lw=LW_STATE_BOX, zorder=3)
    ax.add_patch(patch)
    ax.text(x, y, label, ha="center", va="center",
            fontsize=FONT_STATE, fontfamily="sans-serif",
            fontweight="bold", zorder=4)


def _state_arrow(ax, x1, y1, x2, y2, label="", color=C_STATE_ARROW,
                 lw=LW_STATE_ARROW, connectionstyle=None, label_offset=(0, 0)):
    """Draw a labeled transition arrow for the state machine."""
    arrowprops = dict(arrowstyle="-|>", mutation_scale=ARROW_HEAD,
                      color=color, lw=lw,
                      shrinkA=ARROW_SHRINK, shrinkB=ARROW_SHRINK)
    if connectionstyle:
        arrowprops["connectionstyle"] = connectionstyle
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=arrowprops, zorder=2)
    if label:
        mx = (x1 + x2) / 2 + label_offset[0]
        my = (y1 + y2) / 2 + label_offset[1]
        ax.text(mx, my, label, ha="center", va="center",
                fontsize=FONT_TRANSITION, fontfamily="sans-serif",
                color="#555555", zorder=5,
                bbox=dict(boxstyle="round,pad=0.1", fc="white",
                          ec="none", alpha=0.85))


# ── Left panel: Component Architecture ──────────────────────

def draw_architecture(ax):
    """Draw the component architecture diagram."""
    ax.set_xlim(-0.5, 17)
    ax.set_ylim(0.3, 15.0)
    ax.set_aspect("equal")
    ax.axis("off")

    # Title
    ax.text(8.25, 14.7, "WMS2 Component Architecture", ha="center",
            va="center", fontsize=FONT_TITLE, fontweight="bold",
            fontfamily="sans-serif")

    # Banner
    ax.text(8.25, 14.15, "FastAPI + uvicorn", ha="center", va="center",
            fontsize=FONT_BANNER, fontstyle="italic",
            fontfamily="sans-serif", color="#777777")

    # Service boundary (dashed rect) — positioned from SVC_CENTER
    cx, cy = SVC_CENTER
    svc_left = cx - SVC_W / 2
    svc_bottom = cy - SVC_H / 2
    svc_rect = FancyBboxPatch(
        (svc_left, svc_bottom), SVC_W, SVC_H,
        boxstyle="round,pad=0.15", fc=C_SERVICE, ec="#999999",
        lw=LW_SERVICE_BORDER, ls="--", zorder=0)
    ax.add_patch(svc_rect)
    ax.text(svc_left + 0.3, svc_bottom + SVC_H - 0.15, "Service Internals",
            ha="left", va="top", fontsize=FONT_BOX_SUBLABEL,
            fontstyle="italic", color="#888888", fontfamily="sans-serif")

    # Draw all boxes (resolved absolute positions)
    for name, (x, y, w, h, label, sublabel, color, fs, bold) in BOXES.items():
        _box(ax, x, y, w, h, label, color, fontsize=fs, bold=bold,
             sublabel=sublabel)

    # Draw all arrows (source/target looked up from BOXES)
    for src, tgt, side_from, side_to, style in ARROWS:
        sx, sy, sw, sh = BOXES[src][:4]
        tx, ty, tw, th = BOXES[tgt][:4]
        x1, y1 = _edge_point(sx, sy, sw, sh, side_from)
        x2, y2 = _edge_point(tx, ty, tw, th, side_to)
        _draw_arrow(ax, x1, y1, x2, y2, **style)

    # Service → DB arrows (from service boundary bottom edge)
    db = BOXES["postgres"]
    _draw_arrow(ax, db[0] - 1.5, svc_bottom + 0.1, db[0] - 1.0, db[1] + db[3] / 2,
                color="#4a8c4a", lw=LW_ARROW_ACCENT)
    _draw_arrow(ax, db[0] + 1.5, svc_bottom + 0.1, db[0] + 1.0, db[1] + db[3] / 2,
                color="#4a8c4a", lw=LW_ARROW_ACCENT)

    # Legend
    legend_y = 0.8
    legend_items = [
        (C_LIFECYCLE, "Lifecycle Manager"),
        (C_WORKER, "Core workers"),
        (C_INFRA, "Infrastructure"),
        (C_CONDOR, "HTCondor"),
        (C_EXTERNAL, "External systems"),
        (C_DB, "Database"),
    ]
    for i, (color, label) in enumerate(legend_items):
        lx = 1.0 + i * 2.7
        patch = FancyBboxPatch(
            (lx - 0.2, legend_y - 0.15), 0.4, 0.3,
            boxstyle="round,pad=0.03", fc=color, ec="black", lw=0.5,
            zorder=3)
        ax.add_patch(patch)
        ax.text(lx + 0.35, legend_y, label, ha="left", va="center",
                fontsize=FONT_LEGEND, fontfamily="sans-serif")


# ── Right panel: Request State Machine ───────────────────────

def draw_state_machine(ax):
    """Draw the request status state machine."""
    ax.set_xlim(-1.5, 9.5)
    ax.set_ylim(-0.5, 15.0)
    ax.set_aspect("equal")
    ax.axis("off")

    ax.text(4.0, 14.7, "Request State Machine", ha="center",
            va="center", fontsize=FONT_TITLE, fontweight="bold",
            fontfamily="sans-serif")

    # Draw all state boxes
    for name, (x, y, color) in STATES.items():
        _state_box(ax, x, y, name, color)

    # Draw all transitions
    for src, tgt, side_from, side_to, label, extra in TRANSITIONS:
        sx, sy, _ = STATES[src]
        tx, ty, _ = STATES[tgt]
        x1, y1 = _edge_point(sx, sy, STATE_W, STATE_H, side_from)
        x2, y2 = _edge_point(tx, ty, STATE_W, STATE_H, side_to)
        _state_arrow(ax, x1, y1, x2, y2, label=label, **extra)

    # Legend
    legend_items = [
        (C_STATE_ACTIVE, "Active state"),
        (C_STATE_OPERATOR, "Operator / error"),
        (C_STATE_TERMINAL, "Terminal state"),
    ]
    for i, (color, label) in enumerate(legend_items):
        ly = 0.7 - i * 0.5
        patch = FancyBboxPatch(
            (0.0, ly - 0.15), 0.4, 0.3,
            boxstyle="round,pad=0.03", fc=color, ec="black", lw=0.5,
            zorder=3)
        ax.add_patch(patch)
        ax.text(0.55, ly, label, ha="left", va="center",
                fontsize=FONT_LEGEND, fontfamily="sans-serif")


# ── Main ─────────────────────────────────────────────────────

def main():
    fig, (ax_arch, ax_sm) = plt.subplots(
        1, 2, figsize=(24, 14),
        gridspec_kw={"width_ratios": [1.7, 1.0]})

    draw_architecture(ax_arch)
    draw_state_machine(ax_sm)

    plt.tight_layout(pad=1.5)
    out = "wms2_architecture.png"
    fig.savefig(out, dpi=300, bbox_inches="tight", facecolor="white")
    print(f"Saved to {out}")
    plt.close()


if __name__ == "__main__":
    main()
