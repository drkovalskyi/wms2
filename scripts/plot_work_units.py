#!/usr/bin/env python3
"""Generate StepChain vs TaskChain work unit comparison diagram."""

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

# ── Tunable font sizes (increase these for slides) ────────────
FONT_TITLE = 20          # diagram titles
FONT_NODE_LABEL = 14     # text inside circles (landing, merge, cleanup)
FONT_STEP_LABEL = 9      # step names inside rectangular blocks
FONT_PROC_LABEL = 9      # "proc_0" .. "proc_7" labels below boxes
FONT_LAYER_LABEL = 11    # step names on left margin (TaskChain)

# ── Configuration ──────────────────────────────────────────────
N_PROCS = 8
STEPS = ["GEN-SIM", "DIGI", "HLT", "RECO", "MINI", "NANO"]
OUTPUT_STEPS = {"MINI", "NANO"}

# Colors
C_LANDING = "#cccccc"
C_STEP = "#b3d4fc"       # blue for intermediate steps
C_OUTPUT = "#b6e2b6"     # green for output-producing steps
C_MERGE = "#7bc47b"
C_CLEANUP = "#f0ad4e"
C_ARROW = "#555555"
C_ARROW_LIGHT = "#bbbbbb"

# Layout — oval nodes
NODE_RX = 0.55            # oval horizontal radius
NODE_RY = 0.30            # oval vertical radius
PROC_SPACING = 1.2       # horizontal spacing between proc columns
STEP_BOX_H = 0.36        # actual height of each step rectangle
STEP_PITCH = 0.62        # vertical distance between step centers (box + gap)
STEP_W = 0.80            # step block width
PROC_BOX_W = 0.95        # StepChain proc box width
PROC_BOX_PAD = 0.18      # padding above/below steps in proc box

# Shared Y coordinates (both diagrams use these)
Y_LANDING = 0
Y_PROC_TOP = -1.6                                          # top of proc region
Y_PROC_BOT = Y_PROC_TOP - (len(STEPS) * STEP_PITCH + 2 * PROC_BOX_PAD)
Y_MERGE = Y_PROC_BOT - 1.6
Y_CLEANUP = Y_MERGE - 1.4


def _arrow(ax, xy_from, xy_to, color=C_ARROW, lw=0.8):
    ax.annotate("", xy=xy_to, xytext=xy_from,
                arrowprops=dict(arrowstyle="-|>", color=color, lw=lw,
                                shrinkA=2, shrinkB=2))


def _oval(ax, x, y, label, color, rx=NODE_RX, ry=NODE_RY,
          fontsize=FONT_NODE_LABEL):
    from matplotlib.patches import Ellipse
    e = Ellipse((x, y), 2 * rx, 2 * ry, fc=color, ec="black", lw=0.8, zorder=3)
    ax.add_patch(e)
    ax.text(x, y, label, ha="center", va="center", fontsize=fontsize,
            zorder=4, fontfamily="sans-serif")


def _step_block(ax, x, y, label, color):
    """Draw a single step rectangle centered at (x, y)."""
    block = FancyBboxPatch(
        (x - STEP_W / 2, y - STEP_BOX_H / 2),
        STEP_W, STEP_BOX_H,
        boxstyle="round,pad=0.02", fc=color, ec="black", lw=0.5, zorder=3)
    ax.add_patch(block)
    ax.text(x, y, label, ha="center", va="center",
            fontsize=FONT_STEP_LABEL, zorder=4, fontfamily="sans-serif")


def _step_ys():
    """Return list of y-centers for each step, top to bottom."""
    return [Y_PROC_TOP - PROC_BOX_PAD - j * STEP_PITCH - STEP_PITCH / 2
            for j in range(len(STEPS))]


def _draw_stepchain(ax):
    """Draw StepChain work unit on the given axes."""
    total_w = (N_PROCS - 1) * PROC_SPACING
    x_left = -total_w / 2
    proc_xs = [x_left + i * PROC_SPACING for i in range(N_PROCS)]
    step_ys = _step_ys()
    proc_box_h = Y_PROC_TOP - Y_PROC_BOT

    # Landing node
    _oval(ax, 0, Y_LANDING, "landing", C_LANDING)

    # Proc boxes
    for i, px in enumerate(proc_xs):
        # Outer frame
        rect = FancyBboxPatch(
            (px - PROC_BOX_W / 2, Y_PROC_BOT),
            PROC_BOX_W, proc_box_h,
            boxstyle="round,pad=0.05", fc="white", ec="black", lw=0.8, zorder=2)
        ax.add_patch(rect)

        # Step blocks inside
        for j, (step, sy) in enumerate(zip(STEPS, step_ys)):
            color = C_OUTPUT if step in OUTPUT_STEPS else C_STEP
            _step_block(ax, px, sy, step, color)

            if j < len(STEPS) - 1:
                _arrow(ax, (px, sy - STEP_BOX_H / 2),
                       (px, sy - STEP_PITCH - STEP_BOX_H / 2),
                       color="#999999", lw=0.5)

        # proc label below box
        ax.text(px, Y_PROC_BOT - 0.18, f"proc_{i}", ha="center", va="top",
                fontsize=FONT_PROC_LABEL, fontfamily="sans-serif", color="#333333")

        # Arrow: landing → proc
        _arrow(ax, (0, Y_LANDING - NODE_RY),
               (px, Y_PROC_TOP + 0.05),
               color=C_ARROW_LIGHT, lw=0.6)

        # Arrow: proc → merge
        _arrow(ax, (px, Y_PROC_BOT - 0.22),
               (0, Y_MERGE + NODE_RY),
               color=C_ARROW_LIGHT, lw=0.6)

    # Merge + cleanup
    _oval(ax, 0, Y_MERGE, "merge", C_MERGE)
    _oval(ax, 0, Y_CLEANUP, "cleanup", C_CLEANUP)
    _arrow(ax, (0, Y_MERGE - NODE_RY),
           (0, Y_CLEANUP + NODE_RY), color=C_ARROW, lw=0.8)

    ax.set_title("StepChain Work Unit", fontsize=FONT_TITLE, fontweight="bold",
                 fontfamily="sans-serif", pad=18)


def _draw_taskchain(ax):
    """Draw TaskChain work unit on the given axes."""
    total_w = (N_PROCS - 1) * PROC_SPACING
    x_left = -total_w / 2
    node_xs = [x_left + i * PROC_SPACING for i in range(N_PROCS)]
    step_ys = _step_ys()

    # Landing
    _oval(ax, 0, Y_LANDING, "landing", C_LANDING)

    # Draw layers — each step is a row of rectangular blocks (same as StepChain)
    for k, (step, sy) in enumerate(zip(STEPS, step_ys)):
        color = C_OUTPUT if step in OUTPUT_STEPS else C_STEP

        # Layer label on the left
        ax.text(x_left - 0.8, sy, step, ha="right", va="center",
                fontsize=FONT_LAYER_LABEL, fontfamily="sans-serif",
                fontstyle="italic", color="#555555")

        for i, nx in enumerate(node_xs):
            _step_block(ax, nx, sy, f"proc_{i+k*8}", color)

            # Arrow: landing → first layer
            if k == 0:
                _arrow(ax, (0, Y_LANDING - NODE_RY),
                       (nx, sy + STEP_BOX_H / 2),
                       color=C_ARROW_LIGHT, lw=0.6)

            # Arrow: previous layer → this layer
            if k > 0:
                prev_sy = step_ys[k - 1]
                _arrow(ax, (nx, prev_sy - STEP_BOX_H / 2),
                       (nx, sy + STEP_BOX_H / 2),
                       color=C_ARROW, lw=0.7)

    # Arrows: last layer → merge
    last_sy = step_ys[-1]
    for nx in node_xs:
        _arrow(ax, (nx, last_sy - STEP_BOX_H / 2),
               (0, Y_MERGE + NODE_RY),
               color=C_ARROW_LIGHT, lw=0.6)

    # Merge + cleanup
    _oval(ax, 0, Y_MERGE, "merge", C_MERGE)
    _oval(ax, 0, Y_CLEANUP, "cleanup", C_CLEANUP)
    _arrow(ax, (0, Y_MERGE - NODE_RY),
           (0, Y_CLEANUP + NODE_RY), color=C_ARROW, lw=0.8)

    ax.set_title("TaskChain Work Unit", fontsize=FONT_TITLE, fontweight="bold",
                 fontfamily="sans-serif", pad=18)


def main():
    fig, (ax_step, ax_task) = plt.subplots(1, 2, figsize=(22, 13))

    for ax in (ax_step, ax_task):
        ax.set_aspect("equal")
        ax.axis("off")

    _draw_stepchain(ax_step)
    _draw_taskchain(ax_task)

    # Consistent limits for both
    margin = 1.0
    total_w = (N_PROCS - 1) * PROC_SPACING
    for ax in (ax_step, ax_task):
        ax.set_xlim(-total_w / 2 - 2.0, total_w / 2 + 1.5)
        ax.set_ylim(Y_CLEANUP - margin, Y_LANDING + 1.2)

    plt.tight_layout()
    out = "work_unit_comparison.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"Saved to {out}")
    plt.close()


if __name__ == "__main__":
    main()
