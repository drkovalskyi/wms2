#!/usr/bin/env python3
"""Generate a diagram showing multi-round request processing."""

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Ellipse

# ── Tunable font sizes ────────────────────────────────────────
FONT_TITLE = 22
FONT_ROUND_LABEL = 20    # "Round 0", "Round 1", etc.
FONT_WU_LABEL = 14        # "WU0", "WU1", etc. inside boxes
FONT_NODE_LABEL = 12     # text inside ovals
FONT_ANNOTATION = 14     # annotations like "pilot" or "adaptive"

# ── Configuration ──────────────────────────────────────────────
ROUNDS = [
    {"label": "Round 0", "n_wus": 1},
    {"label": "Round 1", "n_wus": 10},
    {"label": "Round 2", "n_wus": 7},
    {"label": "Round 3", "n_wus": 7},
]

# Colors
C_PILOT_WU = "#f9d77e"   # amber for pilot WU
C_WU = "#b3d4fc"          # blue for production WUs
C_OVAL = "#cccccc"        # gray for start/end ovals
C_OPTIMIZE = "#e8d4f0"   # light purple for optimization step
C_ARROW = "#555555"
C_ROUND_BG = "#f5f5f5"   # light gray background for round boxes
C_ROUND_BORDER = "#aaaaaa"

# Layout
WU_W = 0.7               # work unit box width
WU_H = 0.5               # work unit box height
WU_SPACING = 0.85         # horizontal spacing between WUs
ROUND_SPACING = 2.8       # vertical spacing between rounds
ROUND_PAD_X = 0.4         # horizontal padding inside round box
ROUND_PAD_TOP = 0.6       # padding above WUs for round label
ROUND_PAD_BOT = 0.3       # padding below WUs
OVAL_RX = 0.7
OVAL_RY = 0.28
OPT_W = 1.8              # optimization box width
OPT_H = 0.45             # optimization box height


def _arrow(ax, xy_from, xy_to, color=C_ARROW, lw=1.0):
    ax.annotate("", xy=xy_to, xytext=xy_from,
                arrowprops=dict(arrowstyle="-|>", color=color, lw=lw,
                                shrinkA=3, shrinkB=3))


def _oval(ax, x, y, label, color, rx=OVAL_RX, ry=OVAL_RY,
          fontsize=FONT_NODE_LABEL):
    e = Ellipse((x, y), 2 * rx, 2 * ry, fc=color, ec="black", lw=0.8, zorder=3)
    ax.add_patch(e)
    ax.text(x, y, label, ha="center", va="center", fontsize=fontsize,
            zorder=4, fontfamily="sans-serif")


def _wu_box(ax, x, y, label, color):
    box = FancyBboxPatch(
        (x - WU_W / 2, y - WU_H / 2), WU_W, WU_H,
        boxstyle="round,pad=0.04", fc=color, ec="black", lw=0.6, zorder=3)
    ax.add_patch(box)
    ax.text(x, y, label, ha="center", va="center",
            fontsize=FONT_WU_LABEL, zorder=4, fontfamily="sans-serif")


def _optimize_box(ax, x, y):
    box = FancyBboxPatch(
        (x - OPT_W / 2, y - OPT_H / 2), OPT_W, OPT_H,
        boxstyle="round,pad=0.04", fc=C_OPTIMIZE, ec="black",
        lw=0.6, zorder=3, linestyle="--")
    ax.add_patch(box)
    ax.text(x, y, "optimization", ha="center", va="center",
            fontsize=FONT_ANNOTATION, zorder=4, fontfamily="sans-serif",
            fontstyle="italic")


def main():
    fig, ax = plt.subplots(1, 1, figsize=(14, 18))
    ax.set_aspect("equal")
    ax.axis("off")

    # Find max width needed
    max_wus = max(r["n_wus"] for r in ROUNDS)
    total_w = (max_wus - 1) * WU_SPACING + 2 * ROUND_PAD_X + WU_W
    center_x = 0

    # Start oval
    y_start = 0
    _oval(ax, center_x, y_start, "request\nsubmitted", C_OVAL)

    y_cursor = y_start - OVAL_RY - 0.6

    for ri, rnd in enumerate(ROUNDS):
        n = rnd["n_wus"]
        label = rnd["label"]
        is_pilot = (ri == 0)

        # Round box dimensions
        round_w = (n - 1) * WU_SPACING + WU_W + 2 * ROUND_PAD_X
        round_h = WU_H + ROUND_PAD_TOP + ROUND_PAD_BOT
        round_x = center_x - round_w / 2
        round_y_top = y_cursor
        round_y_bot = round_y_top - round_h

        # Background box for the round
        bg = FancyBboxPatch(
            (round_x, round_y_bot), round_w, round_h,
            boxstyle="round,pad=0.08", fc=C_ROUND_BG, ec=C_ROUND_BORDER,
            lw=1.0, zorder=1)
        ax.add_patch(bg)

        # Round label at top of box
        ax.text(center_x, round_y_top - 0.15, label,
                ha="center", va="top", fontsize=FONT_ROUND_LABEL,
                fontweight="bold", fontfamily="sans-serif", zorder=2)

        # WU boxes centered in the round box
        wu_total = (n - 1) * WU_SPACING
        wu_x_left = center_x - wu_total / 2
        wu_y = round_y_bot + ROUND_PAD_BOT + WU_H / 2

        for i in range(n):
            wx = wu_x_left + i * WU_SPACING
            color = C_PILOT_WU if is_pilot else C_WU
            _wu_box(ax, wx, wu_y, f"WU{i}", color)

        # Arrow from previous element to this round
        _arrow(ax, (center_x, y_cursor + 0.15),
               (center_x, round_y_top + 0.08),
               color=C_ARROW, lw=1.0)

        y_cursor = round_y_bot - 0.5

        # Optimization step between rounds (after pilot and between production rounds)
        if ri < len(ROUNDS) - 1:
            opt_y = y_cursor
            _optimize_box(ax, center_x, opt_y)
            # Arrow: round → optimization
            _arrow(ax, (center_x, round_y_bot - 0.08),
                   (center_x, opt_y + OPT_H / 2),
                   color=C_ARROW, lw=1.0)
            y_cursor = opt_y - OPT_H / 2 - 0.5

    # End oval
    y_end = y_cursor - 0.3
    _oval(ax, center_x, y_end, "request\ncomplete", C_OVAL)
    _arrow(ax, (center_x, y_cursor + 0.15),
           (center_x, y_end + OVAL_RY),
           color=C_ARROW, lw=1.0)

    ax.set_title("Multi-Round Request Processing", fontsize=FONT_TITLE,
                 fontweight="bold", fontfamily="sans-serif", pad=20)

    # Limits
    margin = 1.0
    ax.set_xlim(-total_w / 2 - margin, total_w / 2 + margin)
    ax.set_ylim(y_end - margin, y_start + 1.2)

    plt.tight_layout()
    out = "request_rounds.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"Saved to {out}")
    plt.close()


if __name__ == "__main__":
    main()
