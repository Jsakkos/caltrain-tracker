"""Shared matplotlib styling for the analysis notebooks."""
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

# Categorical slots (fixed order, never cycled): NB is always blue, SB always orange.
BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET, RED = (
    "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948")
DIR_COLOR = {"NB": BLUE, "SB": ORANGE}
SERVICE_COLOR = {"Local": BLUE, "Limited": AQUA, "Express": ORANGE, "South County": VIOLET}
TEXT, MUTED, GRID = "#0b0b0b", "#52514e", "#e6e5e1"

# Sequential: one hue light -> dark. Diverging: blue <- neutral gray -> orange.
SEQ = LinearSegmentedColormap.from_list("seq_blue", ["#f3f7fd", "#cde2fb", "#6da7ec", "#2a78d6", "#184f95", "#0d366b"])
DIV = LinearSegmentedColormap.from_list("div_blue_orange", ["#184f95", "#6da7ec", "#dcdcd8", "#f0a27d", "#b8431a"])


def setup() -> None:
    mpl.rcParams.update({
        "figure.dpi": 110, "figure.figsize": (10, 4.2), "savefig.bbox": "tight",
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.edgecolor": MUTED, "axes.labelcolor": TEXT, "axes.titlecolor": TEXT,
        "axes.titlesize": 12, "axes.titleweight": "semibold", "axes.titlelocation": "left",
        "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
        "xtick.color": MUTED, "ytick.color": MUTED, "xtick.labelsize": 9, "ytick.labelsize": 9,
        "legend.frameon": False, "legend.fontsize": 9,
        "lines.linewidth": 2, "lines.markersize": 5,
        "font.size": 10, "text.color": TEXT,
    })


def note(ax, text: str) -> None:
    """Small caption under the axes for sample sizes and caveats."""
    ax.text(0, -0.18, text, transform=ax.transAxes, fontsize=8, color=MUTED, va="top")


setup()
