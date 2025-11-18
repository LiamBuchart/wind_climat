# simple functions to make better plots
# mostly things that I have found online
import numpy as np
import matplotlib.pyplot as plt

def label_line(ax, line, x, label=None, align=True, **kwargs):
    """
    Place a label on a given line at position x.

    Parameters:
        ax      : Matplotlib Axes object
        line    : Line2D object
        x       : x-coordinate for label placement
        label   : Text for the label (defaults to line's label)
        align   : If True, rotate text to match line slope
        kwargs  : Additional text properties
    """
    xdata = line.get_xdata()
    ydata = line.get_ydata()

    # Ensure x is within data range
    if (x < xdata.min()) or (x > xdata.max()):
        raise ValueError("x position for label is outside data range.")

    # Interpolate y for given x
    y = np.interp(x, xdata, ydata)

    if label is None:
        label = line.get_label()

    if align:
        # Calculate slope for rotation
        dx = (xdata[2] - xdata[1]) * 2
        dy = (ydata[2] - ydata[1]) * 2
        angle = np.degrees(np.arctan2(dy, dx))
    else:
        angle = 0

    # Place the label
    ax.text(x, y, label, rotation=angle, rotation_mode='anchor',
            ha='center', va='center', backgroundcolor='white', **kwargs)
    
