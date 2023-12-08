from types import TracebackType
from typing import Self, Any, ContextManager
import logging
from pathlib import Path

import yaml

import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib import get_backend, use

from pyramid.model.model import DynamicImport
from pyramid.trials.trials import Trial


class Plotter(DynamicImport):
    """Abstract interface for objects that plot to a figure and update each trial."""

    please_quit:bool = False

    def quit(self, event: Any = None) -> None:
        """Any plotter instance may call quit() on itself to request Pyramid quit from gui mode.

        Takes an event argument to make this convenient as a Matplotlib widget callback.
        """
        self.please_quit = True

    def set_up(
        self,
        fig: Figure,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        """Use the given fig to set up and store any axes, lines, user data, etc for this plot."""
        pass  # pragma: no cover

    def update(
        self,
        fig: Figure,
        current_trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        """Update stored axes, lines, user data, etc for the current trial."""
        pass  # pragma: no cover

    def clean_up(self, fig: Figure) -> None:
        """Clean up when it's time to go, if needed."""
        pass  # pragma: no cover


class PlotFigureController(ContextManager):
    """Registry and utils for Plotter instances and corresponding, managed figures.

    We want pyramid GUI mode to be able to juggle several tasks at the same time:
     - checking for new reader data and delimited trials
     - updating plots for each new trial
     - responding to GUI user inputs like resizing figures or pressing buttons/widgets
     - responding to GUI window closing so we can exit

    So, things are asyncronous from both the trial data side and from the user interface side.
    This is manageable with matplotlib, but not automatic.
    Here's some reading that informed the approach used here:
     - https://matplotlib.org/stable/users/explain/interactive_guide.html#explicitly-spinning-the-event-loop
     - https://stackoverflow.com/questions/7557098/matplotlib-interactive-mode-determine-if-figure-window-is-still-displayed

    We'll expect the pyramid GUI runner to loop through these tasks.
    It will expect the data side to poll for data or block with a short timeout.
    This will allow us to interleave GUI updates and event processing as well.
    This class implements the GUI updates and event processing part.
    """

    def __init__(
        self,
        plotters: list[Plotter] = [],
        experiment_info: dict[str, Any] = {},
        subject_info: dict[str, Any] = {},
        plot_positions_yaml: str = None
    ) -> None:
        self.plotters = plotters
        self.experiment_info = experiment_info
        self.subject_info = subject_info
        self.figures = {}
        self.plot_positions_yaml = plot_positions_yaml

    def __eq__(self, other: object) -> bool:
        """Compare controllers field-wise, to support use of this class in tests."""
        if isinstance(other, self.__class__):
            plotter_counts_equal = len(self.plotters) == len(other.plotters)
            plotter_types_equal = [isinstance(a, b.__class__) for a, b in zip(self.plotters, other.plotters)]
            return (
                plotter_counts_equal
                and all(plotter_types_equal)
                and self.experiment_info == other.experiment_info
                and self.subject_info == other.subject_info
            )
        else:  # pragma: no cover
            return False

    def __enter__(self) -> Self:
        # Use matplotlib in interactive mode instead of blocking on calls like plt.show().
        plt.ion()

        # Create a managed figure for each plotter to use.
        self.figures = {plotter: plt.figure() for plotter in self.plotters}

        # Reposition figures from a given plot positions YAML file.
        if self.plot_positions_yaml and Path(self.plot_positions_yaml).exists():
            with open(self.plot_positions_yaml, 'r') as f:
                plot_positions = yaml.safe_load(f)
            for fig in self.figures.values():
                figure_key = str(fig.number)
                set_figure_position(fig, plot_positions.get(figure_key, None))

        # Let each plotter set itself up.
        for plotter, fig in self.figures.items():
            plotter.set_up(fig, self.experiment_info, self.subject_info)

        return self

    def plot_next(self, current_trial: Trial, trial_number: int) -> None:
        """Let each plotter update for the current trial."""
        for plotter, fig in self.figures.items():
            if plt.fignum_exists(fig.number):
                plotter.update(fig, current_trial, trial_number, self.experiment_info, self.subject_info)
                fig.canvas.draw_idle()

    def update(self) -> None:
        """Let figure window process async, inteactive UI events."""
        for fig in self.figures.values():
            if plt.fignum_exists(fig.number):
                fig.canvas.flush_events()

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        # Close each managed figure.
        plot_positions = {}
        for plotter, fig in self.figures.items():
            plotter.clean_up(fig)
            if plt.fignum_exists(fig.number):
                key = str(fig.number)
                plot_positions[key] = get_figure_position(fig)
                plt.close(fig)

        # Record figure positions to be restored later.
        if self.plot_positions_yaml:
            with open(self.plot_positions_yaml, 'w') as f:
                yaml.safe_dump(plot_positions, f)

    def get_open_figures(self) -> list[Figure]:
        return [figure for figure in self.figures.values() if plt.fignum_exists(figure.number)]

    def stil_going(self) -> bool:
        return len(self.get_open_figures()) > 0 and not any([plotter.please_quit for plotter in self.plotters])


# Here are several utils for wrangling figure window positions.
# I thought it would be easy and handy to restore figure positions automatically.
# It wasn't easy, darn it!
# But I think it might still be handy.


def looks_like_tkinter(fig: Figure) -> bool:
    """Check if the Matplotlib graphics backend has the functions we want to use.

    Matplotlib doesn't have a general way to get and set figure window position -- it depends on the graphics backend.
    This check ckecks for the Python "tkinter" backend (or compatible),
    which I think is available by default on Linux, macOS, and Win.
    Let's start with this one backend and maybe add others if we find we need them.
    """
    return (
        hasattr(fig, "canvas")
        and hasattr(fig.canvas, "manager")
        and hasattr(fig.canvas.manager, "window")
        and hasattr(fig.canvas.manager.window, "geometry")
        and hasattr(fig.canvas.manager.window, "winfo_x")
    )


def format_geometry(position: dict[str, int]) -> str:
    """Format explicit, width, height, x, and y into a tkinter "geometry" string."""
    return f"{position['width']}x{position['height']}+{position['x']}+{position['y']}"


def parse_geometry(geometry: str) -> dict[str, int]:
    """Parse a tkinter "geometry" string into explicit width, height, x, and y."""
    geometry_parts = geometry.split("+")
    x = int(geometry_parts[1])
    y = int(geometry_parts[2])
    size_parts = geometry_parts[0].split("x")
    width = int(size_parts[0])
    height = int(size_parts[1])
    return {
        "width": width,
        "height": height,
        "x": x,
        "y": y
    }


def set_figure_position(fig: Figure, position: dict[str, int]) -> None:
    """Set the figure's current position given position values corresponding to tkinter "winfo_*()" functions.

    For whatever reason, tkinter "winfo_*()" functions seem to be more accurate than the "geometry()" function.
    But the "geometry()" function is the only way to set the position!
    So, before setting with geometry(), measure the offsets between the "winfo_*()" and "geometry()" APIs.
    """
    if position is None:  # pragma: no cover
        return

    if looks_like_tkinter(fig):
        offsets = measure_geometry_offsets(fig)
        corrected_position = {
            "width": position["width"] + offsets["width"],
            "height": position["height"] + offsets["height"],
            "x": position["x"] + offsets["x"],
            "y": position["y"] + offsets["y"],
        }
        geometry = format_geometry(corrected_position)
        fig.canvas.manager.window.geometry(geometry)
    else:  # pragma: no cover
        logging.info(f"Pyramid doesn't know how to set figure position for Matplotlib backend {get_backend()}")


def get_figure_position(fig: Figure) -> dict[str, int]:
    """Query the figure's current position using tkinter "winfo_*()" functions.

    For whatever reason, the tkinter "winfo_*()" functions seem to be more accurate than the "geometry()" function.
    """
    if looks_like_tkinter(fig):
        return {
            "width": fig.canvas.manager.window.winfo_width(),
            "height": fig.canvas.manager.window.winfo_height(),
            "x": fig.canvas.manager.window.winfo_x(),
            "y": fig.canvas.manager.window.winfo_y()
        }
    else:  # pragma: no cover
        logging.info(f"Pyramid doesn't know how to get figure position for Matplotlib backend {get_backend()}")
        return None


def measure_geometry_offsets(fig: Figure) -> dict[str, int]:
    """Measure the offsets between tkinter "winfo_*()" vs "geometry() functions.

    For whatever reason, tkinter "winfo_*()" functions seem to be more accurate than the "geometry()" function.
    But the "geometry()" function is the only way to set the position, so we're forced to use it.
    This util measures the offsets (for eg window border, title bar) between the two APIs.
    It does this by moving the figure, so we should only call this right before setting the position, anyway.
    """

    if looks_like_tkinter(fig):
        # Go to the screen origin at the top-left, which is usually possible, and seems to give consistent results.
        fig.canvas.manager.window.geometry("+0+0")
        fig.canvas.flush_events()

        # From that known position, how do the "winfo_*()" and "geometry()" APIs compare?
        geometry = fig.canvas.manager.window.geometry()
        geometry_parts = parse_geometry(geometry)
        return {
            "width": geometry_parts["width"] - fig.canvas.manager.window.winfo_width(),
            "height": geometry_parts["height"] - fig.canvas.manager.window.winfo_height(),
            "x": geometry_parts["x"] - fig.canvas.manager.window.winfo_x(),
            "y": geometry_parts["y"] - fig.canvas.manager.window.winfo_y()
        }
    else:  # pragma: no cover
        logging.info(f"Pyramid doesn't know how to measure geometry offsets for Matplotlib backend {get_backend()}")
        return {
            "width": 0,
            "height": 0,
            "x": 0,
            "y": 0
        }
