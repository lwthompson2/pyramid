from typing import Any
import time
import re
from binascii import crc32

import numpy as np
from matplotlib.figure import Figure
from matplotlib.pyplot import get_cmap
from matplotlib.widgets import Button

from pyramid.trials.trials import Trial
from pyramid.plotters.plotters import Plotter


# Choose some colors that are useful for plotting distict lines on a light background.
color_count = 14
color_map = get_cmap('brg', color_count)


def name_to_color(name: str, alpha: float = 1.0) -> str:
    """Choose a color that corresponds in a stable way to the name of a data element."""
    hash = crc32(name.encode("utf-8"))
    index = hash % color_count
    return color_map(index, alpha=alpha)


def format_number(number):
    """Choose an arbitrary, consistent way to format numbers in UI widgets."""
    if number is None:
        return ""
    else:
        return '{:.3f} sec'.format(number)


class BasicInfoPlotter(Plotter):
    """Show static experiment and subject data and progress through trials.  Also a Quit button."""

    def set_up(
        self,
        fig: Figure,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        axes = fig.subplots(2, 1)
        axes[0].set_title(f"Pyramid!")
        axes[0].axis("off")
        axes[1].axis("off")

        static_info = []
        if experiment_info:
            static_info += [[name, value] for name, value in experiment_info.items()]

        if subject_info:
            static_info += [[name, value] for name, value in subject_info.items()]

        if static_info:
            self.static_table = axes[0].table(
                cellText=static_info,
                cellLoc="left",
                loc="center"
            )

        self.trials_table = axes[1].table(
            cellText=[
                ["pyramid elapsed:", 0],
                ["trial number:", 0],
                ["trial start:", 0],
                ["trial wrt:", 0],
                ["trial end:", 0],
            ],
            cellLoc="left",
            loc="center"
        )

        quit_axes = fig.add_axes([0, 0, .1, .05])
        self.quit_button = Button(quit_axes, "Quit")
        self.quit_button.on_clicked(self.quit)

        self.start_time = time.time()

    def update(
        self,
        fig: Figure,
        current_trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        elapsed = time.time() - self.start_time
        self.trials_table.get_celld()[(0, 1)].get_text().set_text(format_number(elapsed))
        self.trials_table.get_celld()[(1, 1)].get_text().set_text(trial_number)
        self.trials_table.get_celld()[(2, 1)].get_text().set_text(format_number(current_trial.start_time))
        self.trials_table.get_celld()[(3, 1)].get_text().set_text(format_number(current_trial.wrt_time))
        self.trials_table.get_celld()[(4, 1)].get_text().set_text(format_number(current_trial.end_time))

    def clean_up(self, fig: Figure) -> None:
        pass


class NumericEventsPlotter(Plotter):
    """Plot Pyramid NumericEventList data from buffers with names that match a pattern."""

    def __init__(
        self,
        history_size: int = 10,
        xmin: float = -2.0,
        xmax: float = 2.0,
        match_pattern: str = None,
        ylabel: str = None,
        value_index: int = 0,
        marker: str = "o",
        old_marker: str = '.'
    ) -> None:
        self.history_size = history_size
        self.history = []

        self.xmin = xmin
        self.xmax = xmax
        self.match_pattern = match_pattern
        self.ylabel = ylabel
        self.value_index = value_index
        self.marker = marker
        self.old_marker = old_marker

    def set_up(
        self,
        fig: Figure,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        self.ax = fig.subplots()
        self.ax.set_axisbelow(True)

    def update(
        self,
        fig: Figure,
        current_trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        self.ax.clear()
        self.ax.grid(which="major", axis="both")
        self.ax.set_xlabel("trial time (s)")
        self.ax.set_ylabel(self.ylabel)

        if self.match_pattern:
            self.ax.set_title(f"Numeric Events: {self.match_pattern}")
        else:
            self.ax.set_title("Numeric Events")

        # Show old events faded out.
        for old in self.history:
            for name, data in old.items():
                self.ax.scatter(
                    data.get_times(),
                    data.get_values(value_index=self.value_index),
                    color=name_to_color(name, 0.125),
                    marker=self.old_marker,
                )

        # Update finite, rolling history.
        new = {
            name: event_list
            for name, event_list in current_trial.numeric_events.items()
            if (self.match_pattern is None or re.fullmatch(self.match_pattern, name)) and event_list.event_count() > 0
        }
        self.history.append(new)
        self.history = self.history[-self.history_size:]

        # Show new events on top in full color.
        for name, data in new.items():
            self.ax.scatter(
                data.get_times(),
                data.get_values(value_index=self.value_index),
                color=name_to_color(name),
                marker=self.marker,
                label=name
            )

        self.ax.set_xlim(xmin=self.xmin, xmax=self.xmax)
        if new:
            self.ax.legend()

    def clean_up(self, fig: Figure) -> None:
        self.history = []


class SignalChunksPlotter(Plotter):
    """Plot Pyramid SignalChunk data from buffers with names that match a pattern and channels in a list."""

    def __init__(
        self,
        history_size: int = 10,
        xmin: float = -2.0,
        xmax: float = 2.0,
        match_pattern: str = None,
        channel_ids: list[str | int] = None,
        ylabel: str = None
    ) -> None:
        self.history_size = history_size
        self.history = []

        self.xmin = xmin
        self.xmax = xmax
        self.match_pattern = match_pattern
        self.channel_ids = channel_ids
        self.ylabel = ylabel

    def set_up(
        self,
        fig: Figure,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        self.ax = fig.subplots()
        self.ax.set_axisbelow(True)

    def update(
        self,
        fig: Figure,
        current_trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        self.ax.clear()
        self.ax.grid(which="major", axis="both")
        self.ax.set_xlabel("trial time (s)")
        self.ax.set_ylabel(self.ylabel)

        if self.channel_ids:
            self.ax.set_title(f"Signals: {self.channel_ids}")
        else:
            self.ax.set_title("Signals")

        # Show old events faded out.
        for old_chunks in self.history:
            for name, data in old_chunks.items():
                if self.channel_ids:
                    ids = [channel_id for channel_id in self.channel_ids if channel_id in data.channel_ids]
                else:
                    ids = data.channel_ids
                for channel_id in ids:
                    full_name = f"{name} {channel_id}"
                    self.ax.plot(data.get_times(), data.get_channel_values(
                        channel_id), color=name_to_color(full_name, 0.125))

        # Update finite, rolling history.
        new = {
            name: signal_chunk
            for name, signal_chunk in current_trial.signals.items()
            if (self.match_pattern is None or re.fullmatch(self.match_pattern, name)) and signal_chunk.sample_count() > 0
        }
        self.history.append(new)
        self.history = self.history[-self.history_size:]

        # Show new events on top in full color.
        for name, data in new.items():
            if self.channel_ids:
                ids = [channel_id for channel_id in self.channel_ids if channel_id in data.channel_ids]
            else:
                ids = data.channel_ids
            for channel_id in ids:
                full_name = f"{name} {channel_id}"
                self.ax.plot(
                    data.get_times(),
                    data.get_channel_values(channel_id),
                    color=name_to_color(full_name),
                    label=full_name)

        self.ax.set_xlim(xmin=self.xmin, xmax=self.xmax)
        if new:
            self.ax.legend()

    def clean_up(self, fig: Figure) -> None:
        self.history = []


class EnhancementTimesPlotter(Plotter):
    """Plot time-related Trial enhancements as occurence time(s) of named events."""

    def __init__(
        self,
        history_size: int = 10,
        xmin: float = -2.0,
        xmax: float = 2.0,
        enhancement_categories: list[str] = ["time"],
        match_pattern: str = None,
        marker: str = "o",
        old_marker: str = '.'
    ) -> None:
        self.history_size = history_size
        self.history = []

        self.xmin = xmin
        self.xmax = xmax
        self.enhancement_categories = enhancement_categories
        self.match_pattern = match_pattern

        self.marker = marker
        self.old_marker = old_marker

    def set_up(
        self,
        fig: Figure,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        self.ax = fig.subplots()
        self.ax.set_axisbelow(True)
        self.all_names = []

    def update(
        self,
        fig: Figure,
        current_trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        self.ax.clear()
        self.ax.grid(which="major", axis="both")
        self.ax.set_xlabel("trial time (s)")

        if self.match_pattern:
            self.ax.set_title(f"Enhancement Times: {self.enhancement_categories} {self.match_pattern}")
        else:
            self.ax.set_title(f"Enhancement Times: {self.enhancement_categories}")

        # Show old events faded out.
        for old in self.history:
            for name, times in old.items():
                row = self.all_names.index(name)
                self.ax.scatter(
                    times, row * np.ones([1, len(times)]),
                    color=name_to_color(name, 0.125),
                    marker=self.old_marker)

        # Update finite, rolling history.
        enhancement_names = []
        for category in self.enhancement_categories:
            enhancement_names += current_trial.enhancement_categories.get(category, [])

        new = {}
        for name in enhancement_names:
            if self.match_pattern is None or re.fullmatch(self.match_pattern, name):
                new[name] = current_trial.get_enhancement(name, [])
                if name not in self.all_names:
                    self.all_names.append(name)
        self.history.append(new)
        self.history = self.history[-self.history_size:]

        # Show new events on top in full color.
        for name, times in new.items():
            row = self.all_names.index(name)
            self.ax.scatter(
                times, row * np.ones([1, len(times)]),
                color=name_to_color(name),
                label=name, marker=self.marker)

        self.ax.set_yticks(range(len(self.all_names)), self.all_names)
        self.ax.set_xlim(xmin=self.xmin, xmax=self.xmax)

    def clean_up(self, fig: Figure) -> None:
        self.history = []


class EnhancementXYPlotter(Plotter):
    """Plot 2D/XY data from specific pairs of Trial enhancements."""

    def __init__(
        self,
        xy_points: dict[str, str] = {},
        xy_groups: dict[str, dict[str, str]] = {},
        history_size: int = 10,
        xmin: float = -2.0,
        xmax: float = 2.0,
        ymin: float = -2.0,
        ymax: float = 2.0,
        marker: str = "o",
        old_marker: str = '.',
        linestyle: str = ":"
    ) -> None:
        self.xy_points = xy_points
        self.xy_groups = xy_groups

        self.history_size = history_size
        self.history = []

        self.xmin = xmin
        self.xmax = xmax
        self.ymin = ymin
        self.ymax = ymax

        self.marker = marker
        self.old_marker = old_marker
        self.linestyle = linestyle

    def set_up(
        self,
        fig: Figure,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        self.ax = fig.subplots()
        self.ax.set_axisbelow(True)

    def update(
        self,
        fig: Figure,
        current_trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        self.ax.clear()
        self.ax.grid(which="major", axis="both")
        self.ax.set_title(f"XY Value Pairs")
        self.ax.set_xlabel("x")
        self.ax.set_ylabel("y")

        # Show old events faded out.
        for old in self.history:
            for name, point in old.items():
                if isinstance(point[0], list):
                    self.ax.plot(
                        point[0],
                        point[1],
                        color=name_to_color(name, 0.125),
                        linestyle=self.linestyle,
                        marker=self.old_marker,
                        markevery=[-1]
                    )
                else:
                    self.ax.scatter(point[0], point[1], color=name_to_color(name, 0.125), marker=self.old_marker)

        new = {}
        for x_name, y_name in self.xy_points.items():
            x_value = current_trial.get_enhancement(x_name)
            y_value = current_trial.get_enhancement(y_name)
            if x_value is not None and y_value is not None:
                new[x_name] = (x_value, y_value)

        for group_name, group_xy_pairs in self.xy_groups.items():
            group = current_trial.get_enhancement(group_name)
            if isinstance(group, dict):
                # Get xy pairs out of a nested dictionary.
                x_values = []
                y_values = []
                for x_name, y_name in group_xy_pairs.items():
                    x_value = group.get(x_name, None)
                    y_value = group.get(y_name, None)
                    if x_value is not None and y_value is not None:
                        x_values.append(x_value)
                        y_values.append(y_value)
                new[group_name] = (x_values, y_values)

        self.history.append(new)
        self.history = self.history[-self.history_size:]

        # Show new events on top in full color.
        for name, point in new.items():
            if isinstance(point[0], list):
                self.ax.plot(
                    point[0],
                    point[1],
                    color=name_to_color(name),
                    label=name,
                    linestyle=self.linestyle,
                    marker=self.marker,
                    markevery=[-1]
                )
            else:
                self.ax.scatter(point[0], point[1], color=name_to_color(name), label=name, marker=self.marker)

        self.ax.set_xlim(xmin=self.xmin, xmax=self.xmax)
        self.ax.set_ylim(ymin=self.ymin, ymax=self.ymax)
        if new:
            self.ax.legend()

    def clean_up(self, fig: Figure) -> None:
        self.history = []


class SpikeEventsPlotter(Plotter):
    """Plot spike events per trial, from buffers with names that match a pattern."""

    def __init__(
        self,
        xmin: float = -2.0,
        xmax: float = 2.0,
        match_pattern: str = None,
        value_selection: int = None,
        value_index: int = 0,
        marker: str = "|"
    ) -> None:
        self.xmin = xmin
        self.xmax = xmax
        self.match_pattern = match_pattern
        self.value_selection = value_selection
        self.value_index = value_index
        self.marker = marker

    def set_up(
        self,
        fig: Figure,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        self.ax = fig.subplots()
        self.ax.set_axisbelow(True)

        title = "Spike Events"
        if self.match_pattern:
            title += f" for {self.match_pattern}"
        if self.value_selection is not None:
            title += f" where value[{self.value_index}]=={self.value_selection}"
        self.ax.set_title(title)

        self.ax.grid(which="major", axis="both")
        self.ax.set_xlabel("trial time (s)")
        self.ax.set_xlim(xmin=self.xmin, xmax=self.xmax)
        self.ax.set_ylabel("trial number")
        self.ax.yaxis.get_major_locator().set_params(integer=True)

    def update(
        self,
        fig: Figure,
        current_trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        # Add a row for this trial.
        for name, event_list in current_trial.numeric_events.items():
            if (self.match_pattern is None or re.fullmatch(self.match_pattern, name)) and event_list.event_count() > 0:
                times = event_list.get_times()
                trials = trial_number * np.ones(times.shape)
                if self.value_selection is not None:
                    values = event_list.get_values(value_index=self.value_index)
                    selector = values == self.value_selection
                    times = times[selector]
                    trials = trials[selector]
                if times.size > 0:
                    self.ax.scatter(times, trials, color=name_to_color(name, alpha=0.5), marker=self.marker, label=name)

        ymax = np.ceil((trial_number + 1) / 10) * 10
        self.ax.set_ylim(ymin=0, ymax=ymax)

        (artists, labels) = self.ax.get_legend_handles_labels()
        legend_by_label = dict(zip(labels, artists))
        self.ax.legend(legend_by_label.values(), legend_by_label.keys())
