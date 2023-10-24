from typing import Any

import numpy as np

from pyramid.trials.trials import Trial, TrialCollecter


class SessionPercentageCollecter(TrialCollecter):
    """A simple enhancer that computes start time a percentage of the while session, for demo and testing."""

    def __init__(self) -> None:
        self.max_start_time = 0

    def __eq__(self, other: object) -> bool:
        """Compare by attribute, to support use of this class in tests."""
        if isinstance(other, self.__class__):
            return self.max_start_time == other.max_start_time
        else:  # pragma: no cover
            return False

    def __hash__(self) -> int:
        """Hash by attribute, to support use of this class in tests."""
        return self.max_start_time.__hash__()

    def collect(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict,
        subject_info: dict
    ) -> None:
        # Store the max start time over all trials.
        self.max_start_time = max(self.max_start_time, trial.start_time)

    def enhance(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        # Compute the start time of this trial as a percentage of the whole session.
        percent_complete = 100 * trial.start_time / self.max_start_time
        trial.add_enhancement("percent_complete", percent_complete, "value")


class SignalNormalizer(TrialCollecter):
    """Adjust a signal in place for each trial, to normalize by max absolute value across the whole session."""

    def __init__(
        self,
        buffer_name: str,
        channel_id: str | int = None
    ) -> None:
        self.buffer_name = buffer_name
        self.channel_id = channel_id

        self.max_absolute_value = 0

    def collect(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict,
        subject_info: dict
    ) -> None:
        # Locate the named buffer in the current trial.
        signal = trial.signals.get(self.buffer_name, None)
        if signal is None:
            return

        # Locate the given or default signal channel (signals can have one or more column of data).
        if self.channel_id is None:
            channel_index = 0
        else:
            channel_index = signal.channel_ids.index(self.channel_id)

        # Get the max absoulute value of the signal in this trial.
        trial_max = np.absolute(signal.sample_data[:, channel_index]).max()

        # Collect the max value as a stat across the whole session.
        self.max_absolute_value = max(self.max_absolute_value, trial_max)

    def enhance(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        # Locate the named buffer in the current trial.
        signal = trial.signals.get(self.buffer_name, None)
        if signal is None:
            return

        # Locate the given or default signal channel (signals can have one or more columns of data).
        if self.channel_id is None:
            channel_index = 0
        else:
            channel_index = signal.channel_ids.index(self.channel_id)

        # Normalize the signal data in place, with respect to the whole session.
        signal.sample_data[:, channel_index] /= self.max_absolute_value
