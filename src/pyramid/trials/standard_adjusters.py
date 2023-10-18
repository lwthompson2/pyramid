from typing import Any

import numpy as np

from pyramid.trials.trials import Trial, TrialEnhancer


class SignalSmoother(TrialEnhancer):
    """Adjust a signal in place for each trial, to smooth it out."""

    def __init__(
        self,
        buffer_name: str,
        channel_id: str | int = None,
        kernel_size: int = 10
    ) -> None:
        self.buffer_name = buffer_name
        self.channel_id = channel_id

        # Make a simple, uniform kernel to smooth the data.
        self.kernel = np.ones(kernel_size) / kernel_size

    def enhance(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        # Locate the named buffer in the current trial.
        signal = trial.signals.get(self.buffer_name, None)
        if signal is None or signal.sample_count() < self.kernel.size:
            return

        # Locate the given or default signal channel (signals can have one or more columns of data).
        if self.channel_id is None:
            channel_index = 0
        else:
            channel_index = signal.channel_ids.index(self.channel_id)

        # Smooth the signal data in place, keeping it's size the "same".
        signal.sample_data[:, channel_index] = np.convolve(
            signal.sample_data[:, channel_index],
            self.kernel,
            "same"
        )
