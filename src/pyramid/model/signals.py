from typing import Any, Self
from dataclasses import dataclass
import numpy as np

from pyramid.model.model import BufferData


@dataclass
class SignalChunk(BufferData):
    """Wrap a 2D array with a chunk of signal data where rows are samples and columns are channels."""

    sample_data: np.ndarray
    """2D array backing the signal chunk.

    signal_data must have shape (n, m) where:
     - n is the number of samples (evenly spaced in time)
     - m is the number of channels
    """

    sample_frequency: float
    """Frequency in Hz of the samples in signal_data."""

    first_sample_time: float
    """Time in seconds of the first sample in signal_data."""

    channel_ids: list[str | int]
    """Identifiers for the channels represented in this signal chunk.
    
    channel_ids should have m elements, where m is the number of columns in signal_data.
    """

    def __eq__(self, other: object) -> bool:
        """Compare signal_data arrays as-a-whole instead of element-wise."""
        if isinstance(other, self.__class__):
            arrays_equal = (
                (self.sample_data.size == 0 and other.sample_data.size == 0)
                or np.array_equal(self.sample_data, other.sample_data)
            )
            return (
                arrays_equal
                and self.sample_frequency == other.sample_frequency
                and self.first_sample_time == other.first_sample_time
                and self.channel_ids == other.channel_ids
            )
        else:
            return False

    def copy(self) -> Self:
        """Implementing BufferData superclass."""
        return SignalChunk(
            self.sample_data.copy(),
            self.sample_frequency,
            self.first_sample_time,
            self.channel_ids
        )

    def copy_time_range(self, start_time: float = None, end_time: float = None) -> Self:
        """Implementing BufferData superclass."""
        sample_times = self.get_times()
        if start_time is None:
            tail_selector = True
        else:
            tail_selector = sample_times >= start_time

        if end_time is None:
            head_selector = True
        elif end_time == start_time:
            head_selector = [False]*sample_times.size
            head_selector[next((i for i, x in enumerate(tail_selector) if x), None)] = True
        else:
            head_selector = sample_times < end_time

        rows_in_range = tail_selector & head_selector

        range_sample_data = self.sample_data[rows_in_range, :]
        if range_sample_data.size > 0:
            range_first_sample_time = sample_times[rows_in_range][0]
        else:
            range_first_sample_time = None

        return SignalChunk(
            range_sample_data,
            self.sample_frequency,
            range_first_sample_time,
            self.channel_ids
        )

    def append(self, other: Self) -> None:
        """Implementing BufferData superclass."""
        self.sample_data = np.concatenate([self.sample_data, other.sample_data])

        if self.sample_frequency is None:
            self.sample_frequency = other.sample_frequency

        if self.first_sample_time is None:
            self.first_sample_time = other.first_sample_time

    def discard_before(self, start_time: float) -> None:
        """Implementing BufferData superclass."""
        sample_times = self.get_times()
        rows_to_keep = sample_times >= start_time
        self.sample_data = self.sample_data[rows_to_keep, :]
        if self.sample_data.size > 0:
            self.first_sample_time = sample_times[rows_to_keep][0]
        else:
            self.first_sample_time = None

    def shift_times(self, shift: float) -> None:
        """Implementing BufferData superclass."""
        if self.first_sample_time is not None:
            self.first_sample_time += shift

    def get_end_time(self) -> float:
        """Implementing BufferData superclass."""
        sample_count = self.sample_count()
        if sample_count > 0:
            duration = (self.sample_count() - 1) / self.sample_frequency
            return self.first_sample_time + duration
        else:
            return None

    def apply_offset_then_gain(self, offset: float = 0, gain: float = 1, channel_id: str | int = None) -> None:
        """Transform sample data by a constant gain and offset.

        Uses a convention of applying offset first, then gain.

        By default this modifies samples on all channels.
        Pass in a channel_id to select one specific channel.

        This modifies the signal_data in place.
        """
        if channel_id is None:
            channel_index = True
        else:
            channel_index = self.channel_ids.index(channel_id)

        self.sample_data[:, channel_index] += offset
        self.sample_data[:, channel_index] *= gain

    def sample_count(self) -> int:
        """Get the number of samples in the chunk."""
        return self.sample_data.shape[0]

    def channel_count(self) -> int:
        """Get the number of channels in the chunk."""
        return self.sample_data.shape[1]

    def get_times(self) -> np.ndarray:
        """Get all the sample times, ignoring channel values."""
        sample_indexes = np.array(range(self.sample_count()))
        sample_offsets = sample_indexes / self.sample_frequency
        sample_times = self.first_sample_time + sample_offsets
        return sample_times

    def get_channel_values(self, channel_id: str | int = None) -> np.ndarray:
        """Get sample values from one channel, by id.
        """
        if channel_id is None:
            channel_id = self.channel_ids[0]
        channel_index = self.channel_ids.index(channel_id)
        return self.sample_data[:, channel_index]
