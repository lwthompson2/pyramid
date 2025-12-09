@dataclass
class SignalTimeChunk(BufferData):
    """Wrap a 2D array with a chunk of signal data where rows are samples and columns are channels, with explicit timestamps for each sample."""

    sample_data: np.ndarray
    """2D array backing the signal chunk. Shape (n, m): n samples, m channels."""

    timestamps: np.ndarray
    """1D array of timestamps for each sample (length n)."""

    sample_frequency: float = None
    """Frequency in Hz of the samples (optional, for downstream use)."""

    channel_ids: list[str | int]
    """Identifiers for the channels represented in this signal chunk."""

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            arrays_equal = (
                (self.sample_data.size == 0 and other.sample_data.size == 0)
                or np.array_equal(self.sample_data, other.sample_data)
            )
            return arrays_equal and np.array_equal(self.timestamps, other.timestamps) and self.channel_ids == other.channel_ids
        else:
            return False

    @classmethod
    def empty(cls, channel_ids: list[str | int] = None, sample_frequency: float = None, dtype=np.float64) -> Self:
        if channel_ids is None:
            num_channels = 0
        else:
            num_channels = len(channel_ids)
        return SignalTimeChunk(
            np.empty([0, num_channels], dtype=dtype),
            np.empty([0], dtype=np.float64),
            sample_frequency,
            channel_ids
        )

    def copy(self) -> Self:
        return SignalTimeChunk(
            self.sample_data.copy(),
            self.timestamps.copy(),
            self.sample_frequency,
            self.channel_ids.copy() if self.channel_ids is not None else None
        )

    def get_time_selector(self, start_time: float = None, end_time: float = None):
        if start_time is None:
            tail_selector = np.ones(self.timestamps.shape, dtype=bool)
        else:
            tail_selector = self.timestamps >= start_time
        if end_time is None:
            head_selector = np.ones(self.timestamps.shape, dtype=bool)
        else:
            head_selector = self.timestamps < end_time
        return (self.timestamps, tail_selector & head_selector)

    def copy_time_range(self, start_time: float = None, end_time: float = None) -> Self:
        (sample_times, rows_in_range) = self.get_time_selector(start_time, end_time)
        range_sample_data = self.sample_data[rows_in_range, :]
        range_timestamps = self.timestamps[rows_in_range]
        return SignalTimeChunk(
            range_sample_data,
            range_timestamps,
            self.sample_frequency,
            self.channel_ids
        )

    def append(self, other: Self) -> None:
        self.sample_data = np.concatenate([self.sample_data, other.sample_data])
        self.timestamps = np.concatenate([self.timestamps, other.timestamps])

    def discard_before(self, start_time: float) -> None:
        rows_to_keep = self.timestamps >= start_time
        self.sample_data = self.sample_data[rows_to_keep, :]
        self.timestamps = self.timestamps[rows_to_keep]

    def shift_times(self, shift: float) -> None:
        self.timestamps += shift

    def start(self) -> float:
        if self.timestamps.size > 0:
            return self.timestamps[0]
        else:
            return None

    def end(self) -> float:
        if self.timestamps.size > 0:
            return self.timestamps[-1]
        else:
            return None

    def times(self, value: Any = None, value_index: int = 0, start_time: float = None, end_time: float = None) -> np.ndarray:
        (sample_times, rows_in_range) = self.get_time_selector(start_time, end_time)
        if value is None:
            matching_rows = np.ones(self.sample_data.shape[0], dtype=bool)
        else:
            matching_rows = (self.sample_data[:, value_index] == value)
        return sample_times[rows_in_range & matching_rows]

    def sample_count(self) -> int:
        return self.sample_data.shape[0]

    def channel_count(self) -> int:
        return self.sample_data.shape[1]

    def channel_index(self, channel_id: str | int = None) -> int:
        return self.channel_ids.index(channel_id)

    def first(self, value_index: int = 0):
        if self.sample_count() > 0:
            return self.sample_data[0, value_index]
        else:
            return None

    def last(self, value_index: int = 0):
        if self.sample_count() > 0:
            return self.sample_data[-1, value_index]
        else:
            return None

    def values(self, value_index: int = 0, start_time: float = None, end_time: float = None) -> np.ndarray:
        if start_time is None and end_time is None:
            return self.sample_data[:, value_index]
        else:
            (_, rows_in_range) = self.get_time_selector(start_time, end_time)
            return self.sample_data[rows_in_range, value_index]

    def at(self, time: float = 0.0, value_index: int = 0) -> Any:
        # Find the closest timestamp
        if self.timestamps.size == 0:
            return None
        idx = np.searchsorted(self.timestamps, time)
        if idx == 0:
            return self.sample_data[0, value_index]
        elif idx < self.sample_count():
            return self.sample_data[idx, value_index]
        return None

    def each(self) -> Iterator[tuple[float, list[float]]]:
        return ((time, self.sample_data[index, :]) for index, time in enumerate(self.timestamps))
    
    
from typing import Any, Self, Iterator
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

    @classmethod
    def empty(
        cls,
        sample_frequency: float = None,
        first_sample_time: float = None,
        channel_ids: list[str | int] = None,
        dtype=np.float64
    ) -> Self:
        """Convenience for creating an empty signal chunk with given params and data type."""
        if channel_ids is None:
            num_channels = 0
        else:
            num_channels = len(channel_ids)
        return SignalChunk(
            np.empty([0, num_channels], dtype=dtype),
            sample_frequency,
            first_sample_time,
            channel_ids
        )

    def copy(self) -> Self:
        """Implementing BufferData superclass."""
        return SignalChunk(
            self.sample_data.copy(),
            self.sample_frequency,
            self.first_sample_time,
            self.channel_ids
        )

    def compute_sample_times(self) -> np.ndarray:
        sample_indexes = np.array(range(self.sample_count()))
        sample_offsets = sample_indexes / self.sample_frequency
        sample_times = self.first_sample_time + sample_offsets
        return sample_times

    def get_time_selector(self, start_time: float = None, end_time: float = None):
        sample_times = self.compute_sample_times()
        if start_time is None:
            tail_selector = True
        else:
            tail_selector = sample_times >= start_time

        if end_time is None:
            head_selector = True
        else:
            head_selector = sample_times < end_time

        return (sample_times, tail_selector & head_selector)

    def copy_time_range(self, start_time: float = None, end_time: float = None) -> Self:
        """Implementing BufferData superclass."""
        (sample_times, rows_in_range) = self.get_time_selector(start_time, end_time)

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
        (sample_times, rows_to_keep) = self.get_time_selector(start_time=start_time)
        self.sample_data = self.sample_data[rows_to_keep, :]
        if self.sample_data.size > 0:
            self.first_sample_time = sample_times[rows_to_keep][0]
        else:
            self.first_sample_time = None

    def shift_times(self, shift: float) -> None:
        """Implementing BufferData superclass."""
        if self.first_sample_time is not None:
            self.first_sample_time += shift

    def start(self) -> float:
        """Get the time of the first data item still in the buffer."""
        if self.sample_count() > 0:
            return self.first_sample_time
        else:
            return None

    def end(self) -> float:
        """Implementing BufferData superclass."""
        sample_count = self.sample_count()
        if sample_count > 0:
            duration = (self.sample_count() - 1) / self.sample_frequency
            return self.first_sample_time + duration
        else:
            return None

    def times(
        self,
        value: Any = None,
        value_index: int = 0,
        start_time: float = None,
        end_time: float = None
    ) -> np.ndarray:
        """Implementing BufferData superclass.

        This searches the value_index-th channel for exact occurrences of the given value.
        value_index should be a raw index into the data, not a string or other channel_id.
        """
        (_, rows_in_range) = self.get_time_selector(start_time, end_time)
        if value is None:
            matching_rows = np.ones((self.sample_count(),), dtype=np.bool_)
        else:
            matching_rows = (self.sample_data[:, value_index] == value)
        sample_indexes = np.nonzero(rows_in_range & matching_rows)[0]
        sample_offsets = sample_indexes / self.sample_frequency
        sample_times = self.first_sample_time + sample_offsets
        return sample_times

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
            channel_index = self.channel_index(channel_id)

        self.sample_data[:, channel_index] += offset
        self.sample_data[:, channel_index] *= gain

    def sample_count(self) -> int:
        """Get the number of samples in the chunk."""
        return self.sample_data.shape[0]

    def channel_count(self) -> int:
        """Get the number of channels in the chunk."""
        return self.sample_data.shape[1]

    def channel_index(self, channel_id: str | int = None) -> np.ndarray:
        """Get the raw index of a channel from its string or number id."""
        return self.channel_ids.index(channel_id)

    def first(self, value_index: int = 0):
        """Implementing BufferData superclass.

        value_index should be a raw index into the data, not a string or other channel_id.
        """
        if self.sample_count() > 0:
            return self.sample_data[0, value_index]
        else:
            return None

    def last(self, value_index: int = 0):
        """Implementing BufferData superclass.

        value_index should be a raw index into the data, not a string or other channel_id.
        """
        if self.sample_count() > 0:
            return self.sample_data[-1, value_index]
        else:
            return None

    def values(
        self,
        value_index: int = 0,
        start_time: float = None,
        end_time: float = None
    ) -> np.ndarray:
        """Implementing BufferData superclass.

        value_index should be a raw index into the data, not a string or other channel_id.
        """
        if start_time is None and end_time is None:
            return self.sample_data[:, value_index]
        else:
            (_, rows_in_range) = self.get_time_selector(start_time, end_time)
            return self.sample_data[rows_in_range, value_index]

    def at(
        self,
        time: float = 0.0,
        value_index: int = 0,
    ) -> Any:
        """Implementing BufferData superclass.

        value_index should be a raw index into the data, not a string or other channel_id.
        """
        sample_index = int((time - self.first_sample_time) * self.sample_frequency)
        if sample_index < 0:
            return self.sample_data[0, value_index]
        elif sample_index < self.sample_count():
            return self.sample_data[sample_index, value_index]
        return None

    def each(self) -> Iterator[tuple[float, list[float]]]:
        """Implementing BufferData superclass."""
        return ((time, self.sample_data[index, :]) for index, time in enumerate(self.times()))
