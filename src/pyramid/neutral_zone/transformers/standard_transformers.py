import logging

import numpy as np

from pyramid.model.model import BufferData
from pyramid.model.events import NumericEventList, TextEventList
from pyramid.model.signals import SignalChunk, SignalTimeChunk
from pyramid.neutral_zone.transformers.transformers import Transformer


class OffsetThenGain(Transformer):
    """Apply an offset, then gain, to values in a Pyramid BufferData type."""

    def __init__(self, offset: float = 0.0, gain: float = 1.0, value_id: int | str = 0, **kwargs) -> None:
        self.offset = offset
        self.gain = gain
        self.value_id = value_id

    def __eq__(self, other: object) -> bool:
        """Compare transformers field-wise, to support use of this class in tests."""
        if isinstance(other, self.__class__):
            return (
                self.offset == other.offset
                and self.gain == other.gain
                and self.value_id == other.value_id
            )
        else:  # pragma: no cover
            return False

    def transform(self, data: BufferData) -> BufferData:
        if isinstance(data, NumericEventList):
            data.apply_offset_then_gain(self.offset, self.gain, self.value_id)
        elif isinstance(data, (SignalChunk, SignalTimeChunk)):
            data.apply_offset_then_gain(self.offset, self.gain)
        else:  # pragma: no cover
            logging.warning(f"OffsetThenGain doesn't know how to apply to {data.__class__.__name__}")
        return data


class FilterRange(Transformer):
    """Filter values, taking only those in the half open interval [min, max), from a Pyramid type like NumericEventList."""

    def __init__(self, min: float = None, max: float = None, value_index: int = 0, **kwargs) -> None:
        self.min = min
        self.max = max
        self.value_index = value_index

    def transform(self, data: BufferData) -> BufferData:
        if isinstance(data, NumericEventList):
            return data.copy_value_range(self.min, self.max, self.value_index)
        else:  # pragma: no cover
            logging.warning(f"FilterRange doesn't know how to apply to {data.__class__.__name__}")
            return data


class SmashCase(Transformer):
    """Filter text event values, transforming them to all UPPER or all lower case."""

    def __init__(self, upper_case: bool = True, **kwargs) -> None:
        self.upper_case = upper_case

    def transform(self, data: BufferData):
        if isinstance(data, TextEventList):
            if (self.upper_case):
                return TextEventList(data.timestamp_data, np.char.upper(data.text_data))
            else:
                return TextEventList(data.timestamp_data, np.char.lower(data.text_data))
        else:
            logging.warning(f"SmashCase doesn't know how to apply to {data.__class__.__name__}")
            return data


class SparseSignal(Transformer):
    """Convert incoming numeric event lists into continuous signal chunks.

    Gaps between samples will be filled with the given fill_with constant,
    or interpolated if this is None (default).

    Output signal chunks will all use the given sample_frequency and channel_ids.
    """

    def __init__(
        self,
        fill_with: float = None,
        sample_frequency: float = None,
        channel_ids: list[str | int] = None,
        **kwargs
    ) -> None:
        self.fill_with = fill_with
        self.sample_frequency = sample_frequency
        self.channel_ids = channel_ids

        self.sample_interval = 1 / sample_frequency
        self.last_sample_time = None
        self.last_sample_value = None

    def transform(self, data: BufferData):
        if not isinstance(data, NumericEventList):
            logging.warning(f"SparseSignal doesn't know how to apply to {data.__class__.__name__}")
            return data

        dtype = data.event_data.dtype
        if data.event_count() < 1:
            return SignalChunk.empty(sample_frequency=self.sample_frequency, channel_ids=self.channel_ids, dtype=dtype)

        # Figure out where to start this new signal chunk.
        # Account for any gap since the last call to transform().
        event_times = data.times()
        first_event_time = event_times.min()
        if self.last_sample_time is None:
            new_chunk_start_time = first_event_time
        else:
            new_chunk_start_time = self.last_sample_time + self.sample_interval

        # Build a signal chunk at the nominal frequency and deal in new event data wherever they fit.
        new_offsets = np.uint64((event_times - new_chunk_start_time) * self.sample_frequency)
        new_count = new_offsets.max() + np.uint64(1)
        if self.fill_with is None:
            complete_times = np.arange(new_count, dtype=np.float64) * self.sample_interval + new_chunk_start_time
            if self.last_sample_time is None:
                known_times = event_times
            else:
                known_times = np.concatenate([[self.last_sample_time], event_times])
            new_data = np.zeros([new_count, data.values_per_event()], dtype=dtype)
            for value_index in range(data.values_per_event()):
                event_values = data.values(value_index=value_index)
                if self.last_sample_value is None:
                    known_values = event_values
                else:
                    known_values = np.concatenate([[self.last_sample_value[value_index]], event_values])
                new_data[:, value_index] = np.interp(complete_times, known_times, known_values)
        else:
            new_data = np.full([new_count, data.values_per_event()], self.fill_with, dtype=dtype)
            new_data[new_offsets] = data.event_data[:, 1:]

        new_chunk = SignalChunk(
            new_data,
            sample_frequency=self.sample_frequency,
            first_sample_time=new_chunk_start_time,
            channel_ids=self.channel_ids,
        )
        self.last_sample_time = new_chunk.end()
        self.last_sample_value = new_chunk.sample_data[-1,:]
        return new_chunk
