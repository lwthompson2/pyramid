from types import TracebackType
from typing import Self
from collections import namedtuple

import numpy as np
from open_ephys.analysis import Session
from scipy.signal import butter
from pyramid.file_finder import FileFinder
from pyramid.model.model import BufferData
from pyramid.model.events import NumericEventList, TextEventList
from pyramid.model.signals import SignalChunk
from pyramid.neutral_zone.readers.readers import Reader
from pyramid.model.signals import SignalTimeChunk


class OpenEphysSession():
    """A shared util for accessing an Open Ephys session and recording."""

    def __init__(
        self,
        session_dir: str,
        record_node_index: int = -1,
        recording_index: int = -1,
    ) -> None:
        # Construct the top-level session reader, but don't try to access data yet.
        self.session = Session(session_dir)
        if record_node_index is None:
            self.record_node = None
            self.recording = self.session.recordings[recording_index]
        else:
            self.record_node = self.session.recordnodes[record_node_index]
            self.recording = self.record_node.recordings[recording_index]


class OpenEphysSessionSignalReader(Reader):
    """Read continuous signal data from an Open Ephys session.

    This is based on the Open Ephys Python Tools Analysis module:
    https://github.com/open-ephys/open-ephys-python-tools/blob/main/src/open_ephys/analysis/README.md

    Args:
        session_dir:        Directory for Open Ephys Python Tools to seach for saved data (Open Ephys Binary or NWB2 format)
        file_finder:        Utility to find() files in the conigured Pyramid configured search path.
                            Pyramid will automatically create and pass in the file_finder for you.
        stream_name:        Which Open Ephys data stream to select for reading (default None, take the first stream).
        channel_names:      List of channel names to keep within the selected data stream (default None, take all channels).
        record_node_index:  When session_dir contains record node subdirs, which node/dir to pick (default -1, the last one).
        recording_index:    When which recording to pick within a record node (default -1, the last one).
        result_name:        Name to use for the Pyramid SignalChunk results (default None, use the stream_name).
        samples_per_chunk:  How many signal samples (time steps across all channels) to take per read_next() (default 10000).
    """

    def __init__(
        self,
        session_dir: str,
        file_finder: FileFinder = FileFinder(),
        stream_name: str = None,
        channel_names: list[str] = None,
        record_node_index: int = -1,
        recording_index: int = -1,
        result_name: str = None,
        samples_per_chunk: int = 10000
    ) -> None:
        self.session = OpenEphysSession(file_finder.find(session_dir), record_node_index, recording_index)
        self.stream_name = stream_name
        self.channel_names = channel_names
        self.result_name = result_name
        self.samples_per_chunk = samples_per_chunk

        if self.stream_name is None:
            self.continuous = self.session.recording.continuous[0]
        else:
            # Pick a continuous data object by name.
            # Using recording.continuous runs "LOAD_CONTINUOUS" (NWBRECORDING.PY), which will load all continuous streams.
            datasets = list(self.session.recording.nwb["acquisition"].keys())
            for index, item in enumerate(datasets):
                if self.stream_name in item and "TTL" not in item:
                    self.continuous = self.session.recording.Continuous(self.session.recording.nwb, item)
                    break  # Stop at the first match

        # Pick a set of channels to keep, by name.
        if "channel_names" in self.continuous.metadata:
            all_names = self.continuous.metadata["channel_names"]
        else:
            # It seems the NWB format doesn't fill in explicit channel names.
            all_names = [f"CH{index+1}" for index in range(self.continuous.metadata['num_channels'])]
        if self.channel_names is None:
            # Default to all channels.
            self.channel_ids = all_names
            self.channel_indexes = list(range(self.continuous.metadata['num_channels']))
        else:
            self.channel_ids = self.channel_names
            self.channel_indexes = [all_names.index(name) for name in self.channel_names]

        # Default result buffer name is the name of the stream.
        if self.result_name is None:
            self.result_name = self.continuous.metadata["stream_name"]

        # Look ahead to know when to stop reading.
        self.total_samples = self.continuous.sample_numbers.size
        self.next_sample = None

    def __enter__(self) -> Self:
        self.next_sample = 0
        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        self.next_sample = None
        return None

    def get_initial(self) -> dict[str, BufferData]:
        return {
            self.result_name: SignalChunk.empty(
                sample_frequency=self.continuous.metadata['sample_rate'],
                channel_ids=self.channel_ids
            )
        }

    def read_next(self) -> dict[str, BufferData]:
        if self.next_sample >= self.total_samples:
            # The previous read exhausted the signal data -- all done.
            raise StopIteration

        # Read a full chunk, or up to the end of the signal.
        first_sample_time = self.continuous.timestamps[self.next_sample]
        samples = self.continuous.get_samples(
            self.next_sample,
            self.next_sample + self.samples_per_chunk,
            self.channel_indexes
        )
        self.next_sample += samples.shape[0]
        return {
            self.result_name: SignalChunk(
                sample_data=samples,
                sample_frequency=self.continuous.metadata['sample_rate'],
                first_sample_time=first_sample_time,
                channel_ids=self.channel_ids
            )
        }


class OpenEphysSessionNumericEventReader(Reader):
    """Read numeric aka "ttl" event data from an Open Ephys session.

    This is based on the Open Ephys Python Tools Analysis module:
    https://github.com/open-ephys/open-ephys-python-tools/blob/main/src/open_ephys/analysis/README.md

    Args:
        session_dir:        Directory for Open Ephys Python Tools to seach for saved data (Open Ephys Binary or NWB2 format)
        file_finder:        Utility to find() files in the conigured Pyramid configured search path.
                            Pyramid will automatically create and pass in the file_finder for you.
        stream_name:        Which Open Ephys data stream to select for reading (default None, take all streams).
        record_node_index:  When session_dir contains record node subdirs, which node/dir to pick (default -1, the last one).
        recording_index:    When which recording to pick within a record node (default -1, the last one).
        result_name:        Name to use for the Pyramid NumericEventList results (default None, use stream_name if provided or else "ttl").

        The numeric events produced will have event data like: [timestamp, line_number, line_state, processor_id]
    """

    def __init__(
        self,
        session_dir: str,
        file_finder: FileFinder = FileFinder(),
        stream_name: str = None,
        record_node_index: int = -1,
        recording_index: int = -1,
        result_name: str = None
    ) -> None:
        self.session = OpenEphysSession(file_finder.find(session_dir), record_node_index, recording_index)
        self.stream_name = stream_name

        if result_name is None:
            if stream_name is None:
                self.result_name = "ttl"
            else:
                self.result_name = stream_name
        else:
            self.result_name = result_name

        self.events_iterator = None

    def __enter__(self) -> Self:
        # Set up an iterator over the recording's ttl events.
        if self.stream_name:
            # Filter the events by matching on stream_name.
            self.events_iterator = (e for e in self.session.recording.events.itertuples()
                                    if e.stream_name == self.stream_name)
        else:
            # Take all events.
            self.events_iterator = self.session.recording.events.itertuples()

        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        self.events_iterator = None
        return None

    def get_initial(self) -> dict[str, BufferData]:
        return {
            # Events will be like [timestamp, line_number, line_state, processor_id]
            self.result_name: NumericEventList.empty(3)
        }

    def read_next(self) -> dict[str, BufferData]:
        """Read the next event, or throw StopIteration."""
        next = self.events_iterator.__next__()
        event_data = np.array([[next.timestamp, next.line, next.state, next.processor_id]])
        return {
            self.result_name: NumericEventList(event_data)
        }


class OpenEphysSessionTextEventReader(Reader):
    """Read text aka "messge center" data from an Open Ephys session.

    This is based on the Open Ephys Python Tools Analysis module:
    https://github.com/open-ephys/open-ephys-python-tools/blob/main/src/open_ephys/analysis/README.md

    Args:
        session_dir:        Directory for Open Ephys Python Tools to seach for saved data (Open Ephys Binary or NWB2 format)
        file_finder:        Utility to find() files in the conigured Pyramid configured search path.
                            Pyramid will automatically create and pass in the file_finder for you.
        record_node_index:  When session_dir contains record node subdirs, which node/dir to pick (default -1, the last one).
        recording_index:    When which recording to pick within a record node (default -1, the last one).
        result_name:        Name to use for the Pyramid TextEventList results (default "messages").
    """

    def __init__(
        self,
        session_dir: str,
        file_finder: FileFinder = FileFinder(),
        record_node_index: int = -1,
        recording_index: int = -1,
        result_name: str = "messages"
    ) -> None:
        self.session = OpenEphysSession(file_finder.find(session_dir), record_node_index, recording_index)
        self.result_name = result_name

        self.events_iterator = None

    def __enter__(self) -> Self:
        # Set up an iterator over the recording's text messages.
        if self.session.recording.format == "nwb":
            # As of May 2024 Open Ephys Tools NWB format doesn't parse message center events.
            # We can still access them in the underlying NWB data.
            Message = namedtuple("Message", ["timestamp", "message"])
            timestamps = self.session.recording.nwb['acquisition']['messages']['timestamps']
            text = self.session.recording.nwb['acquisition']['messages']['data']
            message_count = timestamps.size
            self.events_iterator = (Message(timestamps[index], text[index]) for index in range(message_count))
        else:
            self.events_iterator = self.session.recording.messages.itertuples()
        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        self.events_iterator = None
        return None

    def get_initial(self) -> dict[str, BufferData]:
        return {
            self.result_name: TextEventList.empty()
        }

    def read_next(self) -> dict[str, BufferData]:
        """Read the next event, or throw StopIteration."""
        next = self.events_iterator.__next__()
        timestamp_data = np.array([next.timestamp])
        text_data = np.array([next.message], dtype=np.str_)
        return {
            self.result_name: TextEventList(timestamp_data, text_data)
        }

class OpenEphysSessionTimeSeriesReader(Reader):
    """Read continuous time series data from an Open Ephys session, returning all sample timestamps in each chunk as NumericEventList, with optional Butterworth filtering."""

    def __init__(
        self,
        session_dir: str,
        file_finder: FileFinder = FileFinder(),
        stream_name: str = None,
        channel_names: list[str] = None,
        record_node_index: int = -1,
        recording_index: int = -1,
        result_name: str = None,
        samples_per_chunk: int = 10000,
        low_cut: float = None,
        high_cut: float = None,
        filter_order: int = 4
    ) -> None:
        self.session = OpenEphysSession(file_finder.find(session_dir), record_node_index, recording_index)
        self.stream_name = stream_name
        self.channel_names = channel_names
        self.result_name = result_name
        self.samples_per_chunk = samples_per_chunk
        self.low_cut = low_cut
        self.high_cut = high_cut
        self.filter_order = filter_order
        self._filter_b = None
        self._filter_a = None

        if self.stream_name is None:
            self.continuous = self.session.recording.continuous[0]
        else:
            if self.session.recording.format == "nwb":
                datasets = list(self.session.recording.nwb["acquisition"].keys())
                for item in datasets:
                    if self.stream_name in item and "TTL" not in item:
                        self.continuous = self.session.recording.Continuous(self.session.recording.nwb, item)
                        break
            elif self.session.recording.format == "binary":
                for item in self.session.recording.continuous:
                    if item.metadata["stream_name"] == self.stream_name:
                        self.continuous = item
                        break
            
        if "channel_names" in self.continuous.metadata:
            all_names = self.continuous.metadata["channel_names"]
        else:
            all_names = [f"CH{index+1}" for index in range(self.continuous.metadata['num_channels'])]
        if self.channel_names is None:
            self.channel_ids = all_names
            self.channel_indexes = list(range(self.continuous.metadata['num_channels']))
        else:
            self.channel_ids = self.channel_names
            self.channel_indexes = [all_names.index(name) for name in self.channel_names]

        if self.result_name is None:
            self.result_name = self.continuous.metadata["stream_name"]

        self.total_samples = self.continuous.sample_numbers.size
        self.next_sample = None

        # Setup filter if needed
        if self.low_cut is not None or self.high_cut is not None:
            nyq = 0.5 * self.continuous.metadata['sample_rate']
            wp = []
            if self.low_cut is not None:
                wp.append(self.low_cut / nyq)
            else:
                wp.append(0)
            if self.high_cut is not None:
                wp.append(self.high_cut / nyq)
            else:
                wp.append(1)
            if self.low_cut is not None and self.high_cut is not None:
                btype = 'bandpass'
            elif self.low_cut is not None:
                btype = 'highpass'
            else:
                btype = 'lowpass'
            self._filter_b, self._filter_a = butter(self.filter_order, wp, btype=btype)

    def __enter__(self) -> Self:
        self.next_sample = 0
        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        self.next_sample = None
        return None

    def get_initial(self) -> dict[str, BufferData]:
        return {
            self.result_name: SignalTimeChunk.empty(channel_ids=self.channel_ids, sample_frequency=self.continuous.metadata['sample_rate'])
        }

    def read_next(self) -> dict[str, BufferData]:
        if self.next_sample >= self.total_samples:
            raise StopIteration

        end_sample = min(self.next_sample + self.samples_per_chunk, self.total_samples)
        chunk_timestamps = self.continuous.timestamps[self.next_sample:end_sample]
        samples = self.continuous.get_samples(self.next_sample, end_sample, self.channel_indexes)

        # Apply Butterworth filter if specified
        if self._filter_b is not None and self._filter_a is not None:
            from scipy.signal import filtfilt
            for i in range(samples.shape[1]):
                samples[:, i] = filtfilt(self._filter_b, self._filter_a, samples[:, i])

        self.next_sample += samples.shape[0]

        return {
            self.result_name: SignalTimeChunk(
                sample_data=samples,
                timestamps=chunk_timestamps,
                sample_frequency=self.continuous.metadata['sample_rate'],
                channel_ids=self.channel_ids
            )
        }