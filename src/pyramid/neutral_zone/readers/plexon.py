import logging
from types import TracebackType
from typing import ContextManager, Self, Any
from pathlib import Path

import numpy as np

from pyramid.file_finder import FileFinder
from pyramid.model.model import BufferData
from pyramid.model.events import NumericEventList
from pyramid.model.signals import SignalChunk
from pyramid.neutral_zone.readers.readers import Reader


# Representing Plexon file from C headers available here:
# http://www.plexon.com/software-downloads

GlobalHeader = np.dtype(
    [
        ('MagicNumber', 'uint32'),
        ('Version', 'int32'),
        ('Comment', 'S128'),
        ('ADFrequency', 'int32'),
        ('NumDSPChannels', 'int32'),
        ('NumEventChannels', 'int32'),
        ('NumSlowChannels', 'int32'),
        ('NumPointsWave', 'int32'),
        ('NumPointsPreThr', 'int32'),
        ('Year', 'int32'),
        ('Month', 'int32'),
        ('Day', 'int32'),
        ('Hour', 'int32'),
        ('Minute', 'int32'),
        ('Second', 'int32'),
        ('FastRead', 'int32'),
        ('WaveformFreq', 'int32'),
        ('LastTimestamp', 'float64'),

        # version >103
        ('Trodalness', 'uint8'),
        ('DataTrodalness', 'uint8'),
        ('BitsPerSpikeSample', 'uint8'),
        ('BitsPerSlowSample', 'uint8'),
        ('SpikeMaxMagnitudeMV', 'uint16'),
        ('SlowMaxMagnitudeMV', 'uint16'),

        # version 105
        ('SpikePreAmpGain', 'uint16'),

        # version 106
        ('AcquiringSoftware', 'S18'),
        ('ProcessingSoftware', 'S18'),

        ('Padding', 'S10'),

        # all version
        ('TSCounts', 'int32', (130, 5)),  # number of timestamps[channel][unit]
        ('WFCounts', 'int32', (130, 5)),  # number of waveforms[channel][unit]
        ('EVCounts', 'int32', (512,)),
    ]
)

DspChannelHeader = np.dtype(
    [
        ('Name', 'S32'),
        ('SIGName', 'S32'),
        ('Channel', 'int32'),
        ('WFRate', 'int32'),
        ('SIG', 'int32'),
        ('Ref', 'int32'),
        ('Gain', 'int32'),
        ('Filter', 'int32'),
        ('Threshold', 'int32'),
        ('Method', 'int32'),
        ('NUnits', 'int32'),
        ('Template', 'uint16', (5, 64)),
        ('Fit', 'int32', (5,)),
        ('SortWidth', 'int32'),
        ('Boxes', 'uint16', (5, 2, 4)),
        ('SortBeg', 'int32'),
        # version 105
        ('Comment', 'S128'),
        # version 106
        ('SrcId', 'uint8'),
        ('reserved', 'uint8'),
        ('ChanId', 'uint16'),

        ('Padding', 'int32', (10,)),
    ]
)

EventChannelHeader = np.dtype(
    [
        ('Name', 'S32'),
        ('Channel', 'int32'),
        # version 105
        ('Comment', 'S128'),
        # version 106
        ('SrcId', 'uint8'),
        ('reserved', 'uint8'),
        ('ChanId', 'uint16'),

        ('Padding', 'int32', (32,)),
    ]
)

SlowChannelHeader = np.dtype(
    [
        ('Name', 'S32'),
        ('Channel', 'int32'),
        ('ADFreq', 'int32'),
        ('Gain', 'int32'),
        ('Enabled', 'int32'),
        ('PreampGain', 'int32'),
        # version 104
        ('SpikeChannel', 'int32'),
        # version 105
        ('Comment', 'S128'),
        # version 106
        ('SrcId', 'uint8'),
        ('reserved', 'uint8'),
        ('ChanId', 'uint16'),

        ('Padding', 'int32', (27,)),
    ]
)

DataBlockHeader = np.dtype(
    [
        ('Type', 'uint16'),
        ('UpperByteOf5ByteTimestamp', 'uint16'),
        ('TimeStamp', 'int32'),
        ('Channel', 'uint16'),
        ('Unit', 'uint16'),
        ('NumberOfWaveforms', 'uint16'),
        ('NumberOfWordsInWaveform', 'uint16'),
    ]
)


class PlexonPlxRawReader(ContextManager):
    """Read a Pleoxn .plx file sequentially, block by block.

    This borrows from the python-neo project's PlexonRawIO.
    https://github.com/NeuralEnsemble/python-neo/blob/master/neo/rawio/plexonrawio.py

    The reason we don't just use PlexonRawIO here is we want to move through the file sequentially,
    block by block, over time. PlexonRawIO takes a different approach of indexing the whole file
    ahead of time and presenting a view per data type and channel, rather than sequentially.

    Thanks to the neo author Samuel Garcia for implementing a .plx file model in pure Python!
    """

    def __init__(self, plx_file: str) -> None:
        self.plx_file = plx_file

        self.plx_stream = None
        self.block_count = 0
        self.global_header = None

        self.dsp_channel_headers = None
        self.gain_per_dsp_channel = None
        self.dsp_frequency = None
        self.timestamp_frequency = None

        self.event_channel_headers = None

        self.slow_channel_headers = None
        self.gain_per_slow_channel = None
        self.frequency_per_slow_channel = None

    def __enter__(self) -> Self:
        self.plx_stream = open(self.plx_file, 'br')

        self.global_header = self.consume_type_as_dict(GlobalHeader)

        # DSP aka "spike" aka "waveform" channel configuration.
        self.dsp_channel_headers = [
            self.consume_type_as_dict(DspChannelHeader)
            for _ in range(self.global_header["NumDSPChannels"])
        ]
        self.gain_per_dsp_channel = self.get_gain_per_dsp_channel()
        self.dsp_frequency = self.global_header["WaveformFreq"]
        self.timestamp_frequency = self.global_header["ADFrequency"]

        # Event channel configuration.
        self.event_channel_headers = [
            self.consume_type_as_dict(EventChannelHeader)
            for _ in range(self.global_header["NumEventChannels"])
        ]

        # Slow, aka "ad", aka "analog" channel configuration.
        self.slow_channel_headers = [
            self.consume_type_as_dict(SlowChannelHeader)
            for _ in range(self.global_header["NumSlowChannels"])
        ]
        self.gain_per_slow_channel = self.get_gain_per_slow_channel()
        self.frequency_per_slow_channel = self.get_frequency_per_slow_channel()

        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        if self.plx_stream:
            self.plx_stream.close()
        self.plx_stream = None

    def consume_type(self, dtype: np.dtype) -> np.ndarray:
        """Consume part of the file, using the given dtype to choose the data size and format."""
        bytes = self.plx_stream.read(dtype.itemsize)
        if not bytes:
            return None
        return np.frombuffer(bytes, dtype)[0]

    def consume_type_as_dict(self, dtype: np.dtype) -> dict[str, Any]:
        """Consume part of the file using the given dtype, return a friendly dict-version of the data."""
        item = self.consume_type(dtype)
        if item is None:  # pragma: no cover
            return None

        result = {}
        for name in dtype.names:
            value = item[name]
            if dtype[name].kind == 'S':
                value = value.decode('utf8')
                value = value.replace('\x03', '')
                value = value.replace('\x00', '')
            result[name] = value
        return result

    def get_gain_per_slow_channel(self) -> dict[int, float]:
        """Compute ad channel gain -- thanks to python-neo and Samuel Garcia!"""
        gains = {}
        for header in self.slow_channel_headers:
            # We don't currently have test files at versions < 103
            if self.global_header['Version'] in [100, 101]:  # pragma: no cover
                gain = 5000. / (2048 * header['Gain'] * 1000.)
            elif self.global_header['Version'] in [102]:  # pragma: no cover
                gain = 5000. / (2048 * header['Gain'] * header['PreampGain'])
            elif self.global_header['Version'] >= 103:
                gain = self.global_header['SlowMaxMagnitudeMV'] / (
                    .5 * (2 ** self.global_header['BitsPerSlowSample']) *
                    header['Gain'] * header['PreampGain'])
            gains[header['Channel']] = gain
        return gains

    def get_frequency_per_slow_channel(self) -> dict[int, float]:
        frequencies = {}
        for header in self.slow_channel_headers:
            frequencies[header['Channel']] = header['ADFreq']
        return frequencies

    def get_gain_per_dsp_channel(self) -> dict[int, float]:
        """Compute spike channel gain -- thanks to python-neo and Samuel Garcia!"""
        gains = {}
        for header in self.dsp_channel_headers:
            # We don't currently have test files at versions < 103
            if self.global_header['Version'] < 103:  # pragma: no cover
                gain = 3000. / (2048 * header['Gain'] * 1000.)
            elif 103 <= self.global_header['Version'] < 105:  # pragma: no cover
                gain = self.global_header['SpikeMaxMagnitudeMV'] / (
                    .5 * 2. ** (self.global_header['BitsPerSpikeSample']) *
                    header['Gain'] * 1000.)
            elif self.global_header['Version'] >= 105:
                gain = self.global_header['SpikeMaxMagnitudeMV'] / (
                    .5 * 2. ** (self.global_header['BitsPerSpikeSample']) *
                    header['Gain'] * self.global_header['SpikePreAmpGain'])
            gains[header['Channel']] = gain
        return gains

    #@profile
    def next_block(self) -> dict[str, Any]:
        """Consume the next block header and any waveform data, as a friendly dict."""
        block_header = self.consume_type(DataBlockHeader)
        if not block_header:
            return None

        self.block_count += 1

        file_offset = self.plx_stream.tell()
        timestamp = int(block_header['UpperByteOf5ByteTimestamp']) * 2 ** 32 + int(block_header['TimeStamp'])
        block_type = block_header['Type']
        if block_type == 4:
            # An event value with no waveform payload.
            return self.block_event_data(block_header, block_type, timestamp, file_offset)
        elif block_type == 1:
            # A spike event with a waveform payload.
            return self.block_dsp_data(block_header, block_type, timestamp, file_offset)
        elif block_type == 5:
            # A slow channel update with a waveform payload.
            return self.block_slow_data(block_header, block_type, timestamp, file_offset)
        else:  # pragma: no cover
            logging.warning(f"Skipping block of unknown type {block_type}.  Block header is: {block_header}")
            return None

    #@profile
    def block_event_data(
        self,
        block_header: np.ndarray,
        block_type: int,
        timestamp: float,
        file_offset: int
    ) -> dict[str, Any]:
        return {
            "type": block_type,
            "file_offset": file_offset,
            "timestamp": timestamp,
            "timestamp_seconds": timestamp / self.timestamp_frequency,
            "channel": block_header['Channel'],
            "unit": block_header['Unit'],
        }

    #@profile
    def consume_block_waveforms(self, block_header: np.ndarray) -> np.ndarray:
        n = int(block_header["NumberOfWaveforms"])
        m = int(block_header["NumberOfWordsInWaveform"])
        bytes = self.plx_stream.read(n * m * 2)
        waveforms = np.frombuffer(bytes, dtype='int16')
        waveforms.reshape([n, m])
        return waveforms

    #@profile
    def block_dsp_data(
        self,
        block_header: np.ndarray,
        block_type: int,
        timestamp: float,
        file_offset: int
    ) -> dict[str, Any]:
        waveforms = self.consume_block_waveforms(block_header)
        channel = block_header['Channel']
        gain = self.gain_per_dsp_channel[channel]
        return {
            "type": block_type,
            "file_offset": file_offset,
            "timestamp": timestamp,
            "timestamp_seconds": timestamp / self.timestamp_frequency,
            "channel": channel,
            "unit": block_header['Unit'],
            "frequency": self.dsp_frequency,
            "waveforms": waveforms * gain
        }

    #@profile
    def block_slow_data(
        self,
        block_header: np.ndarray,
        block_type: int,
        timestamp: float,
        file_offset: int
    ) -> dict[str, Any]:
        waveforms = self.consume_block_waveforms(block_header)
        channel = block_header['Channel']
        gain = self.gain_per_slow_channel[channel]
        channel_frequency = self.frequency_per_slow_channel[channel]
        return {
            "type": block_type,
            "file_offset": file_offset,
            "timestamp": timestamp,
            "timestamp_seconds": timestamp / self.timestamp_frequency,
            "channel": channel,
            "unit": block_header['Unit'],
            "frequency": channel_frequency,
            "waveforms": waveforms * gain
        }


class PlexonPlxReader(Reader):
    """Read plexon .plx ad waveform chunks, spike events, and other numeric events."""

    def __init__(
        self,
        plx_file: str,
        file_finder: FileFinder,
        spikes: str | dict[str, str] = "all",
        events: str | dict[str, str] = "all",
        signals: str | dict[str, str] = "all",
        seconds_per_read: float = 1.0,
        spikes_prefix: str = "spike_",
        events_prefix: str = "event_",
        signals_prefix: str = "signal_"
    ) -> None:
        """Create a new PlexonPlxReader.

        Args:
            plx_file:           Path to the Plexon .plx file to read from.
            file_finder:        Utility to find() files in the conigured Pyramid configured search path.
                                Pyramid will automatically create and pass in the file_finder for you.
            spikes:             Dict of spike channel raw names to aliases, for which channels to keep.
                                Or, use spikes="all" to keep all channels with default names.
            events:             Dict of event channel raw names to aliases, for which channels to keep.
                                Or, use events="all" to keep all channels with default names.
            signals:            Dict of signal channel raw names to aliases, for which channels to keep.
                                Or, use signals="all" to keep all channels with default names.
            seconds_per_read:   How many seconds of data to read per read_next() call.
                                By default reads 1 second of data at a time by consuming blocks until the
                                consumed data span 1 second or more.  This is useful since .plx files are only
                                raggedly ordered in time (ordered within a channel, ragged between channels).
                                So, the next block might have an earlier timestamp than the current block.
                                Choose seconds_per_read to be greater than raggedness between channels.
                                Or, use seconds_per_read=0 to read one block at a time.
            spikes_prefix:      Default prefix for spike channels when spikes="all", to avoid naming collisions.
            events_prefix:      Default prefix for event channels when events="all", to avoid naming collisions.
            signals_prefix:     Default prefix for signals channels when signals="all", to avoid naming collisions.
        """
        self.plx_file = file_finder.find(plx_file)
        self.spikes = spikes
        self.events = events
        self.signals = signals
        self.seconds_per_read = seconds_per_read
        self.spikes_prefix = spikes_prefix
        self.events_prefix = events_prefix
        self.signals_prefix = signals_prefix

        self.raw_reader = PlexonPlxRawReader(self.plx_file)
        self.spike_channel_names = None
        self.event_channel_names = None
        self.signal_channel_names = None

    def __enter__(self) -> Any:
        self.raw_reader.__enter__()

        self.spike_channel_names = self.choose_channel_names(
            self.raw_reader.dsp_channel_headers,
            self.spikes,
            self.spikes_prefix
        )

        self.event_channel_names = self.choose_channel_names(
            self.raw_reader.event_channel_headers,
            self.events,
            self.events_prefix
        )

        self.signal_channel_names = self.choose_channel_names(
            self.raw_reader.slow_channel_headers,
            self.signals,
            self.signals_prefix
        )

        return self

    def choose_channel_names(
        self,
        channel_headers: list[dict[str, Any]],
        to_keep: str | dict[str, str],
        default_prefix: str
    ) -> dict[int, str]:
        if to_keep == "all":
            # Keep all channels with a prefix to prevent collisions between data types.
            return {int(header["Channel"]): f'{default_prefix}{header["Name"]}' for header in channel_headers}

        # Keep only explicitly requested channels, with aliases.
        names_by_id = {}
        for header in channel_headers:
            if header["Name"] in to_keep.keys():
                names_by_id[int(header["Channel"])] = to_keep[header["Name"]]
        return names_by_id

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        return self.raw_reader.__exit__(__exc_type, __exc_value, __traceback)

    #@profile
    def read_next(self) -> dict[str, BufferData]:
        (name, data) = self.read_one_block()
        if name is None:
            # If there's nothing at all to read, the .plx file is done.
            raise StopIteration

        # Otherwise, return at least some results.
        results = {}
        if name != "skip":
            results[name] = data
        first_data_time = data.get_end_time()
        while name is not None and data.get_end_time() - first_data_time < self.seconds_per_read:
            (name, data) = self.read_one_block()
            if name == "skip":
                continue
            elif name in results:
                results[name].append(data)
            else:
                results[name] = data

        return results

    #@profile
    def read_one_block(self) -> tuple[str, BufferData]:
        block = self.raw_reader.next_block()
        if block is None:
            return (None, None)

        block_type = block['type']
        if block_type == 1:
            # Block has one spike event with timestamp, channel, and unit.
            return self.block_spike_event(block)
        elif block_type == 4:
            # Block has one other event with timestamp, value.
            return self.block_event(block)
        elif block_type == 5:
            # Block has a waveform signal chunk.
            return self.block_signal_chunk(block)
        else:  # pragma: no cover
            logging.warning(f"Ignoring block of unknown type {block_type}.")
            return (None, None)

    #@profile
    def block_spike_event(self, block: dict[str, Any]) -> tuple[str, BufferData]:
        channel_id = block['channel']
        name = self.spike_channel_names.get(channel_id, "skip")
        event_list = NumericEventList(np.array([[block['timestamp_seconds'], channel_id, block['unit']]]))
        return (name, event_list)

    #@profile
    def block_event(self, block: dict[str, Any]) -> tuple[str, BufferData]:
        channel_id = block['channel']
        name = self.event_channel_names.get(channel_id, "skip")
        event_list = NumericEventList(np.array([[block['timestamp_seconds'], block['unit']]]))
        return (name, event_list)

    #@profile
    def block_signal_chunk(self, block: dict[str, Any]) -> tuple[str, BufferData]:
        channel_id = block['channel']
        name = self.signal_channel_names.get(channel_id, "skip")
        signal_chunk = SignalChunk(
            sample_data=block['waveforms'].reshape([-1, 1]),
            sample_frequency=float(block['frequency']),
            first_sample_time=float(block['timestamp_seconds']),
            channel_ids=[int(channel_id)]
        )
        return (name, signal_chunk)

    def get_initial(self) -> dict[str, BufferData]:
        """Peek at the .plx file so we can read headers and configure initial buffers -- but not consume data blocks yet."""
        initial = {}
        with PlexonPlxRawReader(self.plx_file) as peek_reader:
            # Spike channels have numeric events like [timestamp, channel_id, unit_id]
            spike_channel_names = self.choose_channel_names(
                peek_reader.dsp_channel_headers,
                self.spikes,
                self.spikes_prefix
            )
            for name in spike_channel_names.values():
                initial[name] = NumericEventList(np.empty([0, 3], dtype=np.float64))

            # Other event channels have numeric events like [timestamp, value]
            event_channel_names = self.choose_channel_names(
                peek_reader.event_channel_headers,
                self.events,
                self.events_prefix
            )
            for name in event_channel_names.values():
                initial[name] = NumericEventList(np.empty([0, 2], dtype=np.float64))

            # Slow ad channels have Signal chunks.
            signal_channel_names = self.choose_channel_names(
                peek_reader.slow_channel_headers,
                self.signals,
                self.signals_prefix
            )
            for channel_id, name in signal_channel_names.items():
                initial[name] = SignalChunk(
                    sample_data=np.empty([0, 1], dtype=np.float64),
                    sample_frequency=float(peek_reader.frequency_per_slow_channel[channel_id]),
                    first_sample_time=None,
                    channel_ids=[int(channel_id)]
                )

        return initial
