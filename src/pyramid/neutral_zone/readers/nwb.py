from types import TracebackType
from typing import Self
import logging
import numpy as np

from pyramid.file_finder import FileFinder
from pyramid.model.model import BufferData
from pyramid.model.events import NumericEventList
from pyramid.model.signals import SignalChunk
from pyramid.neutral_zone.readers.readers import Reader
from open_ephys.analysis import Session


class NwbNumericEventReader(Reader):
    """Read events from an NWB file
    DUE TO AN ERROR IN THE OPEN_EPHYS ANALYSIS TOOLS, THE NwbRecording.py FILE HAS THE FOLLOWING CHANGES:
    AT LINE: 145
        if len(dataset.split('-')) > 2:
            processor_id = int(dataset.split('.')[0].split('-')[2])
        else:
            processor_id = int(dataset.split('.')[0].split('-')[1])

    Skips lines that contain non-numeric values.
    """

    def __init__(
        self,
        nwb_file: str = None,
        stream_name: str = None,
        file_finder: FileFinder = FileFinder(),
        result_name: str = "events",
        sample_frequency = 0,
        **fmtparams
    ) -> None:
        self.nwb_file = file_finder.find(nwb_file)
        self.result_name = result_name
        self.fmtparams = fmtparams
        self.sample_frequency = sample_frequency
        self.file_stream = None
        self.nwb_reader = None
        self.stream_name = stream_name

    def __eq__(self, other: object) -> bool:
        """Compare NWB readers field-wise, to support use of this class in tests."""
        if isinstance(other, self.__class__):
            return (
                self.sample_frequency == other.sample_frequency
                and self.nwb_file == other.nwb_file
                and self.result_name == other.result_name
                and self.fmtparams == other.fmtparams
                and self.stream_name == other.stream_name
            )
        else:  # pragma: no cover
            return False

    def __enter__(self) -> Self:
        
        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        self.nwb_reader = None

    def get_initial(self) -> dict[str, BufferData]:
        self.line_num = -1
        logging.info(f"Found session file, loading...")
        session = Session(self.nwb_file)
        logging.info(f"Complete")
        recording = session.recordnodes[0].recordings[0] # Assuming a single record node and single recording that day
        self.nwb_reader = recording.events.loc[recording.events['stream_name'] == self.stream_name] # should be pandas dataframe already
        self.nwb_reader.reset_index(drop=True, inplace=True) # This is necessary when "slicing a dataframe", otherwise the indices will all be the original #s
        self.total_lines = self.nwb_reader.shape[0]
        if self.nwb_reader['timestamp'][0] < 0:
            self.nwb_reader['timestamp'] = (self.nwb_reader['sample_number'] - min(self.nwb_reader['sample_number']))*(1/self.sample_frequency)
        # NEW LWT 3/21/24
        all_states = np.array(self.nwb_reader['state'])
        diffs = np.diff(all_states) != 0 # Find all bit transitions
        self.indexes = np.nonzero(diffs)[0]+1 # Add one to get index of transitions
        self.byteSampNum = []
        self.total_changes = len(self.indexes)+1
        self.ith_change = -1
        
        
        return {
            self.result_name: NumericEventList(np.empty([0, 2]))
        }

    def read_next(self) -> dict[str, BufferData]:
        """ Iterate over TTL output lines to concatenate individual bits and form event codes.
        Event codes on the OE Aquisition box are unique and sent in the following format:
        Each event code is broken up into 2 separate 8-bit bytes
        The MSB of each byte is a "strobe, followed by 7 actual bits
        After a byte is sent, all of the lines are then set to 0
        So it should go:
        1) first 7 bits
        2) all bits zeroed
        3) second y bits
        4) all bits zeroed
        """
        first_byte = 0
        second_byte = 0
        event_int = []
        event_times = []
        step = 'first_byte'
        got_event = 0
        first_pass = 1
        samp_num_diff = 0
        line_state_diff = 0
        line_num_diff = 0
        while not(got_event) and (self.ith_change <= (self.total_changes)):
            self.ith_change += 1
            byteInt = []
            byteTime = []
            byteSampNum = []
            current_byte = np.array([0] * 8)
            if self.ith_change == 0:
                lines = self.nwb_reader.line.to_numpy()[:self.indexes[self.ith_change]] - 1 # Bit line# to index
                states = self.nwb_reader.state.to_numpy()[:self.indexes[self.ith_change]]
                samp_time = self.nwb_reader.timestamp.to_numpy()[:self.indexes[self.ith_change]]
                samp_num = self.nwb_reader.sample_number.to_numpy()[:self.indexes[self.ith_change]]
            elif self.ith_change < (self.total_changes-1):
                lines = self.nwb_reader.line.to_numpy()[self.indexes[self.ith_change-1]:self.indexes[self.ith_change]] - 1 # Bit line# to index
                states = self.nwb_reader.state.to_numpy()[self.indexes[self.ith_change-1]:self.indexes[self.ith_change]]
                samp_time = self.nwb_reader.timestamp.to_numpy()[self.indexes[self.ith_change-1]:self.indexes[self.ith_change]]
                samp_num = self.nwb_reader.sample_number.to_numpy()[self.indexes[self.ith_change-1]:self.indexes[self.ith_change]]
            else:
                # Last chunk or perhaps single bit
                lines = self.nwb_reader.line.to_numpy()[self.indexes[self.ith_change-1]:] - 1 # Bit line# to index
                states = self.nwb_reader.state.to_numpy()[self.indexes[self.ith_change-1]:]
                samp_time = self.nwb_reader.timestamp.to_numpy()[self.indexes[self.ith_change-1]:]
                samp_num = self.nwb_reader.sample_number.to_numpy()[self.indexes[self.ith_change-1]:]
            if states[0] == 0:
                continue
            else:
                # Assign the byte and do some checks:
                current_byte[lines] = states
                if len(self.byteSampNum) > 1:
                    # Check for a reasonable inter-sample interval for the bytes
                    if (np.min(samp_num) - self.byteSampNum[-1])<150:
                        print('Invalid inter-sample interval timing, skipping')
                        continue
                elif current_byte[-1] != 1:
                    # Check that the byte contains a strobe signal
                    print('Invalid byte, no strobe, skipping')
                    continue
                elif np.sum(current_byte) == 1:
                    # This indicates that the only active bit is the strobe, which if triggered accidentally will fuck everything up.
                    # so we check if the next sample immediately zeros the bit, indicating it was an unlikely event.
                    next_samp_num = self.nwb_reader.sample_number.to_numpy()[self.indexes[self.ith_change]]
                    if (next_samp_num - samp_num[-1]) == 1:
                        print('Likely false strobe signal, skipping')
                        continue
                # Initial checks okay
                byteInt.append(int(''.join(map(str, reversed(current_byte))), 2))
                byteTime.append(np.min(samp_time))
                byteSampNum.append(np.max(samp_num))
                if step == 'first_byte':
                    if byteInt[0] > 206:
                        print('Invalid first byte: exceeds maximum possible integer, skipping')
                        continue
                    else:
                        first_byte = byteInt[0]
                        self.byteSampNum.append(byteSampNum[0]) 
                        event_times.append(byteTime[0])
                        current_byte = np.array([0] * 8)
                        step = 'second_byte'
                else:
                    second_byte = byteInt[0]
                    self.byteSampNum.append(byteSampNum[0])
                    event_int.append(((int(first_byte) & 127) << 7) | (int(second_byte) & 127))
                    got_event = 1   
        if got_event:
            try:
                return {
                    self.result_name: NumericEventList(np.array([event_times, event_int]).reshape(1,2))
                }
            except ValueError as error:
                logging.info(f"Error reading nwb events file because {error.args}, line: {self.line_num-1}")
                return None
        elif self.ith_change == self.self.total_changes:
            raise StopIteration
        else:
            logging.info(f"End of event buffer or empty event, returning default: None")
            return None


class NwbContinuousReader(Reader):
    """Read numeric signals from a continuous data source in an nwb file
    See: https://github.com/open-ephys/open-ephys-python-tools/tree/main/src/open_ephys/analysis
    Because the memory-mapped samples are stored as 16-bit integers in arbitrary units, 
    all analysis should be done on a scaled version of these samples. 
    To load the samples scaled to microvolts, use the get_samples() method.
    Note that your computer may run out of memory when requesting a large number of samples
    for many channels at once. It's also important to note that start_sample_index and 
    end_sample_index represent relative indices in the samples array, rather than absolute sample numbers.
    """

    def __init__(
        self,
        nwb_file: str = None,
        file_finder: FileFinder = FileFinder(),
        sample_frequency: float = 1.0,
        next_sample_time: float = 0.0,
        lines_per_chunk: int = 10,
        result_name: str = "samples",
        stream_name: str = None,
        stream_idx = None,
        channel_ids = None,
        **fmtparams
    ) -> None:
        self.nwb_file = file_finder.find(nwb_file)
        self.sample_frequency = sample_frequency
        self.next_sample_time = next_sample_time
        self.lines_per_chunk = lines_per_chunk
        self.result_name = result_name
        self.fmtparams = fmtparams
        self.channel_ids = channel_ids
        self.stream_name = stream_name
    
    def __eq__(self, other: object) -> bool:
        """Compare NWB readers field-wise, to support use of this class in tests."""
        if isinstance(other, self.__class__):
            return (
                self.sample_rate == other.sample_rate
                and self.channel_ids == other.channel_ids
                and self.stream_name == other.stream_name
                and self.nwb_file == other.nwb_file
                and self.result_name == other.result_name
                and self.fmtparams == other.fmtparams
            )
        else:  # pragma: no cover
            return False

    def __enter__(self) -> Self:
        
        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        self.nwb_reader = None

    def get_initial(self) -> dict[str, BufferData]:
        self.line_num = 0
        logging.info(f"Found session file, loading...")
        session = Session(self.nwb_file)
        logging.info(f"Complete")
        recording = session.recordnodes[0].recordings[0] # Assuming a single record node and single recording that day
        # Get the appropriate data stream
        for i in np.arange(len(recording.continuous)): 
            if recording.continuous[i].metadata['stream_name'] == self.stream_name:
                self.stream_idx = i
                break

        try: self.nwb_reader = recording.continuous[self.stream_idx]
        except Exception:
            logging.info(f"Error reading nwb file, no continuous sources match the stream name")

        if self.channel_ids is None:
            self.channel_ids = np.arange(self.nwb_reader.samples.shape[1]) # default to all channels

        initial = SignalChunk(
            np.empty([0, len(self.channel_ids)]),
            self.sample_frequency,
            self.next_sample_time,
            self.channel_ids
        )
        return {self.result_name: initial}

    def read_next(self) -> dict[str, BufferData]:
        chunk = []
        if self.line_num >= self.nwb_reader.samples.shape[0]:
            raise StopIteration
        try:
            # get_samples will not error if the indices are out of range. It will return up to the last sample
            chunk = self.nwb_reader.get_samples(start_sample_index=self.line_num, 
                                                end_sample_index=self.line_num+self.lines_per_chunk,
                                                selected_channels = self.channel_ids)
            self.line_num += self.lines_per_chunk
        except StopIteration:
            return chunk
            # We reached the end.  We still want to return the last, partial chunk below.

        if len(chunk)>0:
            # We got a complete chunk, or the last, partial chunk.
            signal_chunk = SignalChunk(
                chunk,
                self.sample_frequency,
                self.next_sample_time,
                self.channel_ids
            )
            self.next_sample_time += len(chunk) / self.sample_frequency
            return {self.result_name: signal_chunk}
        else:
            # We're really at the end, past the last chunk, signal stop to the caller.
            raise StopIteration

    
   