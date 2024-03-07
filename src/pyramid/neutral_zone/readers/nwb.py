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
        file_finder: FileFinder = FileFinder(),
        result_name: str = "events",
        **fmtparams
    ) -> None:
        self.nwb_file = file_finder.find(nwb_file)
        self.result_name = result_name
        self.fmtparams = fmtparams

        self.file_stream = None
        self.csv_reader = None

    def __eq__(self, other: object) -> bool:
        """Compare NWB readers field-wise, to support use of this class in tests."""
        if isinstance(other, self.__class__):
            return (
                self.nwb_file == other.nwb_file
                and self.result_name == other.result_name
                and self.fmtparams == other.fmtparams
            )
        else:  # pragma: no cover
            return False

    def __enter__(self) -> Self:
        session = Session(self.nwb_file)
        recording = session.recordnodes[0].recordings[0] # Assuming a single record node and single recording that day
        self.nwb_reader = recording.events # should be pandas dataframe already
        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        self.nwb_reader = None

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

        first_byte = [0] * 8
        second_byte = [0] * 8
        event_int = []
        event_times = []
        step = 'first_byte'
        got_event = 0

        while not(got_event) & (self.line_num <= self.total_lines):
            lines = self.nwb_reader['line'][self.line_num]
            states = self.nwb_reader['state'][self.line_num]
            samp_time = self.nwb_reader['timestamp'][self.line_num]
            self.line_num += 1

            if step == 'first_byte':
                if states:
                    if sum(first_byte) == 0:
                        first_byte_time = samp_time
                    first_byte[lines] = states
                else:
                    byte1 = first_byte
                    first_byte[lines] = states
                    step = "first_zero"
            elif step == 'first_zero':
                if not states:
                    first_byte[lines] = states
                else:
                    if sum(first_byte) > 0:
                        print('first zero complete, but the first byte has not been reset, skipping sample')
                        continue
                    second_byte[lines] = states
                    second_byte_time = samp_time
                    step = 'second_byte'
            elif step == 'second_byte':
                if states:
                    second_byte[lines] = states
                else:
                    byte2 = second_byte
                    second_byte[lines] = states
                    step = "second_zero"
                    byte1Int = int(''.join(map(str, byte1)), 2)
                    byte2Int = int(''.join(map(str, byte2)), 2)
                    integer = ((byte1Int & 127) << 7) | (byte2Int & 127)
                    if first_byte_time in event_times:
                        print('simultaneous events detected')
                    else:
                        event_int = integer
                        event_times = first_byte_time
            elif step == 'second_zero':
                if not states:
                    second_byte[lines] = states
                else:
                    if sum(second_byte) > 0:
                        print('second zero "complete", but the second byte has not been reset, skipping sample')
                        continue
                    # everything is complete, the next iteration should start on the current line 
                    got_event = 1

        try:
            return {
                self.result_name: NumericEventList(np.array([event_times, event_int]))
            }
        except ValueError as error:
            logging.info(f"Error reading nwb events file because {error.args}")
            return None

    def get_initial(self) -> dict[str, BufferData]:
        self.line_num = 0
        self.total_lines = self.nwb_reader.shape[0]
        return {
            self.result_name: NumericEventList(np.empty([0, 2]))
        }