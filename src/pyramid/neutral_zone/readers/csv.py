from types import TracebackType
from typing import Self
import logging
import csv
import numpy as np

from pyramid.file_finder import FileFinder
from pyramid.model.model import BufferData
from pyramid.model.events import NumericEventList
from pyramid.model.signals import SignalChunk
from pyramid.neutral_zone.readers.readers import Reader


class CsvNumericEventReader(Reader):
    """Read numeric events from a CSV of numbers.

    Skips lines that contain non-numeric values.
    """

    def __init__(
        self,
        csv_file: str = None,
        file_finder: FileFinder = FileFinder(),
        result_name: str = "events",
        dialect: str = 'excel',
        **fmtparams
    ) -> None:
        self.csv_file = file_finder.find(csv_file)
        self.result_name = result_name
        self.dialect = dialect
        self.fmtparams = fmtparams

        self.file_stream = None
        self.csv_reader = None

    def __eq__(self, other: object) -> bool:
        """Compare CSV readers field-wise, to support use of this class in tests."""
        if isinstance(other, self.__class__):
            return (
                self.csv_file == other.csv_file
                and self.result_name == other.result_name
                and self.dialect == other.dialect
                and self.fmtparams == other.fmtparams
            )
        else:  # pragma: no cover
            return False

    def __enter__(self) -> Self:
        # See https://docs.python.org/3/library/csv.html#id3 for why this has newline=''
        self.file_stream = open(self.csv_file, mode='r', newline='')
        self.csv_reader = csv.reader(self.file_stream, self.dialect, **self.fmtparams)
        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        if self.file_stream:
            self.file_stream.close()
            self.file_stream = None
        self.csv_reader = None

    def read_next(self) -> dict[str, BufferData]:
        line_num = self.csv_reader.line_num
        next_row = self.csv_reader.__next__()
        try:
            numeric_row = [float(element) for element in next_row]
            return {
                self.result_name: NumericEventList(np.array([numeric_row]))
            }
        except ValueError as error:
            logging.info(f"Skipping CSV '{self.csv_file}' line {line_num} {next_row} because {error.args}")
            return None

    def get_initial(self) -> dict[str, BufferData]:
        first_row = peek_at_csv(self.csv_file, self.dialect, **self.fmtparams)
        if first_row:
            column_count = len(first_row)
        else:
            column_count = 2
            logging.warning("Using default column count for CSV events: {column_count}")

        return {
            self.result_name: NumericEventList(np.empty([0, column_count]))
        }


def peek_at_csv(csv_file: str, dialect: str, **fmtparams) -> list[str]:
    try:
        # Peek at the first line of the CSV to get the first row.
        with open(csv_file, mode='r', newline='') as f:
            reader = csv.reader(f, dialect, **fmtparams)
            return reader.__next__()
    except Exception:
        logging.error(f"Unable to peek at CSV file {csv_file}", exc_info=True)
        return []


class CsvSignalReader(Reader):
    """Read numeric signals from a CSV of numbers.

    Expects a header line with channel ids.
    Skips any other lines that contain non-numeric values.
    """

    def __init__(
        self,
        csv_file: str = None,
        file_finder: FileFinder = FileFinder(),
        sample_frequency: float = 1.0,
        next_sample_time: float = 0.0,
        lines_per_chunk: int = 10,
        result_name: str = "samples",
        dialect: str = 'excel',
        **fmtparams
    ) -> None:
        self.csv_file = file_finder.find(csv_file)
        self.sample_frequency = sample_frequency
        self.next_sample_time = next_sample_time
        self.lines_per_chunk = lines_per_chunk
        self.result_name = result_name
        self.dialect = dialect
        self.fmtparams = fmtparams

        self.file_stream = None
        self.csv_reader = None
        self.channel_ids = None

    def __enter__(self) -> Self:
        # See https://docs.python.org/3/library/csv.html#id3 for why this has newline=''
        self.file_stream = open(self.csv_file, mode='r', newline='')
        self.csv_reader = csv.reader(self.file_stream, self.dialect, **self.fmtparams)
        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        if self.file_stream:
            self.file_stream.close()
            self.file_stream = None
        self.csv_reader = None

    def read_next(self) -> dict[str, BufferData]:
        chunk = []
        while len(chunk) < self.lines_per_chunk:
            line_num = self.csv_reader.line_num 
            try:
                next_row = self.csv_reader.__next__()
            except StopIteration:
                # We reached the end.  We still want to return the last, partial chunk below.
                break

            try:
                numeric_row = [float(element) for element in next_row]
                chunk.append(numeric_row)
            except ValueError as error:
                logging.info(f"Skipping CSV '{self.csv_file}' line {line_num} {next_row} because {error.args}")
                continue

        if chunk:
            # We got a complete chunk, or the last, partial chunk.
            signal_chunk = SignalChunk(
                np.array(chunk),
                self.sample_frequency,
                self.next_sample_time,
                self.channel_ids
            )
            self.next_sample_time += len(chunk) / self.sample_frequency
            return {self.result_name: signal_chunk}
        else:
            # We're really at the end, past the last chunk, signal stop to the caller.
            raise StopIteration

    def get_initial(self) -> dict[str, BufferData]:
        self.channel_ids = peek_at_csv(self.csv_file, self.dialect, **self.fmtparams)
        initial = SignalChunk(
            np.empty([0, len(self.channel_ids)]),
            self.sample_frequency,
            self.next_sample_time,
            self.channel_ids
        )
        return {self.result_name: initial}
