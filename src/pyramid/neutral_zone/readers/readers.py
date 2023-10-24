from types import TracebackType
from typing import Any, ContextManager
from dataclasses import dataclass, field
import logging

from pyramid.model.model import DynamicImport, BufferData, Buffer
from pyramid.model.events import NumericEventList
from pyramid.neutral_zone.transformers.transformers import Transformer


class Reader(DynamicImport, ContextManager):
    """Interface for consuming data from arbitrary sources and converting to Pyramid BufferData types.

    Each reader implementation should:
     - Encapsulate the details of how to connect to a data source and get data from it.
     - Maintain internal state related to the data source, like a file handle and byte offset, a data block index,
       a socket descriptor, etc.
     - Implement read_next() to consume an increment of available data from the source, update internal state
       to reflect this, and return results as a dict of name - BufferData entries.
     - Implement __enter__() and __exit__() to confirm to Python's "context manager protocol"", which
       is how Pyramid manages acquisition and release of system and libarary resources.
       See: https://peps.python.org/pep-0343/#standard-terminology
     - Implement get_initial() to return a dictionary of name - BufferData entries, allowing users of the
       Reader to see the expected names and BufferData sub-types that the reader will produce.

    The focus of a reader implementation should be getting data out of the source incrementally and converting
    each increment into a dict of BufferData values.  From there, Pyramid takes the results of get_initial()
    and read_next() and handles how the data are copied into connected buffers, filtered and transformed into
    desired forms, and eventually assigned to trials.
    """

    def __enter__(self) -> Any:
        """Connect to a data source and acquire related system or library resources.

        Return an object that we can "read_next()" on -- probably return self.
        """
        raise NotImplementedError  # pragma: no cover

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        """Release any resources acquired during __enter()__."""
        raise NotImplementedError  # pragma: no cover

    def read_next(self) -> dict[str, BufferData]:
        """Read/poll for new data at the connected source and convert available data to Pyramid BufferData types.

        This must not block when reading from its data source.
        Rather, it should read/poll for data once and just return None if no data are available yet.
        Pyramid will call read_next() again, soon, to get the next available data.
        This convention allows multiple concurrent readers to be interleaved,
        and for the readers to be interleaved with other tasks like interactive GUI user event handling.

        The implementation can choose its own read/poll strategy or timeout.
        Returning from read_next() within ~1 millisecond should be good.

        Return a dicitonary of any data consumed during the read increment, or None if no data available.
        Dictionary values must all be Pyramid BufferData types.
        Dictionary keys should suggest an interpretation of the interpretation, like "spikes", "event_codes", etc.
        """
        raise NotImplementedError  # pragma: no cover

    def get_initial(self) -> dict[str, BufferData]:
        """Create an initial dictionary of names and BufferData sub-types that Reader expects to produce.

        This is called before __enter__() or read_next().
        It's intended to inform Pyramid what result keys this Reader will produce, and the BufferData sub-types that it will use.
        These help setting up downstream components that receive the results of read_next().
        The initial dictionary returned here can (and should!) depend on the kwargs passed to the Reader's constructor. 
        """
        raise NotImplementedError  # pragma: no cover


@dataclass
class ReaderRoute():
    """Specify the mapping from a reader get_initial() or read_next() diciontary entry to a named buffer."""

    reader_result_name: str
    """How the reader named a result, like "spikes", "events", etc."""

    buffer_name: str
    """Name for the buffer that will receive the BufferData for "spikes", "ecodes", etc."""

    transformers: list[Transformer] = field(default_factory=list)
    """Optional data transformations between reader and buffer, applied in order."""


@dataclass
class ReaderSyncConfig():
    """Specify configuration for how a reader should find sync events and correct for clock drift."""

    is_reference: str = False
    """Whether the reader represents the canonical, reference clock to which others readers will be aligned."""

    reader_result_name: str = None
    """The name of the reader result that will contain clock sync numeric events."""

    event_value: int | float = None
    """The value of sync events to look for, within the named event buffer."""

    event_value_index: int = 0
    """The numeric event value index to use within the named event buffer."""

    reader_name: str = None
    """The name of the reader to act as when aligning data within trials.

    Usually reader_name would be the name of the same reader that this config applies to.
    Or it may be the name of a different reader so that one reader may reuse sync info from another.
    For example, a Phy spike reader might want to use sync info from an upstream data source like Plexon or OpenEphys.
    """


class ReaderSyncRegistry():
    """Keep track of sync events as seen by different readers, and clock drift compared to a referencce reader.

        When comparing sync event times between readers the registry will use the latest sync information recorded so far.
        It will also try to line up times in pairs so that both times correspond to the same real-world sync event.

            reference: |   |   |   |   |   |   |   |
            other:     |   |   |   |   |  |   |   |
                                                  ^^ latest pair seen so far, seems like a reasonable drift estimate

        The registry will form the pairs based on difference in time, as opposed just lining up array indexes.
        This should make the drift estimates robust in case readers record different numbers of sync events.
        For example, one reader might suddenly stop recording sync altogether.

            reference: |   |   |   |   |   |   |   |
            other:     |   |   |
                                  ^ oops, sync from other dropped around here here!

        In this case, pairing up the latest events by array index would lead to "drift" estimates that grow
        in real time, and don't really reflect the underlying clock rates.

        So instead, the registry will consider the latest sync event time from each reader, and pair it with the closest
        event time from the other reader.  From these two "closest" pairs, it will choose the pair with the smallest
        time difference.
            reference: |   |   |   |   |   |   |   |
            other:     |   |   |                   ^ "closest" from reference is huge and growing in real time!
                               ^ "closest" from other is older, but still looks reasonable

        All this assumes that clock drift is small compared to the interval between real-world sync events.  If that's
        true then looking for small differences between readers is a good way to discover which times go together.
    """

    def __init__(
        self,
        reference_reader_name: str
    ) -> None:
        self.reference_reader_name = reference_reader_name
        self.event_times = {}

    def __eq__(self, other: object) -> bool:
        """Compare registry field-wise, to support use of this class in tests."""
        if isinstance(other, self.__class__):
            return (
                self.reference_reader_name == other.reference_reader_name
                and self.event_times == other.event_times
            )
        else:  # pragma: no cover
            return False

    def record_event(self, reader_name: str, event_time: float) -> None:
        """Record a sync event as seen by the named reader."""
        reader_event_times = self.event_times.get(reader_name, [])
        reader_event_times.append(event_time)
        self.event_times[reader_name] = reader_event_times

    def get_drift(
        self,
        reader_name: str,
        reference_end_time: float = None,
        reader_end_time: float = None
    ) -> float:
        """Estimate clock drift between the named reader and the reference, based on events marked for each reader."""
        reference_event_times = self.event_times.get(self.reference_reader_name, None)
        if not reference_event_times:
            return 0.0

        if reference_end_time is not None:
            reference_event_times = [time for time in reference_event_times if time <= reference_end_time]

        reader_event_times = self.event_times.get(reader_name, None)
        if not reader_event_times:
            return 0.0

        if reader_end_time is not None:
            reader_event_times = [time for time in reader_event_times if time <= reader_end_time]

        reader_last = reader_event_times[-1]
        reader_offsets = [reader_last - ref_time for ref_time in reference_event_times]
        drift_from_reader = min(reader_offsets, key=abs)

        reference_last = reference_event_times[-1]
        reference_offsets = [reader_time - reference_last for reader_time in reader_event_times]
        drift_from_reference = min(reference_offsets, key=abs)

        return min(drift_from_reader, drift_from_reference, key=abs)


class ReaderRouter():
    """Get incremental results from a reader, copy and route the data into named buffers.

    If the reader throws an exception, it will be ignored going forward.
    This would apply equally to errors and orderly end-of-data situations.
    """

    def __init__(
        self,
        reader: Reader,
        routes: list[ReaderRoute],
        named_buffers: dict[str, Buffer],
        empty_reads_allowed: int = 3,
        sync_config: ReaderSyncConfig = None,
        sync_registry: ReaderSyncRegistry = None
    ) -> None:
        self.reader = reader
        self.routes = routes
        self.named_buffers = named_buffers
        self.empty_reads_allowed = empty_reads_allowed
        self.sync_config = sync_config
        self.sync_registry = sync_registry

        self.reader_exception = None
        self.max_buffer_time = 0.0
        self.clock_drift = 0.0

    def __eq__(self, other: object) -> bool:
        """Compare routers field-wise, to support use of this class in tests."""
        if isinstance(other, self.__class__):
            return (
                self.reader == other.reader
                and self.routes == other.routes
                and self.named_buffers == other.named_buffers
                and self.empty_reads_allowed == other.empty_reads_allowed
                and self.sync_config == other.sync_config
            )
        else:  # pragma: no cover
            return False

    def still_going(self) -> bool:
        return not self.reader_exception

    def route_next(self) -> bool:
        """Ask the reader to consume an increment of data, unconditoinally, and deal results into connected buffers."""
        if self.reader_exception:
            return False

        try:
            read_result = self.reader.read_next()
        except StopIteration as stop_iteration:
            self.reader_exception = stop_iteration
            logging.info(f"Reader {self.reader.__class__.__name__} is done (it raised StopIteration).")
            return False
        except Exception as exception:
            self.reader_exception = exception
            logging.warning(
                f"Reader {self.reader.__class__.__name__} is disabled (it raised an unexpected error):",
                exc_info=True
            )
            return False

        if not read_result:
            return False

        if self.sync_config is not None and self.sync_registry is not None:
            # Add any new sync events to the sync registry.
            event_data = read_result.get(self.sync_config.reader_result_name, None)
            if event_data is not None and isinstance(event_data, NumericEventList):
                sync_event_times = event_data.get_times_of(
                    event_value=self.sync_config.event_value,
                    value_index=self.sync_config.event_value_index
                )
                for event_time in sync_event_times:
                    self.sync_registry.record_event(self.sync_config.reader_name, event_time)

        for route in self.routes:
            buffer = self.named_buffers.get(route.buffer_name, None)
            if not buffer:
                continue

            data = read_result.get(route.reader_result_name, None)
            if not data:
                continue

            data_copy = data.copy()
            if route.transformers:
                try:
                    for transformer in route.transformers:
                        data_copy = transformer.transform(data_copy)
                except Exception as exception:
                    logging.error(
                        f"Route transformer had an exception, skipping data for {route.reader_result_name} -> {route.buffer_name}:",
                        exc_info=True
                    )
                    continue

            try:
                buffer.data.append(data_copy)
            except Exception as exception:
                logging.error(
                    "Route buffer had exception appending data, skipping data for {route.reader_result_name} -> {route.buffer_name}:",
                    exc_info=True
                )
                continue

        # Update the high water mark for the reader -- the latest timestamp seen so far.
        for buffer in self.named_buffers.values():
            buffer_end_time = buffer.data.get_end_time()
            if buffer_end_time and buffer_end_time > self.max_buffer_time:
                self.max_buffer_time = buffer_end_time

        return True

    def route_until(self, target_reference_time: float) -> float:
        """Ask the reader to read data 0 or more times until catching up to a target time.

        Return the latest timestamp seen, so far.
        """
        empty_reads = 0
        target_reader_time = target_reference_time + self.clock_drift
        while self.max_buffer_time < target_reader_time and empty_reads <= self.empty_reads_allowed:
            got_data = self.route_next()
            if got_data:
                empty_reads = 0
            else:
                empty_reads += 1

        return self.max_buffer_time

    def update_drift_estimate(self, reference_end_time: float = None) -> float:
        """Get a reader clock drift estimate from the sync registry and propagate it to all buffers.

        Return the current drift estimate.
        """
        if self.sync_config is None or self.sync_registry is None:
            return None

        if reference_end_time is None:
            reader_end_time = None
        else:
            reader_end_time = reference_end_time + self.clock_drift
        self.clock_drift = self.sync_registry.get_drift(
            self.sync_config.reader_name,
            reference_end_time,
            reader_end_time
        )
        for buffer in self.named_buffers.values():
            buffer.clock_drift = self.clock_drift

        return self.clock_drift
