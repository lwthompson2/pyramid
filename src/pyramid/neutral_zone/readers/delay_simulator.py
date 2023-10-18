from types import TracebackType
from typing import Self
import time

from pyramid.neutral_zone.readers.readers import Reader
from pyramid.model.model import BufferData


class DelaySimulatorReader(Reader):
    """Simulate delay between events so offline plays back sort of like online."""

    def __init__(self, reader: Reader) -> None:
        self.reader = reader
        self.stashed_result = None
        self.stash_until = None
        self.start_time = None

    def __eq__(self, other: object) -> bool:
        """Compare readers field-wise, to support use of this class in tests."""
        if isinstance(other, self.__class__):
            return self.reader == other.reader
        else:  # pragma: no cover
            return False

    def __enter__(self) -> Self:
        self.start_time = time.time()
        return self.reader.__enter__()

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        self.reader.__exit__(__exc_type, __exc_value, __traceback)

    def read_next(self) -> dict[str, BufferData]:
        if self.stashed_result:
            if time.time() >= self.stash_until:
                stashed = self.stashed_result
                self.stashed_result = None
                self.stash_until = None
                return stashed
            else: # pragma: no cover
                # This branch may or may not occur during tests because it depends on timing.
                return None

        next_result = self.reader.read_next()
        if next_result:
            latest_result_time = max([result.get_end_time() for result in next_result.values()])
            self.stash_until = self.start_time + latest_result_time
            self.stashed_result = next_result

    def get_initial(self) -> dict[str, BufferData]:
        return self.reader.get_initial()
