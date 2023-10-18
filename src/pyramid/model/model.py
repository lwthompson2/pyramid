import sys
from importlib import import_module
from typing import Any, Self
from inspect import signature

from pyramid.file_finder import FileFinder

class DynamicImport():
    """Utility for creating class instances from a dynamically imported module and class.

    Document optional file_finder, injected by context, or None
    """

    @classmethod
    def from_dynamic_import(
        cls,
        import_spec: str,
        file_finder: FileFinder,
        external_package_path: str = None,
        **kwargs
    ) -> Self:
        """Create a class instance from a dynamically imported module and class.

        The given import_spec should be of the form "package.subpackage.module.ClassName".
        The "package.subpackage.module" will be imported dynamically via importlib.
        Then "ClassName" from the imported module will be invoked as a class constructor.

        This should be equivalent to the static statement "from package.subpackage.module import ClassName",
        followed by instance = ClassName(**kwargs)

        Returns a new instance of the imported class.

        Provide external_package_path in order to import a class from a module that was not
        already installed by the usual means, eg conda or pip.  The external_package_path will
        be added temporarily to the Python import search path, then removed when done here.
        """
        last_dot = import_spec.rfind(".")
        module_spec = import_spec[0:last_dot]

        try:
            original_sys_path = sys.path
            if external_package_path:
                sys.path = original_sys_path.copy()
                path_to_add = file_finder.find(external_package_path)
                sys.path.append(path_to_add)
            imported_module = import_module(module_spec, package=None)
        finally:
            sys.path = original_sys_path

        class_name = import_spec[last_dot+1:]
        imported_class = getattr(imported_module, class_name)

        # Does the class constructor want to have a "file_finder" helper injected?
        constructor_signature = signature(imported_class)
        if "file_finder" in constructor_signature.parameters.keys():
            instance = imported_class(file_finder=file_finder, **kwargs)
        else:
            instance = imported_class(**kwargs)
        return instance


class BufferData():
    """An interface to tell us what Pyramid data types must have in common in order to flow from Reader to Trial."""

    def copy(self) -> Self:
        """Create a new, independent copy of the data -- allows reusing raw data along multuple routes/buffers."""
        raise NotImplementedError  # pragma: no cover

    def copy_time_range(self, start_time: float = None, end_time: float = None) -> Self:
        """Copy subset of data in half-open interval [start_time, end_time) -- allows selecting data into trials.

        Omit start_time to copy all events strictly before end_time.
        Omit end_time to copy all events at and after start_time.
        """
        raise NotImplementedError  # pragma: no cover

    def append(self, other: Self) -> None:
        """Append data from the given object to this object, in place -- this is the main buffering operation."""
        raise NotImplementedError  # pragma: no cover

    def discard_before(self, start_time: float) -> None:
        """Discard data strictly before the given start_time -- to prevent buffers from consuming unlimited memory."""
        raise NotImplementedError  # pragma: no cover

    def shift_times(self, shift: float) -> None:
        """Shift data times, in place -- allows Trial "wrt" alignment and Reader clock adjustments."""
        raise NotImplementedError  # pragma: no cover

    def get_end_time(self) -> float:
        """Report the time of the latest data item still in the buffer."""
        raise NotImplementedError  # pragma: no cover


class Buffer():
    """Hold data in a sliding window of time, smoothing any timing mismatch between Readers and Trials.

    In addition to the actual buffer data, holds a clock drift estimate that may change over time.
    Reader routers can update this offset as they calibrate themselves over time,
    and Trials can include this offset querying and aligning data.
    """

    def __init__(
        self,
        initial_data: BufferData,
        initial_clock_drift: float = 0.0
    ) -> None:
        self.data = initial_data
        self.clock_drift = initial_clock_drift

    def __eq__(self, other: object) -> bool:
        """Compare buffers field-wise, to support use of this class in tests."""
        if isinstance(other, self.__class__):
            return (
                self.data == other.data
                and self.clock_drift == other.clock_drift
            )
        else:  # pragma: no cover
            return False

    def raw_time_to_reference(self, raw_time: float) -> float:
        """Convert a time from the buffer's own raw clock to align with the Pyramid reference clock."""
        return raw_time - self.clock_drift

    def reference_time_to_raw(self, reference_time: float) -> float:
        """Convert a time Pyramid's reference clock to align with the buffer's own raw clock."""
        if reference_time is None:
            return None
        else:
            return reference_time + self.clock_drift
