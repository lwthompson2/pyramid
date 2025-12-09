import logging
from types import TracebackType
from typing import Self, ContextManager
from collections.abc import Iterator
from pathlib import Path

import json
import h5py
import numpy as np

from pyramid.model.events import NumericEventList, TextEventList
from pyramid.model.signals import SignalChunk, SignalTimeChunk
from pyramid.trials.trials import Trial


class TrialFile(ContextManager):
    """Write and read Pyramid Trials to and from a file.

    The TrialFile class itself is an abstract interface, to be implemented using various file formats
    like JSON and HDF5.

    It's up to each implementation to handle data mapping/conversion details.
    Each trial written with append_trial() should be recovered from read_trials() such that
      original_trial == recovered_trial
    in the Python sense of == or __eq__().
    """

    def __enter__(self) -> Self:
        """Create a new, empty file for writing trials into."""
        raise NotImplementedError  # pragma: no cover

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        """If needed, clean up resources used while writing trials to disk."""
        pass

    def append_trial(self, trial: Trial) -> None:
        """Write the given trial to the end of the file on disk.

        Implementations should try to leave a well-formed file on disk after each call to append_trial.
        This should allow calls to read_trials() to be interleaved with calls to append_trial(), if needed.

        For example, the file could be opened and closed during each append_trial(),
        as opposed to being opened once during __enter__() and held open until __exit__().
        """
        raise NotImplementedError  # pragma: no cover

    def read_trials(self) -> Iterator[Trial]:
        """Yield a sequence of trials from the file on disk, one at a time, in order.

        Implementations should implement this as a Python generator, using the yield keyword.
        https://wiki.python.org/moin/Generators

        It's OK to only return trials that were written to disk as of when read_trials() was first called.
        The generator doesn't need to check if new trials were written concurrently during iteration.
        """
        raise NotImplementedError  # pragma: no cover

    @classmethod
    def for_file_suffix(cls, file_name: str, create_empty: bool = False) -> Self:
        suffixes = {suffix.lower() for suffix in Path(file_name).suffixes}
        if suffixes.intersection({".json", ".jsonl"}):
            return JsonTrialFile(file_name, create_empty)
        elif suffixes.intersection({".hdf", ".h5", ".hdf5", ".he5"}):
            return Hdf5TrialFile(file_name, create_empty)
        else:
            raise NotImplementedError(f"Unsupported trial file suffixes: {suffixes}")


class JsonTrialFile(TrialFile):
    """Text-based trial file using one line of JSON per trial.

    This trial file implementation uses the concept of "JSON Lines" to support large, streamable JSON files.
    https://jsonlines.org/
    """

    def __init__(self, file_name: str, create_empty: bool = False) -> None:
        self.file_name = file_name
        self.create_empty = create_empty

    def __enter__(self) -> Self:
        if self.create_empty:
            with open(self.file_name, "w", encoding="utf-8"):
                logging.info(f"Creating empty JSON trial file: {self.file_name}")
        return self

    def append_trial(self, trial: Trial) -> None:
        trial_dict = self.dump_trial(trial)
        trial_json = json.dumps(trial_dict)
        with open(self.file_name, 'a', encoding="utf-8") as f:
            f.write(trial_json + "\n")

    def read_trials(self) -> Iterator[Trial]:
        with open(self.file_name, 'r', encoding="utf-8") as f:
            for json_line in f:
                trial_dict = json.loads(json_line)
                yield self.load_trial(trial_dict)

    def dump_numeric_event_list(self, numeric_event_list: NumericEventList) -> list:
        return numeric_event_list.event_data.tolist()

    def load_numeric_event_list(self, raw_list: list) -> NumericEventList:
        return NumericEventList(np.array(raw_list))

    def dump_text_event_list(self, text_event_list: TextEventList) -> dict:
        return {
            "timestamp_data": text_event_list.timestamp_data.tolist(),
            "text_data": text_event_list.text_data.tolist()
        }

    def load_text_event_list(self, raw_dict: dict) -> TextEventList:
        return TextEventList(
            timestamp_data=np.array(raw_dict["timestamp_data"]),
            text_data=np.array(raw_dict["text_data"], dtype=np.str_)
        )

    def dump_signal_chunk(self, signal_chunk) -> dict:
        if isinstance(signal_chunk, SignalChunk):
            return {
                "type": "SignalChunk",
                "signal_data": signal_chunk.sample_data.tolist(),
                "sample_frequency": signal_chunk.sample_frequency,
                "first_sample_time": signal_chunk.first_sample_time,
                "channel_ids": signal_chunk.channel_ids
            }
        elif isinstance(signal_chunk, SignalTimeChunk):
            return {
                "type": "SignalTimeChunk",
                "signal_data": signal_chunk.sample_data.tolist(),
                "timestamps": signal_chunk.timestamps.tolist(),
                "sample_frequency": signal_chunk.sample_frequency,
                "channel_ids": signal_chunk.channel_ids
            }
        else:
            raise TypeError(f"Unsupported signal chunk type: {type(signal_chunk)}")

    def load_signal_chunk(self, raw_dict: dict):
        if raw_dict.get("type") == "SignalChunk":
            return SignalChunk(
                sample_data=np.array(raw_dict["signal_data"]),
                sample_frequency=raw_dict["sample_frequency"],
                first_sample_time=raw_dict["first_sample_time"],
                channel_ids=raw_dict["channel_ids"]
            )
        elif raw_dict.get("type") == "SignalTimeChunk":
            return SignalTimeChunk(
                sample_data=np.array(raw_dict["signal_data"]),
                timestamps=np.array(raw_dict["timestamps"]),
                channel_ids=raw_dict["channel_ids"],
                sample_frequency=raw_dict["sample_frequency"]
            )
        else:
            raise TypeError(f"Unsupported signal chunk type: {raw_dict.get('type')}")

    def dump_trial(self, trial: Trial) -> dict:
        raw_dict = {
            "start_time": trial.start_time,
            "end_time": trial.end_time,
            "wrt_time": trial.wrt_time
        }

        if trial.numeric_events:
            raw_dict["numeric_events"] = {
                name: self.dump_numeric_event_list(event_list) for name, event_list in trial.numeric_events.items()
            }

        if trial.text_events:
            raw_dict["text_events"] = {
                name: self.dump_text_event_list(event_list) for name, event_list in trial.text_events.items()
            }

        if trial.signals:
            raw_dict["signals"] = {
                name: self.dump_signal_chunk(signal_chunk) for name, signal_chunk in trial.signals.items()
            }

        if trial.enhancements:
            raw_dict["enhancements"] = trial.enhancements

        if trial.categories:
            raw_dict["categories"] = trial.categories

        return raw_dict

    def load_trial(self, raw_dict) -> Trial:
        numeric_events = {
            name: self.load_numeric_event_list(event_data)
            for name, event_data in raw_dict.get("numeric_events", {}).items()
        }

        text_events = {
            name: self.load_text_event_list(event_data)
            for name, event_data in raw_dict.get("text_events", {}).items()
        }

        signals = {
            name: self.load_signal_chunk(signal_data)
            for name, signal_data in raw_dict.get("signals", {}).items()
        }

        trial = Trial(
            start_time=raw_dict["start_time"],
            end_time=raw_dict["end_time"],
            wrt_time=raw_dict["wrt_time"],
            numeric_events=numeric_events,
            text_events=text_events,
            signals=signals,
            enhancements=raw_dict.get("enhancements", {}),
            categories=raw_dict.get("categories", {})
        )
        return trial


class Hdf5TrialFile(TrialFile):
    """HDF5-based trial file using one top-level group per trial.

    The trial file should be loadable from many environments, including:
     - Python: https://docs.h5py.org/en/latest/quick.html
     - Matlab: https://www.mathworks.com/help/matlab/ref/h5read.html
    """

    def __init__(self, file_name: str, truncate: bool = False) -> None:
        self.file_name = file_name
        self.create_empty = truncate

    def __enter__(self) -> Self:
        if self.create_empty:
            with h5py.File(self.file_name, "w"):
                logging.info(f"Creating empty HDF5 trial file: {self.file_name}")
        return self

    def append_trial(self, trial: Trial) -> None:
        with h5py.File(self.file_name, "a") as f:
            group_name = f"trial_{len(f.keys()):04d}"
            trial_group = f.create_group(group_name, track_order=True)
            self.dump_trial(trial, trial_group)

    def read_trials(self) -> Iterator[Trial]:
        with h5py.File(self.file_name, "r") as f:
            for trial_group in f.values():
                yield self.load_trial(trial_group)

    def dump_numeric_event_list(
        self,
        numeric_event_list: NumericEventList,
        name: str,
        numeric_events_group: h5py.Group
    ) -> None:
        if numeric_event_list.event_data.size > 1:
            numeric_events_group.create_dataset(name, data=numeric_event_list.event_data, compression="gzip")
        else:
            numeric_events_group.create_dataset(name, data=numeric_event_list.event_data)

    def load_numeric_event_list(self, dataset: h5py.Dataset) -> NumericEventList:
        return NumericEventList(np.array(dataset[()]))

    def dump_text_event_list(
        self,
        text_event_list: TextEventList,
        name: str,
        text_events_group: h5py.Group
    ) -> None:
        subgroup = text_events_group.create_group(name)
        encoded_text = np.char.encode(text_event_list.text_data, 'UTF-8')
        if encoded_text.size > 1:
            subgroup.create_dataset("timestamp_data", data=text_event_list.timestamp_data, compression="gzip")
            subgroup.create_dataset("text_data", data=encoded_text, compression="gzip")
        else:
            subgroup.create_dataset("timestamp_data", data=text_event_list.timestamp_data)
            subgroup.create_dataset("text_data", data=encoded_text)

    def load_text_event_list(self, subgroup: h5py.Group) -> TextEventList:
        text_data = subgroup["text_data"][()]
        if text_data.size < 1:
            decoded_text = np.empty([0,], dtype=np.str_)
        else:
            decoded_text = np.char.decode(text_data, 'UTF-8')
        return TextEventList(
            timestamp_data=np.array(subgroup["timestamp_data"][()]),
            text_data=decoded_text
        )

    def dump_signal_chunk(self, signal_chunk, name: str, signals_group: h5py.Group) -> dict:
        if isinstance(signal_chunk, SignalChunk):
            if signal_chunk.sample_data.size > 1:
                dataset = signals_group.create_dataset(name, data=signal_chunk.sample_data, compression="gzip")
            else:
                dataset = signals_group.create_dataset(name, data=signal_chunk.sample_data)
            dataset.attrs["type"] = "SignalChunk"
            if signal_chunk.sample_frequency is None:
                dataset.attrs["sample_frequency"] = np.empty([0, 0])
            else:
                dataset.attrs["sample_frequency"] = signal_chunk.sample_frequency
            if signal_chunk.first_sample_time is None:
                dataset.attrs["first_sample_time"] = np.empty([0, 0])
            else:
                dataset.attrs["first_sample_time"] = signal_chunk.first_sample_time
            dataset.attrs["channel_ids"] = signal_chunk.channel_ids
        elif isinstance(signal_chunk, SignalTimeChunk):
            if signal_chunk.sample_data.size > 1:
                dataset = signals_group.create_dataset(name, data=signal_chunk.sample_data, compression="gzip")
            else:
                dataset = signals_group.create_dataset(name, data=signal_chunk.sample_data)
            dataset.attrs["type"] = "SignalTimeChunk"
            dataset.attrs["timestamps"] = signal_chunk.timestamps
            if signal_chunk.sample_frequency is None:
                dataset.attrs["sample_frequency"] = np.empty([0, 0])
            else:
                dataset.attrs["sample_frequency"] = signal_chunk.sample_frequency
            dataset.attrs["channel_ids"] = signal_chunk.channel_ids
        else:
            raise TypeError(f"Unsupported signal chunk type: {type(signal_chunk)}")

    def load_signal_chunk(self, dataset: h5py.Dataset):
        chunk_type = dataset.attrs.get("type", None)
        if chunk_type == "SignalChunk":
            if dataset.attrs["sample_frequency"].size < 1:
                sample_frequency = None
            else:
                sample_frequency = dataset.attrs["sample_frequency"]
            if dataset.attrs["first_sample_time"].size < 1:
                first_sample_time = None
            else:
                first_sample_time = dataset.attrs["first_sample_time"]
            return SignalChunk(
                sample_data=np.array(dataset[()]),
                sample_frequency=sample_frequency,
                first_sample_time=first_sample_time,
                channel_ids=dataset.attrs["channel_ids"].tolist()
            )
        elif chunk_type == "SignalTimeChunk":
            if dataset.attrs["sample_frequency"].size < 1:
                sample_frequency = None
            else:
                sample_frequency = dataset.attrs["sample_frequency"]
            timestamps = dataset.attrs["timestamps"]
            return SignalTimeChunk(
                sample_data=np.array(dataset[()]),
                timestamps=np.array(timestamps),
                channel_ids=dataset.attrs["channel_ids"].tolist(),
                sample_frequency=sample_frequency
            )
        else:
            raise TypeError(f"Unsupported signal chunk type: {chunk_type}")

    def dump_trial(self, trial: Trial, trial_group: h5py.Group) -> None:
        trial_group.attrs["start_time"] = trial.start_time
        if trial.end_time is None:
            trial_group.attrs["end_time"] = np.empty([0, 0])
        else:
            trial_group.attrs["end_time"] = trial.end_time
        trial_group.attrs["wrt_time"] = trial.wrt_time

        if trial.numeric_events:
            numeric_events_group = trial_group.create_group("numeric_events")
            for name, event_list in trial.numeric_events.items():
                self.dump_numeric_event_list(event_list, name, numeric_events_group)

        if trial.text_events:
            text_events_group = trial_group.create_group("text_events")
            for name, event_list in trial.text_events.items():
                self.dump_text_event_list(event_list, name, text_events_group)

        if trial.signals:
            signals_group = trial_group.create_group("signals")
            for name, signal_chunk in trial.signals.items():
                self.dump_signal_chunk(signal_chunk, name, signals_group)

        if trial.enhancements:
            enhancements_json = json.dumps(trial.enhancements)
            trial_group.attrs["enhancements"] = enhancements_json

        if trial.categories:
            categories_json = json.dumps(trial.categories)
            trial_group.attrs["categories"] = categories_json

    def load_trial(self, trial_group: h5py.Group) -> Trial:
        numeric_events = {}
        numeric_events_group = trial_group.get("numeric_events", None)
        if numeric_events_group:
            for name, dataset in numeric_events_group.items():
                numeric_events[name] = self.load_numeric_event_list(dataset)

        text_events = {}
        text_events_group = trial_group.get("text_events", None)
        if text_events_group:
            for name, subgroup in text_events_group.items():
                text_events[name] = self.load_text_event_list(subgroup)

        signals = {}
        signals_group = trial_group.get("signals", None)
        if signals_group:
            for name, dataset in signals_group.items():
                signals[name] = self.load_signal_chunk(dataset)

        enhancements_json = trial_group.attrs.get("enhancements", None)
        if enhancements_json:
            enhancements = json.loads(enhancements_json)
        else:
            enhancements = {}

        categories_json = trial_group.attrs.get("categories", None)
        if categories_json:
            categories = json.loads(categories_json)
        else:
            categories = {}

        if trial_group.attrs["end_time"].size < 1:
            end_time = None
        else:
            end_time = trial_group.attrs["end_time"]
        trial = Trial(
            start_time=trial_group.attrs["start_time"],
            end_time=end_time,
            wrt_time=trial_group.attrs["wrt_time"],
            numeric_events=numeric_events,
            text_events=text_events,
            signals=signals,
            enhancements=enhancements,
            categories=categories
        )
        return trial
