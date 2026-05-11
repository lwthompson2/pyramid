from typing import Any
from numbers import Number
import logging
import csv
from pathlib import Path

import numpy as np
from scipy.ndimage import gaussian_filter1d

from pyramid.file_finder import FileFinder
from pyramid.model.events import NumericEventList, TextEventList
from pyramid.trials.trials import Trial, TrialEnhancer, TrialExpression


class PhyClusterInfoEnhancer(TrialEnhancer):
    """Persist selected Phy cluster metadata into trial enhancements.

    This enhancer reads Phy cluster CSV/TSV files once, caches metadata by cluster id,
    and for each trial writes metadata for clusters that appear in a configured
    NumericEventList buffer (default: "phy_clusters").
    """

    def __init__(
        self,
        file_finder: FileFinder,
        phy_params_file: str = None,
        cluster_buffer_name: str = "phy_clusters",
        enhancement_name: str = "phy_cluster_info",
        cluster_glob: str = "cluster_*",
        cluster_id_column: str = "cluster_id",
        cluster_delimiters: dict[str, str] = {".tsv": "\t", ".csv": ","},
        csv_dialect: str = "excel",
        **csv_fmtparams,
    ) -> None:
        self.phy_params_file = file_finder.find(phy_params_file) if phy_params_file else None
        self.cluster_buffer_name = cluster_buffer_name
        self.enhancement_name = enhancement_name
        self.cluster_glob = cluster_glob
        self.cluster_id_column = cluster_id_column
        self.cluster_delimiters = cluster_delimiters
        self.csv_dialect = csv_dialect
        self.csv_fmtparams = csv_fmtparams
        self.cluster_info = self._load_cluster_info() if self.phy_params_file else {}

    def _coerce_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, str):
            stripped = value.strip()
            if stripped == "":
                return ""
            lowered = stripped.lower()
            if lowered == "true":
                return True
            if lowered == "false":
                return False
            try:
                return int(stripped)
            except ValueError:
                pass
            try:
                return float(stripped)
            except ValueError:
                pass
            return stripped
        return value

    def _load_cluster_info(self) -> dict[int, dict[str, Any]]:
        if self.phy_params_file is None:
            return {}

        params_path = Path(self.phy_params_file)
        cluster_files = sorted(params_path.parent.glob(self.cluster_glob))
        cluster_info: dict[int, dict[str, Any]] = {}

        for cluster_file in cluster_files:
            delimiter = self.cluster_delimiters.get(cluster_file.suffix.lower(), None)
            if delimiter is None:
                continue

            with open(cluster_file, mode="r", newline="") as f:
                reader = csv.DictReader(
                    f,
                    delimiter=delimiter,
                    dialect=self.csv_dialect,
                    **self.csv_fmtparams,
                )
                for row in reader:
                    if self.cluster_id_column not in row:
                        continue

                    cluster_id = int(row[self.cluster_id_column])
                    info = cluster_info.get(cluster_id, {})
                    for key, value in row.items():
                        info[key] = self._coerce_value(value)
                    cluster_info[cluster_id] = info

        return cluster_info

    def _get_runtime_cluster_info(self) -> dict[int, dict[str, Any]]:
        if self.cluster_info:
            return self.cluster_info

        try:
            from pyramid.neutral_zone.readers.phy import PhyClusterEventReader
        except Exception:
            return {}

        return dict(PhyClusterEventReader.latest_selected_cluster_info)

    def enhance(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict[str, Any],
        subject_info: dict[str, Any]
    ) -> None:
        event_list = trial.numeric_events.get(self.cluster_buffer_name, None)
        if event_list is None or event_list.event_data.size == 0:
            return

        if event_list.event_data.shape[1] < 2:
            return

        runtime_cluster_info = self._get_runtime_cluster_info()
        if not runtime_cluster_info:
            return

        trial_cluster_ids = np.unique(event_list.event_data[:, 1].astype(int))
        info_for_trial = {
            str(cluster_id): runtime_cluster_info[cluster_id]
            for cluster_id in trial_cluster_ids
            if cluster_id in runtime_cluster_info
        }

        if info_for_trial:
            trial.add_enhancement(self.enhancement_name, info_for_trial, "spikes")


class TrialDurationEnhancer(TrialEnhancer):
    """A simple enhancer that computes trial duration, for demo and testing."""

    def __init__(self, default_duration: float = None) -> None:
        self.default_duration = default_duration

    def __eq__(self, other: object) -> bool:
        """Compare by attribute, to support use of this class in tests."""
        if isinstance(other, self.__class__):
            return self.default_duration == other.default_duration
        else:  # pragma: no cover
            return False

    def __hash__(self) -> int:
        """Hash by attribute, to support use of this class in tests."""
        return self.default_duration.__hash__()

    def enhance(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        if trial.end_time is None:
            duration = None
        else:
            duration = trial.end_time - trial.start_time
        trial.add_enhancement("duration", duration, "value")


class PairedCodesEnhancer(TrialEnhancer):
    """Look for pairs of numeric events that represent property-value pairs.

    buffer_name is the name of a buffer of NumericEventList.

    rules_csv is one or more .csv files where each row contains a rule for how to extract a property from the
    named buffer.  Each .csv must have the following columns:

        - "type": Used to select relevant rows of the .csv, and also the trial category to
                  use for each property.  By defalt only types "id" and "value" will be used.
                  Pass in rule_types to change this default.
        - "value": a numeric value that represents a property, for example 1010
        - "name": the string name to use for the property, for example "fp_on"
        - "min": the smallest event value to consier when looking for the property's value events
        - "max": the largest event value to consier when looking for the property's value events
        - "base": the base value to subtract from the property's value events, for example 7000
        - "scale": how to scale each event value after subtracting its base, for example 0.1

    Each .csv may contain additional columns, which will be ignored (eg a "comment" column).

    file_finder is a utility to find() files in the conigured Pyramid configured search path.
    Pyramid will automatically create and pass in the file_finder for you.

    value_index is which event value to look for, in the NumericEventList
    (default is 0, the first value for each event).

    rule_types is a list of strings to match against the .csv "type" column.
    The default is ["id", "value"].

    dialect and any additional fmtparams are passed on to the .csv reader.
    """

    def __init__(
        self,
        buffer_name: str,
        rules_csv: str | list[str],
        file_finder: FileFinder,
        value_index: int = 0,
        rule_types: list[str] = ["id", "value"],
        dialect: str = 'excel',
        **fmtparams
    ) -> None:
        self.buffer_name = buffer_name
        if isinstance(rules_csv, list):
            self.rules_csv = [file_finder.find(file) for file in rules_csv]
        else:
            self.rules_csv = [file_finder.find(rules_csv)]
        self.value_index = value_index
        self.rule_types = rule_types
        self.dialect = dialect
        self.fmtparams = fmtparams

        rules = {}
        for rules_csv in self.rules_csv:
            with open(rules_csv, mode='r', newline='') as f:
                csv_reader = csv.DictReader(f, dialect=self.dialect, **self.fmtparams)
                for row in csv_reader:
                    if row['type'] in self.rule_types:
                        value = float(row['value'])
                        rules[value] = {
                            'type': row['type'],
                            'name': row['name'],
                            'base': float(row['base']),
                            'min': float(row['min']),
                            'max': float(row['max']),
                            'scale': float(row['scale']),
                        }
        self.rules = rules

    def enhance(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        event_list = trial.numeric_events[self.buffer_name]
        for value, rule in self.rules.items():
            # Did / when did this trial contain events indicating this rule/property?
            property_times = event_list.times(value, self.value_index)
            if property_times is not None and property_times.size > 0:
                # Get potential events that hold values for the indicated rule/property.
                value_list = event_list.copy_value_range(min=rule['min'], max=rule['max'], value_index=self.value_index)
                value_list.apply_offset_then_gain(-rule['base'], rule['scale'])
                for property_time in property_times:
                    # For each property event, pick the soonest value event that follows.
                    values = value_list.values(start_time=property_time, value_index=self.value_index)
                    if values.size > 0:
                        trial.add_enhancement(rule['name'], values[0], rule['type'])


class EventTimesEnhancer(TrialEnhancer):
    """Look for times when named events occurred.

    buffer_name is the name of a buffer of NumericEventList.

    rules_csv is one or more .csv files where each row contains a rule for how to extract events from the
    named buffer.  Each .csv must have the following columns:

        - "type": Used to select relevant rows of the .csv, and also the trial category to
                  use for each property.  By defalt only the type "time" will be used.
                  Pass in rule_types to change this default.
        - "value": a numeric value that represents a property, for example 1010
        - "name": the string name to use for the property, for example "fp_on"

    Each .csv may contain additional columns, which will be ignored (eg a "comment" column).

    file_finder is a utility to find() files in the conigured Pyramid configured search path.
    Pyramid will automatically create and pass in the file_finder for you.

    value_index is which event value to look for, in the NumericEventList
    (default is 0, the first value for each event).

    rule_types is a list of strings to match against the .csv "type" column.
    The default is ["time"].

    dialect and any additional fmtparams are passed on to the .csv reader.
    """

    def __init__(
        self,
        buffer_name: str,
        rules_csv: str | list[str],
        file_finder: FileFinder,
        value_index: int = 0,
        rule_types: list[str] = ["time"],
        dialect: str = 'excel',
        **fmtparams
    ) -> None:
        self.buffer_name = buffer_name
        if isinstance(rules_csv, list):
            self.rules_csv = [file_finder.find(file) for file in rules_csv]
        else:
            self.rules_csv = [file_finder.find(rules_csv)]
        self.value_index = value_index
        self.rule_types = rule_types
        self.dialect = dialect
        self.fmtparams = fmtparams

        rules = {}
        for rules_csv in self.rules_csv:
            with open(rules_csv, mode='r', newline='') as f:
                csv_reader = csv.DictReader(f, dialect=self.dialect, **self.fmtparams)
                for row in csv_reader:
                    if row['type'] in self.rule_types:
                        value = float(row['value'])
                        rules[value] = {
                            'type': row['type'],
                            'name': row['name'],
                        }
        self.rules = rules

    def enhance(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        event_list = trial.numeric_events[self.buffer_name]
        for value, rule in self.rules.items():
            # Did / when did this trial contain events of interest with the requested value?
            event_times = event_list.times(value, self.value_index)
            trial.add_enhancement(rule['name'], event_times.tolist(), rule['type'])


class ExpressionEnhancer(TrialEnhancer):
    """Evaluate a TrialExpression for each trial and add the result as a named enhancement.

    Args:
        expression:     string Python expression to evaluate for each trial as a TrialExpression
        value_name:     name of the enhancement to add to each trial, with the expression value
        value_category: optional category to go with value_name (default is "value")
        default_value:  default value to return in case of expression evaluation error (default is None)
    """

    def __init__(
        self,
        expression: str,
        value_name: str,
        value_category: str = "value",
        default_value: Any = None,
    ) -> None:
        self.trial_expression = TrialExpression(expression=expression, default_value=default_value)
        self.value_name = value_name
        self.value_category = value_category

    def enhance(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        value = self.trial_expression.evaluate(trial)

        # Many numpy types are json-serializable like standard Python float, int, etc. -- but not numpy.bool_!
        if isinstance(value, np.bool_):
            value = bool(value)

        trial.add_enhancement(self.value_name, value, self.value_category)


class TextKeyValueEnhancer(TrialEnhancer):
    """Parse text events for key-value pairs, group results by name, add to the trial as event lists.

    Each event should have text_data formated like these:

        - "name=<name>, value=<value>, type=<type>"
        - "name=fpon"
        - "name=fp_x, value=0, type=double"

    In general, the expected format is:

        - Event text_data is one list of **entries** that are separated by commas (",").
        - Each **entry** is a key-value pair with **key** and **value** separted by an equals sign ("=").
        - Both **key** and **value** can have surrounding whitespace, which will be trimmed.

    This enhancer will transform text events from the named buffer by parsing out keys and values from the
    event text.  It will group parsed events by name and add them back to the trial in groups of numeric or
    or text events.

    It parses each event like this:

        - Look for an entry with **key** "name".  Use its **value** as the name of the trial enhancement.
            - If there is no "name" entry, skip the text event.
        - Look for an entry with **key** "value".  Parse its **value** to get the value for the enhancement.
            - If there is no "value" entry, take the event's timestamp as the value.
        - Look for an entry with **key** "type".  If present, this can be a hint for how to parse the "value" entry:
            - type=float or type=double -> Try to parse "value" as a Python float.
            - type=int or type=long -> Try to parse "value" as a Python int.
            - type=str -> Take "value" as-is as a Python str.
            - no "type" entry or unknown type -> Take "value" as-is as a Python str.

    The literal delimiters and keys used for parsing can be cusomized via constructer args.

    Args:
        buffer_name:            Name of a trial TextEventList buffer to read from.
        entry_delimiter:        How parse data into entries (default is ",")
        key_value_delimiter:    How to parse each entry into a **key** and **value** (default is "=")
        name_key:               Key to use for the name of each enhancement (default is "name")
        value_key:              Key to use for the value of each enhancement (default is "value")
        type_key:               Key to use for the type of each value (default is "type")
        float_types:            List of types to parse as Python floats (default is ["float", "double"])
        float_default:          Default value to use when float parsing fails (default is 0.0)
        int_types:              List of types to parse as Python ints (default is ["int", "long"])
        int_default:            Default value to use when int parsing fails (default is 0)
    """

    def __init__(
        self,
        buffer_name: str,
        entry_delimiter: str = ",",
        key_value_delimiter: str = "=",
        name_key: str = "name",
        value_key: str = "value",
        type_key: str = "type",
        float_types: list[str] = ["float", "double"],
        float_default: float = 0.0,
        int_types: list[str] = ["int", "long"],
        int_default: int = 0,
    ) -> None:
        self.buffer_name = buffer_name
        self.entry_delimiter = entry_delimiter
        self.key_value_delimiter = key_value_delimiter
        self.name_key = name_key
        self.value_key = value_key
        self.type_key = type_key
        self.float_types = float_types
        self.float_default = float_default
        self.int_types = int_types
        self.int_default = int_default

    def enhance(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        # Parse each text event from the named buffer.
        event_list = trial.text_events[self.buffer_name]

        # Group event values and categories by name.
        events_by_name = {}
        for index in range(event_list.event_count()):
            # Parse out a name and a value for this event.
            timestamp = event_list.timestamp_data[index]
            text = str(event_list.text_data[index])
            key_value_entries = self.parse_entries(text)
            name = key_value_entries.get(self.name_key, None)
            if name is None:
                logging.warning(f"Skipping text that has no name key '{self.name_key}': {text}")
                continue
            value = self.parse_value(key_value_entries, timestamp)

            # Group parsed event info by name.
            if name in events_by_name:
                events_by_name[name]["timestamps"].append(timestamp)
                events_by_name[name]["values"].append(value)
            else:
                events_by_name[name] = {
                    "timestamps": [timestamp],
                    "values": [value]
                }

        # For each name group create a text or numeric event list.
        for name, data in events_by_name.items():
            timestamps = data["timestamps"]
            values = data["values"]
            if isinstance(values[0], str):
                timestamp_data = np.array(timestamps)
                text_data = np.array(values, dtype=np.str_)
                text_event_list = TextEventList(timestamp_data, text_data)
                trial.add_enhancement(name, text_event_list)
            else:
                event_data = np.stack([timestamps, values]).T
                numeric_event_list = NumericEventList(event_data)
                trial.add_enhancement(name, numeric_event_list)

    def parse_entries(self, text: str) -> dict[str, str]:
        entries = text.split(self.entry_delimiter)
        info = {}
        for entry in entries:
            parts = entry.split(self.key_value_delimiter, maxsplit=1)
            if len(parts) != 2:
                logging.warning(f"Unable to parse key-value text: {entry}")
                continue
            key = parts[0].strip()
            value = parts[1].strip()
            info[key] = value
        return info

    def parse_value(self, info: dict[str, str], timestamp: float) -> Any:
        raw_value = info.get(self.value_key, None)
        if raw_value is None:
            # Use the event's timesetamp as the value.
            return timestamp

        # Parse out a value based on the type, if provided.
        type = info.get(self.type_key, None)
        if type in self.int_types:
            try:
                return int(raw_value)
            except Exception:
                logging.warning(f"Could not parse value as int, using default ({self.int_default}): {raw_value}")
                return self.int_default
        elif type in self.float_types:
            try:
                return float(raw_value)
            except Exception:
                logging.warning(f"Could not parse value as float, using default ({self.float_default}): {raw_value}")
                return self.float_default
        else:
            return raw_value


class SaccadesEnhancer(TrialEnhancer):
    """Parse saccades from the x,y eye position traces in a trial using velocity and acceleration thresholds.

    Args:
        max_saccades:                           Parse this number of saccades, at most (default 1).
        center_at_fp:                           Whether to re-zero gaze position at fp off time (default True).
        x_buffer_name:                          Name of a Trial buffer with gaze signal data (default "gaze").
        x_channel_id:                           Channel id to use within x_buffer_name (default "x").
        y_buffer_name:                          Name of a Trial buffer with gaze signal data (default "gaze").
        y_channel_id:                           Channel id to use within y_buffer_name (default "y").
        fp_off_name:                            Name of a trial enhancement with time to start saccade parsing (default "fp_off").
        all_off_name:                           Name of a trial enhancement with time to end saccade parsing (default "all_off").
        fp_x_name:                              Name of a trial enhancement with fixation x position (default "fp_x").
        fp_y_name:                              Name of a trial enhancement with fixation y position (default "fp_y").
        position_smoothing_kernel_size_ms:      Width of kernel to smooth gaze position samples (default 0 -- ie no smoothing).
        velocity_smoothing_kernel_size_ms:      Width of kernel to smooth gaze velocity samples (default 10 -- ie smoothing).
        acceleration_smoothing_kernel_size_ms:  Width of kernel to smooth gaze acceleration samples (default 0 -- no ie smoothing).
        velocity_threshold_deg_per_s:           Threshold for detecting saccades by velocity in gaze deg/s (default 300).
        acceleration_threshold_deg_per_s2:      Threshold for detecting saccades by acceleration in gaze deg/s^2 (default 8).
        min_length_deg:                         Minimum length for a saccade to count in gaze deg (default 3.0).
        min_latency_ms: float = 10,             Minimum latency in ms that must elapse before a saccade for it to count (default 10).
        min_duration_ms: float = 5.0,           Minimum duration in ms of a saccade for it to count (default 5.0).
        max_duration_ms: float = 90.0,          Maximum duration in ms of a saccade for it to count (default 90.0).
        saccades_name:                          Trial enhancement name to use when adding detected saccades (default "saccades").
        saccades_category:                      Trial category to use when adding detected saccades (default "saccades").
    """

    def __init__(
        self,
        max_saccades: int = 1,
        center_at_fp: bool = True,
        x_buffer_name: str = "gaze",
        x_channel_id: str | int = "x",
        y_buffer_name: str = "gaze",
        y_channel_id: str | int = "y",
        fp_off_name: str = "fp_off",
        all_off_name: str = "all_off",
        fp_x_name: str = "fp_x",
        fp_y_name: str = "fp_y",
        position_smoothing_kernel_size_ms: int = 0,
        velocity_smoothing_kernel_size_ms: int = 10,
        acceleration_smoothing_kernel_size_ms: int = 0,
        velocity_threshold_deg_per_s: float = 300,
        acceleration_threshold_deg_per_s2: float = 8,
        min_length_deg: float = 3.0,
        min_latency_ms: float = 10,
        min_duration_ms: float = 5.0,
        max_duration_ms: float = 90.0,
        saccades_name: str = "saccades",
        saccades_category: str = "saccades"
    ) -> None:
        self.max_saccades = max_saccades
        self.center_at_fp = center_at_fp
        self.x_buffer_name = x_buffer_name
        self.x_channel_id = x_channel_id
        self.y_buffer_name = y_buffer_name
        self.y_channel_id = y_channel_id
        self.fp_off_name = fp_off_name
        self.all_off_name = all_off_name
        self.fp_x_name = fp_x_name
        self.fp_y_name = fp_y_name
        self.position_smoothing_kernel_size_ms = position_smoothing_kernel_size_ms
        self.velocity_smoothing_kernel_size_ms = velocity_smoothing_kernel_size_ms
        self.acceleration_smoothing_kernel_size_ms = acceleration_smoothing_kernel_size_ms
        self.velocity_threshold_deg_per_s = velocity_threshold_deg_per_s
        self.acceleration_threshold_deg_per_s2 = acceleration_threshold_deg_per_s2
        self.min_length_deg = min_length_deg
        self.min_latency_ms = min_latency_ms
        self.min_duration_ms = min_duration_ms
        self.max_duration_ms = max_duration_ms
        self.saccades_name = saccades_name
        self.saccades_category = saccades_category

    def enhance(self, trial: Trial, trial_number: int, experiment_info: dict, subject_info: dict) -> None:

        # Get event times from trial enhancements to delimit saccade parsing.
        fp_off_time = trial.get_time(self.fp_off_name)
        all_off_time = trial.get_time(self.all_off_name)
        if fp_off_time is None or all_off_time is None:
            return

        # Use trial.signals for gaze signal chunks.
        if self.x_buffer_name not in trial.signals or self.y_buffer_name not in trial.signals:  # pragma: no cover
            return
        x_signal = trial.signals[self.x_buffer_name]
        y_signal = trial.signals[self.y_buffer_name]
        if x_signal.end() < fp_off_time or y_signal.end() < fp_off_time:  # pragma: no cover
            return

        # Possibly center at fp.
        x_channel_index = x_signal.channel_index(self.x_channel_id)
        y_channel_index = y_signal.channel_index(self.y_channel_id)
        if self.center_at_fp is True:
            x_signal.apply_offset_then_gain(-x_signal.at(fp_off_time, x_channel_index), 1)
            y_signal.apply_offset_then_gain(-y_signal.at(fp_off_time, y_channel_index), 1)

        # Get x,y data from the relevant time range, fp_off to all_off.
        x_position = x_signal.values(x_channel_index, fp_off_time, all_off_time)
        y_position = y_signal.values(y_channel_index, fp_off_time, all_off_time)

        # Possibly smooth position.
        if self.position_smoothing_kernel_size_ms > 0:
            # Convert kernel width in ms to a number of samples.
            kernel_width = self.position_smoothing_kernel_size_ms * x_signal.sample_frequency / 1000.0
            x_position = gaussian_filter1d(x_position, kernel_width)
            y_position = gaussian_filter1d(y_position, kernel_width)

        # Compute instantaneous velocity.
        dx = np.diff(x_position)
        dy = np.diff(y_position)
        distance = np.sqrt(dx**2 + dy**2)
        velocity = distance * x_signal.sample_frequency

        # Possibly smooth velocity.
        if self.velocity_smoothing_kernel_size_ms > 0:
            # Convert kernel width in ms to a number of samples.
            kernel_width = self.velocity_smoothing_kernel_size_ms * x_signal.sample_frequency / 1000.0
            velocity = gaussian_filter1d(velocity, kernel_width)

        # Compute instantaneous acceleration.
        acceleration = np.concatenate([[0], np.diff(velocity)])

        # Possibly smooth acceleration.
        if self.acceleration_smoothing_kernel_size_ms > 0:
            # Convert kernel width in ms to a number of samples.
            kernel_width = self.acceleration_smoothing_kernel_size_ms * x_signal.sample_frequency / 1000.0
            acceleration = gaussian_filter1d(acceleration, kernel_width)

        # Look for saccades!
        num_samples = len(distance)
        sample_index = 0
        saccades = []
        while (sample_index < num_samples) and (len(saccades) < self.max_saccades):

            # Reset indices.
            start_index = -1
            end_index = -1

            # Check for saccade, first try acceleration, then velocity treshold.
            if acceleration[sample_index] >= self.acceleration_threshold_deg_per_s2:

                # Crossed acceleration threshold.
                start_index = sample_index

                # Look for deceleration.
                while (sample_index < num_samples and
                       acceleration[sample_index] > -self.acceleration_threshold_deg_per_s2):
                    sample_index += 1

                # Look for end of deceleration.
                if (sample_index < num_samples and
                        (acceleration[sample_index] <= -self.acceleration_threshold_deg_per_s2 or
                         velocity[sample_index] <= self.velocity_threshold_deg_per_s)):
                    end_index = sample_index

            elif velocity[sample_index] >= self.velocity_threshold_deg_per_s:

                # Crossed velocity threshold.
                start_index = sample_index

                # Look for slowing.
                while (sample_index < num_samples and
                       velocity[sample_index] >= self.velocity_threshold_deg_per_s):
                    sample_index += 1

                # Look for end of super-threshold velocity.
                if (sample_index < num_samples and
                        velocity[sample_index] <= self.velocity_threshold_deg_per_s):
                    end_index = sample_index

            # Check if we found something.
            if (start_index != -1) and (end_index != 1):

                # Get start/end times wrt fixation onset.
                sac_start_time = fp_off_time + (start_index + 1) / x_signal.sample_frequency
                sac_end_time = fp_off_time + (end_index + 1) / x_signal.sample_frequency
                sac_duration = sac_end_time - sac_start_time

                # Saccade distance.
                sac_length = np.sqrt(
                    (x_position[end_index+1] - x_position[start_index+1])**2 +
                    (y_position[end_index+1] - y_position[start_index+1])**2)

                if (
                    (sac_length >= self.min_length_deg)
                    and (sac_duration <= self.max_duration_ms / 1000)
                    and (sac_duration >= self.min_duration_ms / 1000)
                    and (sac_start_time >= self.min_latency_ms / 1000)
                ):
                    # Save the saccade as a dictionary in the list of saccades.
                    saccades.append({
                        "t_start": sac_start_time,
                        "t_end": sac_end_time,
                        "v_max": np.max(velocity[start_index:end_index]),
                        "v_avg": sac_length / sac_duration,
                        "x_start": x_position[start_index + 1],
                        "y_start": y_position[start_index + 1],
                        "x_end": x_position[end_index + 1],
                        "y_end": y_position[end_index + 1],
                        "raw_distance": np.sum(velocity[start_index:end_index])/x_signal.sample_frequency,
                        "vector_distance": sac_length,
                    })

            # Update index to continue looking.
            sample_index += 1

        # Add the list of saccade dictionaries to trial enhancements.
        trial.add_enhancement(self.saccades_name, saccades, self.saccades_category)


class RenameRescaleEnhancer(TrialEnhancer):
    """Rename and optionally rescale trial buffers and enhancements based on rules declared in a .csv file.

    Args:
        rules_csv:      one or more .csv files where each row contains a rule for how to rename and rescale
                        buffers and enhancements.  Each .csv can use the following columns headers:

                            "value":    name of an existing trial buffer or enhancement, for example "1010"
                            "name":     new name to use for the same buffer or enhancement, for example "fp_on"
                            "scale":    optinal scale factor to apply to buffer values
                            "type":     a category to use for the named buffer or enhancement, for example "id" or "time"

        file_finder:    a utility to find() files in the conigured Pyramid configured search path.
                        Pyramid will automatically create and pass in the file_finder for you.
        dialect:        CSV dialect to pass on to the .csv reader
        fmtparams:      Additional format parameters to pass on to the .csv reader.

    The expected .csv column names "value", "name", "scale", and "type" were chosen to match the column names expected by
    PairedCodesEnhancer and EventTimesEnhancer.
    """

    def __init__(
        self,
        rules_csv: str | list[str],
        file_finder: FileFinder,
        dialect: str = 'excel',
        **fmtparams
    ) -> None:
        if isinstance(rules_csv, list):
            self.rules_csv = [file_finder.find(file) for file in rules_csv]
        else:
            self.rules_csv = [file_finder.find(rules_csv)]
        self.dialect = dialect
        self.fmtparams = fmtparams

        rules = {}
        for rules_csv in self.rules_csv:
            with open(rules_csv, mode='r', newline='') as f:
                csv_reader = csv.DictReader(f, dialect=self.dialect, **self.fmtparams)
                for row in csv_reader:
                    old_name = row['value']
                    new_name = row['name']
                    raw_scale = row.get('scale', None)
                    if raw_scale:
                        # Must be present (not None) and not empty.
                        scale = float(raw_scale)
                    else:
                        scale = None
                    new_category = row.get('type', None)
                    rules[old_name] = (new_name, scale, new_category)
        self.rules = rules

    def enhance(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:
        for old_name, (new_name, scale, new_category) in self.rules.items():
            if old_name in trial.signals:
                trial.signals[new_name] = trial.signals.pop(old_name)
                if scale is not None:
                    trial.signals[new_name].apply_offset_then_gain(gain=scale)

            if old_name in trial.numeric_events:
                trial.numeric_events[new_name] = trial.numeric_events.pop(old_name)
                if scale is not None:
                    trial.numeric_events[new_name].apply_offset_then_gain(gain=scale)

            if old_name in trial.text_events:
                trial.text_events[new_name] = trial.text_events.pop(old_name)

            if old_name in trial.enhancements:
                trial.enhancements[new_name] = trial.enhancements.pop(old_name)
                if scale is not None and isinstance(trial.enhancements[new_name], Number):
                    trial.enhancements[new_name] *= scale

            if new_category is None:
                # Rename the value in any category where it already appears.
                affected_categories = trial.remove_from_category(old_name)
                for category in affected_categories:
                    trial.add_to_category(new_name, category)
            else:
                # Rename the old name from any category where it already appears.
                trial.remove_from_category(old_name)

                # Add the new name to a new category.
                trial.add_to_category(new_name, new_category)
