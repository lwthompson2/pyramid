from typing import Any
from numpy import bool_
import csv

from pyramid.file_finder import FileFinder
from pyramid.trials.trials import Trial, TrialEnhancer, TrialExpression


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

        - "type": Used to select relevant rows of the .csv, and also the trial enhancement category to
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
            property_times = event_list.get_times_of(value, self.value_index)
            if property_times is not None and property_times.size > 0:
                # Get potential events that hold values for the indicated rule/property.
                value_list = event_list.copy_value_range(min=rule['min'], max=rule['max'], value_index=self.value_index)
                value_list.apply_offset_then_gain(-rule['base'], rule['scale'])
                for property_time in property_times:
                    # For each property event, pick the soonest value event that follows.
                    values = value_list.get_values(start_time=property_time, value_index=self.value_index)
                    if values.size > 0:
                        trial.add_enhancement(rule['name'], values[0], rule['type'])


class EventTimesEnhancer(TrialEnhancer):
    """Look for times when named events occurred.

    buffer_name is the name of a buffer of NumericEventList.

    rules_csv is one or more .csv files where each row contains a rule for how to extract events from the
    named buffer.  Each .csv must have the following columns:

        - "type": Used to select relevant rows of the .csv, and also the trial enhancement category to
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
            event_times = event_list.get_times_of(value, self.value_index)
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

        # Many numpy types are json-serializable like standard Python float, int, etc. -- But not numpy.bool_ !
        if isinstance(value, bool_):
            value = bool(value)

        trial.add_enhancement(self.value_name, value, self.value_category)
