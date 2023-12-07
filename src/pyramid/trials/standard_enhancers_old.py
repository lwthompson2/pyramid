from typing import Any
from numpy import bool_
import csv
import os
import ctypes
import numpy as np
import time

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

""" Saccade detection, see:
    https://link.springer.com/article/10.3758/s13428-019-01304-3
    https://github.com/richardschweitzer/OnlineSaccadeDetection
"""
### this is the class for the returned structure of the detection algorithm
class detection_results(ctypes.Structure):
    _fields_ = [('sac_detected', ctypes.c_double),
                ('sac_t', ctypes.c_double), 
                ('sac_vx', ctypes.c_double), 
                ('sac_vy', ctypes.c_double), 
                ('threshold_vx', ctypes.c_double), 
                ('threshold_vy', ctypes.c_double), 
                ('sac_t_onset', ctypes.c_double) ]

### this is the detection module
class online_sac_detect:
    
    def __init__(self, lib_name='detect_saccade.so', lib_path=''):
        # print('Loading online saccade detection Python module ' + __name__ + ' (by Richard Schweitzer)')
        print('Loading online saccade detection Python module (by Richard Schweitzer)')
        # load the C file
        if lib_path=='':
            lib_path = os.path.dirname(os.path.realpath(__file__)) + '/'
        self.lib_name = lib_name
        self.lib_path = lib_path
        # print('Now CDLL-loading: ' + self.lib_path + self.lib_name)
        self.lib_sac_detect = ctypes.CDLL(self.lib_path + self.lib_name)
        # define the results type of the detect function 
        self.lib_sac_detect.run_detection.restype = detection_results 
        # not preallocate
        self.x = np.array([])
        self.y = np.array([])
        self.t = np.array([])
        self.current_n_samples = 0
        # set the default parameters:
        # print('Loading default parameters... Call set_parameters() to change!')
        self.set_parameters(print_parameters=False)
        # print('Ready to go! Now you only have to import some data via add_data()!')

    def set_parameters(self, thres_fac=10, above_thres_needed=3, 
                       restrict_dir_min=0, restrict_dir_max=0,
                       samp_rate=0, anchor_vel_thres=10, print_results=0, 
                       print_parameters=False):
        self.thres_fac = thres_fac
        self.above_thres_needed = above_thres_needed
        self.restrict_dir_min = restrict_dir_min
        self.restrict_dir_max = restrict_dir_max
        self.samp_rate = samp_rate
        self.anchor_vel_thres = anchor_vel_thres
        self.print_results = print_results
        if print_parameters:
            print('Sac detect parameters are now: thres_fac=' + str(self.thres_fac) +  
                  ' above_thres_needed=' + str(self.above_thres_needed) + 
                  ' restrict_dir min max =' + str([self.restrict_dir_min, self.restrict_dir_max]) + 
                  ' samp_rate=' + str(self.samp_rate) + 
                  ' anchor_vel_thres=' + str(self.anchor_vel_thres) + 
                  ' print_results=' + str(self.print_results) )
    
    def get_parameters(self):
        return(self.thres_fac, self.above_thres_needed, 
            self.restrict_dir_min, self.restrict_dir_max, 
            self.samp_rate, self.anchor_vel_thres, self.print_results)
    
    def reset_data(self):
        self.x = np.array([], dtype=float)
        self.y = np.array([], dtype=float)
        self.t = np.array([], dtype=float)
        self.current_n_samples = 0
    
    def add_data(self, x, y, t):
        self.x = np.append(self.x, np.array(x, dtype=float))
        self.y = np.append(self.y, np.array(y, dtype=float))
        self.t = np.append(self.t, np.array(t, dtype=float))
        assert(np.size(x)==np.size(y))
        assert(np.size(x)==np.size(t))
        self.current_n_samples = len(self.x)
    
    def return_data(self):
        return(self.x, self.y, self.t)
    
    def run_detection(self):
        x_p = self.x.ctypes.data_as(ctypes.c_void_p)
        y_p = self.y.ctypes.data_as(ctypes.c_void_p)
        t_p = self.t.ctypes.data_as(ctypes.c_void_p)
        if np.size(self.x) < (2*self.above_thres_needed):
            print('WARNING: You run the detection without having more than twice the amount of samples needed!')
        t0 = time.time()
        res_here = self.lib_sac_detect.run_detection( x_p, y_p, t_p, 
                                 self.thres_fac, self.above_thres_needed, 
                                 self.restrict_dir_min, self.restrict_dir_max,
                                 self.samp_rate, self.anchor_vel_thres, self.print_results, 
                                 self.current_n_samples ) 
        run_time_here = (time.time() - t0) * 1000 # in ms
        return(res_here, run_time_here)

class SaccadesEnhancer(TrialEnhancer):
    """Standard way of parsing saccades from the eye position traces in a trial

    Args:
        expression:     string Python expression to evaluate for each trial as a TrialExpression
        value_name:     name of the enhancement to add to each trial, with the expression value
        value_category: optional category to go with value_name (default is "value")
        default_value:  default value to return in case of expression evaluation error (default is None)
    """

    def __init__(
        self,
        max_saccades: int = 2,
        center_at_fp: bool = True,
        x_buffer_name: str = "gaze_x",
        y_buffer_name: str = "gaze_y",
        fp_off_name: str = "fp_off",
        fp_x_name: str = "fp_x",
        fp_y_name: str = "fp_y",
        max_time_ms: float = 2000,
        target_locations: list = [],
        thres_fac: int = 10, # lambda, velocity criterion, usu 5,10,15,20 (higher is more conservative)        
        above_thres_needed: int = 3, # k, usu 1,2,3,4 (higher is more conservative)
        restrict_dir_min: int = 0, # direction restriction in degrees
        restrict_dir_max: int = 0, # direction restriction in degrees
        samp_rate: int = 0,
        saccades_name: str = "saccades",
        saccades_category: str = "saccades",
        broken_fixation_name: str = "bf",
        broken_fixation_category: str = "id",
    ) -> None:
        self.max_saccades = max_saccades
        self.center_at_fp = center_at_fp
        self.x_buffer_name = x_buffer_name
        self.y_buffer_name = y_buffer_name
        self.fp_off_name = fp_off_name
        self.fp_x_name = fp_x_name
        self.fp_y_name = fp_y_name
        self.max_time_ms = max_time_ms
        self.target_locations = target_locations
        self.saccades_name = saccades_name
        self.saccades_category = saccades_category
        self.broken_fixation_name = broken_fixation_name
        self.broken_fixation_category = broken_fixation_category

        # Setup saccade detector
        self.saccade_detector = online_sac_detect()
        self.saccade_detector.set_parameters(thres_fac, above_thres_needed, 
                               restrict_dir_min, restrict_dir_max, samp_rate)        

    def enhance(self, trial: Trial, trial_number: int, experiment_info: dict, subject_info: dict) -> None:

        # Use trial.get_one() to get the time of the first occurence of the named "time" event.
        fp_off_time = trial.get_one(self.fp_off_name)

        # Use trial.signals for gaze signal chunks.
        x_signal = trial.signals[self.x_buffer_name]
        y_signal = trial.signals[self.y_buffer_name]
        if x_signal.get_end_time() < fp_off_time or y_signal.get_end_time() < fp_off_time:
            return

        # Possibly center at fp
        if self.center_at_fp is True:
            x_signal.apply_offset_then_gain(x_signal.copy_time_range(fp_off_time, fp_off_time).get_channel_values(), 1)
            y_signal.apply_offset_then_gain(y_signal.copy_time_range(fp_off_time, fp_off_time).get_channel_values(), 1)

        # Clear and then add all of the data to the saccade detector
        self.saccade_detector.reset_data()
        self.saccade_detector.add_data(
            x_signal.copy_time_range(fp_off_time, fp_off_time + self.max_time_ms).get_channel_values(),
            y_signal.copy_time_range(fp_off_time, fp_off_time + self.max_time_ms).get_channel_values(),
            y_signal.copy_time_range(fp_off_time, fp_off_time + self.max_time_ms).get_sample_times())
                            
        # Keep running the detector until if/when we get the saccade we're looking for
        saccade_countdown = self.max_saccades
        while saccade_countdown > 0

            # Run the detector
            res_here, run_time_here = self.saccade_detector.run_detection()

            # Check the result
            if res_here.sac_detected is True:
                
                # If targets are given, 

            else:
                saccade_countdown = 0

        # Find the choice saccade

        # Placeholder: start a bogus "saccade" at fp_off_time, and end it 1 second later.
        arbitrary_duration = 1.0
        x_values = x_signal.copy_time_range(fp_off_time, fp_off_time + arbitrary_duration).get_channel_values()
        x_start = x_values[0]
        x_end = x_values[-1]
        y_values = y_signal.copy_time_range(fp_off_time, fp_off_time + arbitrary_duration).get_channel_values()
        y_start = y_values[0]
        y_end = y_values[-1]

        x_displacement = x_end - x_start
        y_displacement = y_end - y_start
        raw_distance = math.sqrt(x_displacement ** 2 + y_displacement ** 2)

        # Represent each saccade as a dictionary that has certain keys by convention.
        example_saccade = {
             "t_start": fp_off_time,
             "t_end": fp_off_time + arbitrary_duration,
             "v_max": raw_distance / arbitrary_duration,
             "v_avg": raw_distance / arbitrary_duration,
             "x_start": x_start,
             "y_start": y_start,
             "x_end": x_end,
             "y_end": y_end,
             "raw_distance": raw_distance,
             "vector_distance": 1,
        }

        # Maybe produce a list of saccade dictionaries.
        # Lists of dicts can be added directly to trial enhancements.
        saccades = [example_saccade]
        trial.add_enhancement(self.saccades_name, saccades, self.saccades_category)

        # The same enhancer can also annotate broken fixation or not.
        trial.add_enhancement(self.broken_fixation_name, False, self.broken_fixation_category)
