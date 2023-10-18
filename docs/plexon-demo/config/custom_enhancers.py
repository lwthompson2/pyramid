from typing import Any
import math

from pyramid.trials.trials import Trial, TrialEnhancer


# We can define utility functions for the TrialEnhancer to use.
def ang_deg(x: float, y: float) -> float:
    """Compute an angle in degrees, in [0, 360)."""
    degrees = math.atan2(y, x) * 180 / math.pi
    return math.fmod(degrees + 360, 360)


def log10(x: float) -> float:
    """Compute log10 of x, allowing for log10(0.0) -> -inf."""
    if x == 0.0:
        return float("-inf")
    else:
        return math.log10(x)


# This is a rough version of the trial compute code from spmADPODR.m.
# It's incomplete and wrong!
# I'm hoping it shows the Pyramid version of how to get and set the same per-trial data as in FIRA.
class CustomEnhancer(TrialEnhancer):

    def enhance(
        self,
        trial: Trial,
        trial_number: int,
        experiment_info: dict[str: Any],
        subject_info: dict[str: Any]
    ) -> None:

        # Use trial.get_enhancement() to get values already set from ecode-rules.csv
        # via PairedCodesEnhancer and EventTimesEnhancer.

        task_id = trial.get_enhancement("task_id")

        # Use trial.add_enhancement() to set new values from custom computations.
        # You can set a category like "time", "id", or "value" (the default).

        t1_angle = ang_deg(trial.get_enhancement('t1_x'), trial.get_enhancement('t1_y'))
        trial.add_enhancement('t1_angle', t1_angle, "id")
        t2_angle = ang_deg(trial.get_enhancement('t2_x', 0), trial.get_enhancement('t2_y', 0))
        trial.add_enhancement('t2_angle', t2_angle, "id")

        if task_id == 1:

            # For MSAC, set target
            correct_target_angle = t1_angle
            correct_target = 1
            trial.add_enhancement("correct_target", correct_target, "id")

        elif task_id in (2, 3, 4, 5):

            # For ADPODR, figure out sample angle, correct/error target, LLR
            sample_angle = ang_deg(trial.get_enhancement("sample_x"), trial.get_enhancement("sample_y"))
            trial.add_enhancement("sample_angle", sample_angle)

            # parse trial id
            trial_id = trial.get_enhancement("trial_id") - 100 * task_id

            # Parse LLR
            # task_adaptiveODR3.c "Task Info" menu has P1-P9, which
            #   corresponds to the probability of showing the cue
            #   at locations far from (P1) or close to (P9) the true
            #   target

            # Pyramid passes in the subject_info, which we can check for conditoinal computations.
            # This info came from the command line as "--subject subject.yaml"

            # 0-8 for T1/T2, used below
            llr_id = int(trial_id) % 9
            if subject_info.get("subject_id") == "Cicero":
                if task_id == 2:
                    # ORDER: P1->P9
                    ps = [0.0, 0.05, 0.10, 0.10, 0.15, 0.15, 0.20, 0.15, 0.10]
                else:
                    ps = [0.0, 0.05, 0.10, 0.10, 0.15, 0.15, 0.20, 0.15, 0.10]
            else: # "MrM"
                ps = [0.0, 0.0, 0.0, 0.10, 0.20, 0.30, 0.15, 0.15, 0.10]

            if trial_id < 9:
                correct_target_angle = t1_angle
                error_target_angle = t2_angle
                correct_target = 1
                sample_id = llr_id - 4
                llr = log10(ps[llr_id + 1]) - log10(ps[-llr_id])
            else:
                correct_target_angle = t2_angle
                error_target_angle = t2_angle
                correct_target = 2
                sample_id = -(llr_id - 4)
                llr = log10(ps[-llr_id]) - log10(ps[llr_id + 1])

            trial.add_enhancement("correct_target", correct_target, "id")
            # neg are close to T1, pos are close to T2
            trial.add_enhancement("sample_id", sample_id, "id")
            # evidence for T1 (-) vs T2 (+)
            trial.add_enhancement("llr", llr)

        # Use trial.get_enhancement() to get saccade info already parsed by SaccadesEnhancer.
        broken_fixation = trial.get_enhancement("bf", True)
        saccades = trial.get_enhancement("saccades")
        score = -1
        if broken_fixation:
            score = -2
        elif not saccades or not math.isfinite(saccades[0]["t_start"]):
            score = -1
        else:
            # EventTimesEnhancer stores lists of named event times.
            # use trial.get_first() to get the first time as a scalar (or a default).
            trial.add_enhancement("score", -2, "id")

            # Choose one saccade to save -- very abridged and wrong!
            targ_acq = trial.get_one("targ_acq", 0) - trial.get_one("fp_off", 0)
            score = 0
            for saccade in saccades:
                if saccade["t_start"] > targ_acq:
                    score = 1
                    trial.add_enhancement("RT", saccade["t_start"])
                    trial.add_enhancement("scored_saccade", saccade, "saccades")

        # 1=correct, 0=error, -1=nc, -2=brfix,-3=sample
        trial.add_enhancement("score", score, "id")

        # Use trial.get_one() to grab the first timestamp from each "time" enchancement.
        score_times = [
            trial.get_one("online_brfix", default=None),
            trial.get_one("online_ncerr", default=None),
            trial.get_one("online_error", default=None),
            trial.get_one("online_correct", default=None),
        ]

        # We can use Python list comprehension to search for the non-None times.
        l_score = [index for index, time in enumerate(score_times) if time is not None]
        if l_score:
            # convert to -2 -> 1
            online_score = l_score[0] - 3
            # online score: 1=correct, 0=error, -1=nc, -2=brfix
            trial.add_enhancement("online_score", online_score)
            trial.add_enhancement("score_match", score == online_score)


# We could implement saccade parsing as a per-trial enhancer, either here or in standard_enhancers.py.
# This is a placeholder -- it's incomplete and wrong!
# The rough outline and parameters are taken from FIRA getFIRA_saccadesPerTrial.m and findSaccadesADPODR.m.
class SaccadesEnhancer(TrialEnhancer):

    def __init__(
        self,
        num_saccades: int = 2,
        recal: bool = True,
        horiz_buffer_name: str = "gaze_x",
        vert_buffer_name: str = "gaze_y",
        fp_off_name: str = "fp_off",
        fp_x_name: str = "fp_x",
        fp_y_name: str = "fp_y",
        max_time_ms: float = 1000,
        horiz_gain: float = 1.0,
        vert_gain: float = 1.0,
        d_min: float = 0.2, # minimum distance of a sacccade (deg)
        vp_min: float = 0.08, # minimum peak velocity of a saccade (deg/ms)
        vi_min: float = 0.03, # minimum instantaneous velocity of a saccade (deg/ms)
        a_min: float = 0.004, # minimum instantaneous acceleration of a saccade
        smf: list[float] = [0.0033, 0.0238, 0.0971, 0.2259, 0.2998, 0.2259, 0.0971, 0.0238, 0.0033], # for smoothing
        saccades_name: str = "saccades",
        saccades_category: str = "saccades",
        broken_fixation_name: str = "bf",
        broken_fixation_category: str = "id",
    ) -> None:
        self.num_saccades = num_saccades
        self.recal = recal
        self.horiz_buffer_name = horiz_buffer_name
        self.vert_buffer_name = vert_buffer_name
        self.fp_off_name = fp_off_name
        self.fp_x_name = fp_x_name
        self.fp_y_name = fp_y_name
        self.max_time_ms = max_time_ms
        self.horiz_gain = horiz_gain
        self.vert_gain = vert_gain

        self.d_min = d_min
        self.vp_min = vp_min
        self.vi_min = vi_min
        self.a_min = a_min
        
        self.smf = smf
        self.hsmf = (len(smf) - 1) / 2

        self.saccades_name = saccades_name
        self.saccades_category = saccades_category
        self.broken_fixation_name = broken_fixation_name
        self.broken_fixation_category = broken_fixation_category

    def enhance(self, trial: Trial, trial_number: int, experiment_info: dict, subject_info: dict) -> None:
        # This is only a placeholder to show how to access trial data and a way to represent saccade data.

        # We could have lots of *actual* saccade analysis code here!
        # It might be good to fold this code into Pyramid itself, and get it under test coverage.

        # Use trial.get_one() to get the time of the first occurence of the named "time" event.
        fp_off_time = trial.get_one(self.fp_off_name)

        # Use trial.signals for gaze signal chunks.
        x_signal = trial.signals[self.horiz_buffer_name]
        y_signal = trial.signals[self.vert_buffer_name]
        if x_signal.get_end_time() < fp_off_time or y_signal.get_end_time() < fp_off_time:
            return

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
