from pyramid.trials.trials import Trial
from pyramid.trials.standard_collecters import SessionPercentageCollecter


def test_session_percentage_collecter():
    collecter = SessionPercentageCollecter()

    start_times = range(10)
    trials = [Trial(start_time=start, end_time=start+1) for start in start_times]

    # Run through the trials as if hapening normally, collecting the max_start_time stat.
    for index, trial in enumerate(trials):
        collecter.collect(trial, index, {}, {})
        assert collecter.max_start_time == trial.start_time

    # Run through the trials again, enhancing based on overall max_start_time.
    for index, trial in enumerate(trials):
        collecter.enhance(trial, index, {}, {})
        assert trial.get_enhancement("percent_complete") == 100 * trial.start_time / start_times[-1]
