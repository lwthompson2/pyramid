import numpy as np

from pyramid.model.signals import SignalChunk
from pyramid.trials.trials import Trial
from pyramid.trials.standard_collecters import SessionPercentageCollecter, SignalNormalizer


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


def test_signal_normalizer():
    collecter = SignalNormalizer(buffer_name="test_signal")

    # Create random test data with 30 samples for 3 trials at 10Hz.
    sample_data = np.random.rand(30, 1) - 0.5

    first_trial = Trial(0, 1)
    first_trial.add_buffer_data("test_signal", SignalChunk(
        sample_data=sample_data[0:10, :].copy(),
        sample_frequency=10,
        first_sample_time=0,
        channel_ids=["a"]
    ))
    second_trial = Trial(1, 2)
    second_trial.add_buffer_data("test_signal", SignalChunk(
        sample_data=sample_data[10:20, :].copy(),
        sample_frequency=10,
        first_sample_time=10,
        channel_ids=["a"]
    ))
    third_trial = Trial(1, 2)
    third_trial.add_buffer_data("test_signal", SignalChunk(
        sample_data=sample_data[20:30, :].copy(),
        sample_frequency=10,
        first_sample_time=20,
        channel_ids=["a"]
    ))

    # Run through the trials as if coming in normally, collecting the max_absolute_value stat.
    collecter.collect(first_trial, 0, {}, {})
    collecter.collect(second_trial, 1, {}, {})
    collecter.collect(third_trial, 2, {}, {})

    expected_max_absolute_value = np.absolute(sample_data[:,0]).max()
    assert collecter.max_absolute_value == expected_max_absolute_value

    # Run through the trials again, enhancing based on the overall max_absolute_value.
    collecter.enhance(first_trial, 0, {}, {})
    collecter.enhance(second_trial, 1, {}, {})
    collecter.enhance(third_trial, 2, {}, {})

    # Check that the data were scaled as expected.
    normalized_samples = np.concatenate(
        [
            first_trial.signals.get("test_signal").sample_data,
            second_trial.signals.get("test_signal").sample_data,
            third_trial.signals.get("test_signal").sample_data
        ],
        axis=0
    )
    expected_normalized_samples = sample_data / expected_max_absolute_value
    assert np.array_equal(normalized_samples, expected_normalized_samples)


def test_signal_normalizer_multiple_channels():
    # Configure the normalizer to work on the middle of three signal channels.
    collecter = SignalNormalizer(buffer_name="test_signal", channel_id="b")

    # Create random test data with 30 samples across 3 channels, for 3 trials at 10Hz.
    sample_data = np.random.rand(30, 3) - 0.5

    first_trial = Trial(0, 1)
    first_trial.add_buffer_data("test_signal", SignalChunk(
        sample_data=sample_data[0:10, :].copy(),
        sample_frequency=10,
        first_sample_time=0,
        channel_ids=["a", "b", "c"]
    ))
    second_trial = Trial(1, 2)
    second_trial.add_buffer_data("test_signal", SignalChunk(
        sample_data=sample_data[10:20, :].copy(),
        sample_frequency=10,
        first_sample_time=10,
        channel_ids=["a", "b", "c"]
    ))
    third_trial = Trial(1, 2)
    third_trial.add_buffer_data("test_signal", SignalChunk(
        sample_data=sample_data[20:30, :].copy(),
        sample_frequency=10,
        first_sample_time=20,
        channel_ids=["a", "b", "c"]
    ))

    # Run through the trials as if coming in normally, collecting the max_absolute_value stat.
    collecter.collect(first_trial, 0, {}, {})
    collecter.collect(second_trial, 1, {}, {})
    collecter.collect(third_trial, 2, {}, {})

    expected_max_absolute_value = np.absolute(sample_data[:,1]).max()
    assert collecter.max_absolute_value == expected_max_absolute_value

    # Run through the trials again, enhancing based on the overall max_absolute_value.
    collecter.enhance(first_trial, 0, {}, {})
    collecter.enhance(second_trial, 1, {}, {})
    collecter.enhance(third_trial, 2, {}, {})

    # Check that the data were scaled as expected.
    normalized_samples = np.concatenate(
        [
            first_trial.signals.get("test_signal").sample_data,
            second_trial.signals.get("test_signal").sample_data,
            third_trial.signals.get("test_signal").sample_data
        ],
        axis=0
    )
    expected_normalized_samples = sample_data.copy()
    expected_normalized_samples[:,1] /= expected_max_absolute_value
    assert np.array_equal(normalized_samples, expected_normalized_samples)


def test_signal_normalizer_missing_buffer():
    # It should be a safe no-op to try normalizing a signal that's not present in the trial.
    trial = Trial(start_time=0.0, end_time=10.0)
    collecter = SignalNormalizer(buffer_name="missing")
    collecter.collect(trial, 0, {}, {})
    collecter.enhance(trial, 0, {}, {})


def test_signal_normalizer_empty_buffer():
    # It should be a safe no-op to try normalizing a signal that's present but empty.
    trial = Trial(start_time=0.0, end_time=10.0)
    trial.add_buffer_data("empty", SignalChunk(
        sample_data=np.empty([0, 2]),
        sample_frequency=10,
        first_sample_time=0,
        channel_ids=["a"]
    ))

    collecter = SignalNormalizer(buffer_name="empty")
    collecter.collect(trial, 0, {}, {})
    collecter.enhance(trial, 0, {}, {})
