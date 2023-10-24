import numpy as np

from pyramid.model.signals import SignalChunk
from pyramid.trials.trials import Trial
from pyramid.trials.standard_adjusters import SignalSmoother


def test_signal_smoother_in_place():
    # Set up a trial with a signal that looks like a simple delta function.
    raw_samples = np.zeros([100, 1])
    raw_samples[50, 0] = 1
    signal = SignalChunk(
        sample_data=raw_samples,
        sample_frequency=10,
        first_sample_time=0.0,
        channel_ids=["chan_a"]
    )
    trial = Trial(start_time=0.0, end_time=10.0)
    trial.add_buffer_data("test_signal", signal)

    # Set up a signal smoother to smoosh down the delta spike.
    signal_smoother = SignalSmoother(
        buffer_name="test_signal",
        kernel_size=3
    )

    # The delta spike should get smooshed down, into three neighboring samples.
    # 0 0 0 1 0 0 0
    #   x x x
    #     x x x
    #       x x x
    expected_samples = np.zeros([100, 1])
    expected_samples[49:52, 0] = 1 / 3

    # The signal smoother should modify the signal sample data in place.
    signal_smoother.enhance(trial, trial_number=0, experiment_info={}, subject_info={})
    assert np.array_equal(signal.sample_data, expected_samples)


def test_signal_smoother_missing():
    # It should be a safe no-op to try smoothing a signal that's not present in the trial.
    trial = Trial(start_time=0.0, end_time=10.0)
    signal_smoother = SignalSmoother(buffer_name="missing_buffer")
    signal_smoother.enhance(trial, trial_number=0, experiment_info={}, subject_info={})


def test_signal_smoother_multiple_channels():
    # Since signal chunks can have multiple channels, make sure the smoother can target the channel we want.
    raw_samples = np.zeros([100, 3])
    raw_samples[50, :] = 1
    signal = SignalChunk(
        sample_data=raw_samples,
        sample_frequency=10,
        first_sample_time=0.0,
        channel_ids=["chan_a", "chan_b", "chan_c"]
    )
    trial = Trial(start_time=0.0, end_time=10.0)
    trial.add_buffer_data("test_signal", signal)

    # Set up a signal smoother to smoosh down the delta spike.
    signal_smoother = SignalSmoother(
        buffer_name="test_signal",
        channel_id="chan_b",
        kernel_size=3
    )

    expected_samples = np.zeros([100, 3])
    expected_samples[50, 0] = 1
    expected_samples[49:52, 1] = 1 / 3
    expected_samples[50, 2] = 1
    signal_smoother.enhance(trial, trial_number=0, experiment_info={}, subject_info={})
    assert np.array_equal(signal.sample_data, expected_samples)
