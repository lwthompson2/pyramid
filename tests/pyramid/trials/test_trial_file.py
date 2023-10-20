from pathlib import Path
import numpy as np
from pytest import raises

from pyramid.model.events import NumericEventList
from pyramid.model.signals import SignalChunk
from pyramid.trials.trials import Trial
from pyramid.trials.trial_file import TrialFile, JsonTrialFile, Hdf5TrialFile


sample_numeric_events = {
    "empty": NumericEventList(event_data=np.empty([0, 2])),
    "simple": NumericEventList(event_data=np.array([[0.1, 0], [0.2, 1], [0.3, 0]])),
    "complex": NumericEventList(event_data=np.array([[0.1, 0, 42.42], [0.2, 1, 42.42], [0.3, 0, 43.43]]))
}

sample_signals = {
    "empty": SignalChunk(
        sample_data=np.empty([0, 2]),
        sample_frequency=None,
        first_sample_time=None,
        channel_ids=["q", "r"]
    ),
    "simple": SignalChunk(
        sample_data=np.array([[0], [1], [2], [3], [0], [5]]),
        sample_frequency=10,
        first_sample_time=0.1,
        channel_ids=["x"]
    ),
    "complex": SignalChunk(
        sample_data=np.array([[0, 10, 100], [1, 11, 100.1], [2, 12, 100.2], [3, 13, 100.3], [0, 10, 100], [5, 15, 100.5]]),
        sample_frequency=100,
        first_sample_time=-0.5,
        channel_ids=["a", "b", "c"]
    )
}

sample_enhancements = {
    "string": "I'm a string.",
    "int": 42,
    "float": 1.11,
    "empty_dict": {},
    "empty_list": [],
    "dict": {"a": 1, "b":2},
    "list": ["a", 1, "b", 2]
}

sample_trials = [
    Trial(
        start_time=0,
        end_time=1.0,
        wrt_time=0.0
    ),
    Trial(
        start_time=1.0,
        end_time=2.0,
        wrt_time=1.5,
        numeric_events=sample_numeric_events
    ),
    Trial(
        start_time=2.0,
        end_time=3.0,
        wrt_time=2.5,
        signals=sample_signals
    ),
    Trial(
        start_time=3.0,
        end_time=4.0,
        wrt_time=3.5,
        enhancements=sample_enhancements,
        enhancement_categories={"value": list(sample_enhancements.keys())}
    ),
    Trial(
        start_time=4.0,
        end_time=None,
        wrt_time=4.5,
        numeric_events=sample_numeric_events,
        signals=sample_signals,
        enhancements=sample_enhancements,
        enhancement_categories={"value": list(sample_enhancements.keys())}
    )
]


def test_for_file_suffix():
    # Choose TrialFile implementation based on supported extensions.
    assert isinstance(TrialFile.for_file_suffix("trial_file.json"), JsonTrialFile)
    assert isinstance(TrialFile.for_file_suffix("trial_file.jsonl"), JsonTrialFile)
    assert isinstance(TrialFile.for_file_suffix("trial_file.hdf"), Hdf5TrialFile)
    assert isinstance(TrialFile.for_file_suffix("trial_file.h5"), Hdf5TrialFile)
    assert isinstance(TrialFile.for_file_suffix("trial_file.hdf5"), Hdf5TrialFile)
    assert isinstance(TrialFile.for_file_suffix("trial_file.he5"), Hdf5TrialFile)

    # Choose TrialFile implementation with supported extensions obscured by .temp.
    assert isinstance(TrialFile.for_file_suffix("trial_file.json.temp"), JsonTrialFile)
    assert isinstance(TrialFile.for_file_suffix("trial_file.jsonl.temp"), JsonTrialFile)
    assert isinstance(TrialFile.for_file_suffix("trial_file.hdf.temp"), Hdf5TrialFile)
    assert isinstance(TrialFile.for_file_suffix("trial_file.h5.temp"), Hdf5TrialFile)
    assert isinstance(TrialFile.for_file_suffix("trial_file.hdf5.temp"), Hdf5TrialFile)
    assert isinstance(TrialFile.for_file_suffix("trial_file.he5.temp"), Hdf5TrialFile)

    # Be strict about supported extensions, don't try to fall back on a default.
    with raises(NotImplementedError) as exception_info:
        TrialFile.for_file_suffix("trial_file.noway")
    assert exception_info.errisinstance(NotImplementedError)
    assert "Unsupported trial file suffixes: {'.noway'}" in exception_info.value.args


def test_json_empty_trial_file(tmp_path):
    file_path = Path(tmp_path, 'trial_file.json')
    assert not file_path.exists()

    # By default, trial file is not created or truncated.
    with JsonTrialFile(file_path) as trial_file:
        assert not file_path.exists()

    # But it can be created or truncated.
    with JsonTrialFile(file_path, create_empty=True) as trial_file:
        assert file_path.exists()
        trials = [trial for trial in trial_file.read_trials()]

    assert len(trials) == 0


def test_json_sample_trials(tmp_path):
    file_path = Path(tmp_path, 'trial_file.json')
    assert not file_path.exists()

    with JsonTrialFile(file_path, create_empty=True) as trial_file:
        assert file_path.exists()
        for sample_trial in sample_trials:
            trial_file.append_trial(sample_trial)

        trials = [trial for trial in trial_file.read_trials()]

    assert trials == sample_trials


def test_json_interleave_write_and_read(tmp_path):
    file_path = Path(tmp_path, 'trial_file.json')
    assert not file_path.exists()

    with JsonTrialFile(file_path, create_empty=True) as trial_file:
        assert file_path.exists()
        for sample_trial in sample_trials:
            trial_file.append_trial(sample_trial)
            trials = [trial for trial in trial_file.read_trials()]
            assert trials[0] == sample_trials[0]
            assert trials[-1] == sample_trial


def test_hdf5_empty_trial_file(tmp_path):
    file_path = Path(tmp_path, 'trial_file.hdf5')
    assert not file_path.exists()

    # By default, trial file is not created or truncated.
    with Hdf5TrialFile(file_path) as trial_file:
        assert not file_path.exists()

    # But it can be created or truncated.
    with JsonTrialFile(file_path, create_empty=True) as trial_file:
        assert file_path.exists()
        trials = [trial for trial in trial_file.read_trials()]

    assert len(trials) == 0


def test_hdf5_sample_trials(tmp_path):
    file_path = Path(tmp_path, 'trial_file.hdf5')
    assert not file_path.exists()

    with Hdf5TrialFile(file_path, truncate=True) as trial_file:
        assert file_path.exists()
        for sample_trial in sample_trials:
            trial_file.append_trial(sample_trial)

        trials = [trial for trial in trial_file.read_trials()]

    assert trials == sample_trials


def test_hdf5_interleave_write_and_read(tmp_path):
    file_path = Path(tmp_path, 'trial_file.hdf5')
    assert not file_path.exists()

    with Hdf5TrialFile(file_path, truncate=True) as trial_file:
        assert file_path.exists()
        for sample_trial in sample_trials:
            trial_file.append_trial(sample_trial)
            trials = [trial for trial in trial_file.read_trials()]
            assert trials[0] == sample_trials[0]
            assert trials[-1] == sample_trial
