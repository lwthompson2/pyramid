import numpy as np

from pyramid.file_finder import FileFinder
from pyramid.model.events import NumericEventList
from pyramid.model.signals import SignalChunk
from pyramid.neutral_zone.transformers.transformers import Transformer
from pyramid.neutral_zone.transformers.standard_transformers import OffsetThenGain, FilterRange


def test_installed_transformer_dynamic_import():
    # Import a transformer that was installed in the usual way (eg by pip) along with pyramid itself.
    import_spec = "pyramid.neutral_zone.transformers.standard_transformers.OffsetThenGain"
    transformer = Transformer.from_dynamic_import(import_spec, FileFinder())
    assert isinstance(transformer, Transformer)
    assert isinstance(transformer, OffsetThenGain)


def test_offset_then_gain_dynamic_imports_with_kwargs():
    offset_then_gain_spec = "pyramid.neutral_zone.transformers.standard_transformers.OffsetThenGain"
    offset_then_gain = Transformer.from_dynamic_import(
        offset_then_gain_spec,
        FileFinder(),
        offset=10,
        gain=-2,
        ignore="ignore me"
    )
    assert offset_then_gain.offset == 10
    assert offset_then_gain.gain == -2


def test_filter_range_dynamic_imports_with_kwargs():
    filter_range_spec = "pyramid.neutral_zone.transformers.standard_transformers.FilterRange"
    filter_range = Transformer.from_dynamic_import(
        filter_range_spec,
        FileFinder(),
        min=-100,
        max=55,
        ignore="ignore me"
    )
    assert filter_range.min == -100
    assert filter_range.max == 55


def test_offset_then_gain_event_list():
    event_count = 100
    raw_data = [[t, 10*t] for t in range(event_count)]
    event_list = NumericEventList(np.array(raw_data))

    transformer = OffsetThenGain(offset=10, gain=-2)
    transformed = transformer.transform(event_list)

    expected_data = [[t, -2 * (10 + (10*t))] for t in range(event_count)]
    expected = NumericEventList(np.array(expected_data))
    assert transformed == expected


def test_offset_then_gain_signal_chunk():
    sample_count = 100
    raw_data = [[s, 10 * s] for s in range(sample_count)]
    signal_chunk = SignalChunk(np.array(raw_data), sample_frequency=1.0, first_sample_time=0.0, channel_ids=[0, 1])

    transformer = OffsetThenGain(offset=10, gain=-2)
    transformed = transformer.transform(signal_chunk)

    expected_data = [[-2 * (10 + s), -2 * (10 + (10 * s))] for s in range(sample_count)]
    expected = SignalChunk(np.array(expected_data), sample_frequency=1.0, first_sample_time=0.0, channel_ids=[0, 1])
    assert transformed == expected


def test_filter_range():
    raw_data = [[t, 10*t] for t in range(100)]
    event_list = NumericEventList(np.array(raw_data))

    transformer = FilterRange(min=250, max=750)
    transformed = transformer.transform(event_list)

    expected_data = [[t, 10*t] for t in range(25, 75)]
    expected = NumericEventList(np.array(expected_data))
    assert transformed == expected
