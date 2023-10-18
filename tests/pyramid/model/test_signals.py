import numpy as np

from pyramid.model.signals import SignalChunk


def test_signal_chunk_getters():
    sample_count = 100
    raw_data = [[v, 10 + v, 10 * v] for v in range(sample_count)]
    signal_chunk = SignalChunk(
        np.array(raw_data),
        10,
        0,
        ["a", "b", "c"]
    )

    assert signal_chunk.sample_count() == sample_count
    assert signal_chunk.channel_count() == 3

    assert np.array_equal(signal_chunk.get_times(), np.array(range(sample_count)) / 10)
    assert signal_chunk.get_end_time() == 0 + (sample_count - 1) / 10
    assert np.array_equal(signal_chunk.get_channel_values(), np.array(range(sample_count)))
    assert np.array_equal(signal_chunk.get_channel_values("a"), np.array(range(sample_count)))
    assert np.array_equal(signal_chunk.get_channel_values("b"), np.array(range(sample_count)) + 10)
    assert np.array_equal(signal_chunk.get_channel_values("c"), np.array(range(sample_count)) * 10)


def test_signal_chunk_append():
    sample_count = 100
    half_count = int(sample_count / 2)
    raw_data = [[v, 10 + v, 10 * v] for v in range(sample_count)]
    signal_chunk_a = SignalChunk(
        np.array(raw_data[0:half_count]),
        10,
        0,
        ["a", "b", "c"]
    )
    assert np.array_equal(signal_chunk_a.get_times(), np.array(range(half_count)) / 10)
    assert signal_chunk_a.get_end_time() == 4.9

    signal_chunk_b = SignalChunk(
        np.array(raw_data[half_count:]),
        10,
        half_count / 10,
        ["a", "b", "c"]
    )
    assert np.array_equal(signal_chunk_b.get_times(), np.array(range(half_count, sample_count)) / 10)
    assert signal_chunk_b.get_end_time() == 9.9

    signal_chunk_a.append(signal_chunk_b)
    assert np.array_equal(signal_chunk_a.get_times(), np.array(range(sample_count)) / 10)
    assert signal_chunk_a.get_end_time() == 9.9
    assert np.array_equal(signal_chunk_a.get_channel_values("a"), np.array(range(sample_count)))
    assert np.array_equal(signal_chunk_a.get_channel_values("b"), np.array(range(sample_count)) + 10)
    assert np.array_equal(signal_chunk_a.get_channel_values("c"), np.array(range(sample_count)) * 10)


def test_signal_chunk_append_fill_in_missing_fields():
    # An empty placeholder signal chunk, as if we haven't read any data yet.
    signal_chunk_a = SignalChunk(
        sample_data=np.empty([0, 1]),
        sample_frequency=None,
        first_sample_time=None,
        channel_ids=["0"]
    )

    # A full signal chunk, perhaps the first data we read in.
    signal_chunk_b = SignalChunk(
        sample_data=np.arange(100).reshape([-1, 1]),
        sample_frequency=10,
        first_sample_time=7.7,
        channel_ids=["0"]
    )

    assert signal_chunk_a.sample_frequency is None
    assert signal_chunk_a.first_sample_time is None

    # The append operation should fill in sample_frequency if missing.
    signal_chunk_a.append(signal_chunk_b)
    assert signal_chunk_a.sample_frequency == signal_chunk_b.sample_frequency
    assert signal_chunk_a.first_sample_time == signal_chunk_b.first_sample_time


def test_signal_chunk_discard_before():
    sample_count = 100
    raw_data = [[v, 10 + v, 10 * v] for v in range(sample_count)]
    signal_chunk = SignalChunk(
        np.array(raw_data),
        10,
        0,
        ["a", "b", "c"]
    )
    assert np.array_equal(signal_chunk.get_times(), np.array(range(sample_count)) / 10)
    assert signal_chunk.get_end_time() == 9.9

    half_count = int(sample_count / 2)
    signal_chunk.discard_before(half_count / 10)
    assert np.array_equal(signal_chunk.get_times(), np.array(range(half_count, sample_count)) / 10)
    assert signal_chunk.get_end_time() == 9.9
    assert np.array_equal(signal_chunk.get_channel_values("a"), np.array(range(half_count, sample_count)))
    assert np.array_equal(signal_chunk.get_channel_values("b"), np.array(range(half_count, sample_count)) + 10)
    assert np.array_equal(signal_chunk.get_channel_values("c"), np.array(range(half_count, sample_count)) * 10)

    signal_chunk.discard_before(1000)
    assert signal_chunk.get_times().size == 0
    assert signal_chunk.get_end_time() == None


def test_signal_chunk_shift_times():
    sample_count = 100
    raw_data = [[v, 10 + v, 10 * v] for v in range(sample_count)]
    signal_chunk = SignalChunk(
        np.array(raw_data),
        10,
        0,
        ["a", "b", "c"]
    )

    signal_chunk.shift_times(5)
    assert np.array_equal(signal_chunk.get_times(), np.array(range(sample_count)) / 10 + 5)
    assert signal_chunk.get_end_time() == 5 + 9.9


def test_signal_chunk_shift_times_empty():
    signal_chunk = SignalChunk(
        np.empty([0, 3]),
        10,
        0,
        ["a", "b", "c"]
    )
    signal_chunk.shift_times(5)
    assert signal_chunk.get_times().size == 0
    assert signal_chunk.get_end_time() == None


def test_signal_chunk_transform_all_values():
    sample_count = 100
    raw_data = [[v, 10 + v, 10 * v] for v in range(sample_count)]
    signal_chunk = SignalChunk(
        np.array(raw_data),
        10,
        0,
        ["a", "b", "c"]
    )

    signal_chunk.apply_offset_then_gain(offset=-500, gain=2)

    assert np.array_equal(signal_chunk.get_times(), np.array(range(sample_count)) / 10)
    assert signal_chunk.get_end_time() == 9.9
    assert np.array_equal(signal_chunk.get_channel_values("a"), (np.array(range(sample_count)) - 500) * 2)
    assert np.array_equal(signal_chunk.get_channel_values("b"), ((np.array(range(sample_count)) + 10) - 500) * 2)
    assert np.array_equal(signal_chunk.get_channel_values("c"), ((np.array(range(sample_count)) * 10) - 500) * 2)


def test_signal_chunk_transform_channel_values():
    sample_count = 100
    raw_data = [[v, 10 + v, 10 * v] for v in range(sample_count)]
    signal_chunk = SignalChunk(
        np.array(raw_data),
        10,
        0,
        ["a", "b", "c"]
    )

    signal_chunk.apply_offset_then_gain(offset=-500, gain=2, channel_id="b")

    assert np.array_equal(signal_chunk.get_times(), np.array(range(sample_count)) / 10)
    assert signal_chunk.get_end_time() == 9.9
    assert np.array_equal(signal_chunk.get_channel_values("a"), np.array(range(sample_count)))
    assert np.array_equal(signal_chunk.get_channel_values("b"), ((np.array(range(sample_count)) + 10) - 500) * 2)
    assert np.array_equal(signal_chunk.get_channel_values("c"), np.array(range(sample_count)) * 10)


def test_signal_chunk_copy_time_range():
    sample_count = 100
    raw_data = [[v, 10 + v, 10 * v] for v in range(sample_count)]
    signal_chunk = SignalChunk(
        np.array(raw_data),
        10,
        0,
        ["a", "b", "c"]
    )

    range_chunk = signal_chunk.copy_time_range(4, 6)
    assert np.array_equal(range_chunk.get_times(), np.array(range(40, 60)) / 10)
    assert range_chunk.get_end_time() == 5.9
    assert np.array_equal(range_chunk.get_channel_values("a"), np.array(range(40, 60)))

    tail_chunk = signal_chunk.copy_time_range(start_time=4)
    assert np.array_equal(tail_chunk.get_times(), np.array(range(40, sample_count)) / 10)
    assert tail_chunk.get_end_time() == 9.9
    assert np.array_equal(tail_chunk.get_channel_values("a"), np.array(range(40, sample_count)))

    head_chunk = signal_chunk.copy_time_range(end_time=6)
    assert np.array_equal(head_chunk.get_times(), np.array(range(0, 60)) / 10)
    assert head_chunk.get_end_time() == 5.9
    assert np.array_equal(head_chunk.get_channel_values("a"), np.array(range(0, 60)))

    empty_chunk = signal_chunk.copy_time_range(start_time=1000)
    assert empty_chunk.get_times().size == 0
    assert empty_chunk.get_end_time() == None
    assert empty_chunk.get_channel_values("a").size == 0

    # original list should be unchanged
    assert np.array_equal(signal_chunk.get_times(), np.array(range(100)) / 10)
    assert signal_chunk.get_end_time() == 9.9
    assert np.array_equal(signal_chunk.get_channel_values("a"), np.array(range(100)))


def test_signal_chunk_equality():
    foo_chunk = SignalChunk(
        np.array([[v, 10 + v, 10 * v] for v in range(100)]),
        10,
        0,
        ["a", "b", "c"]
    )
    bar_chunk = SignalChunk(
        np.array([[v, 10 + v, 10 * v] for v in range(100)]),
        1000,
        0,
        ["a", "b", "c"]
    )
    baz_chunk = bar_chunk.copy()

    # copies should be equal, but not the same object in memory.
    assert baz_chunk is not bar_chunk

    assert foo_chunk == foo_chunk
    assert bar_chunk == bar_chunk
    assert baz_chunk == baz_chunk
    assert bar_chunk == baz_chunk
    assert baz_chunk == bar_chunk

    assert foo_chunk != bar_chunk
    assert bar_chunk != foo_chunk
    assert foo_chunk != baz_chunk
    assert baz_chunk != foo_chunk

    assert foo_chunk != "wrong type"
    assert bar_chunk != "wrong type"
    assert baz_chunk != "wrong type"
