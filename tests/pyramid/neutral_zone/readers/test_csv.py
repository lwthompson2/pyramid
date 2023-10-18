import numpy as np

from pathlib import Path
from pytest import fixture, raises

from pyramid.model.events import NumericEventList
from pyramid.model.signals import SignalChunk
from pyramid.neutral_zone.readers.csv import CsvNumericEventReader, CsvSignalReader


@fixture
def fixture_path(request):
    this_file = Path(request.module.__file__)
    return Path(this_file.parent, 'fixture_files')


def test_numeric_events_safe_to_spam_exit(fixture_path):
    csv_file = Path(fixture_path, 'numeric_events', 'empty.csv').as_posix()
    reader = CsvNumericEventReader(csv_file)
    reader.__exit__(None, None, None)
    reader.__enter__()
    reader.__exit__(None, None, None)
    reader.__exit__(None, None, None)

    assert reader.file_stream is None


def test_numeric_events_empty_file(fixture_path):
    csv_file = Path(fixture_path, 'numeric_events', 'empty.csv').as_posix()
    with CsvNumericEventReader(csv_file) as reader:
        initial = reader.get_initial()
        with raises(StopIteration) as exception_info:
            reader.read_next()

    expected_initial = {
        reader.result_name: NumericEventList(np.empty([0, 2]))
    }
    assert initial == expected_initial
    assert exception_info.errisinstance(StopIteration)
    assert reader.file_stream is None


def test_numeric_events_with_header_line(fixture_path):
    csv_file = Path(fixture_path, 'numeric_events', 'header_line.csv').as_posix()
    with CsvNumericEventReader(csv_file) as reader:
        initial = reader.get_initial()
        expected_initial = {
            reader.result_name: NumericEventList(np.empty([0, 3]))
        }
        assert initial == expected_initial

        # Consume the header line.
        assert reader.read_next() is None

        # Read 32 lines...
        for t in range(32):
            result = reader.read_next()
            event_list = result[reader.result_name]
            expected_event_list = NumericEventList(np.array([[t, t + 100, t + 1000]]))
            assert event_list == expected_event_list

        # ...then be done.
        with raises(StopIteration) as exception_info:
            reader.read_next()
        assert exception_info.errisinstance(StopIteration)

    assert reader.file_stream is None


def test_numeric_events_with_no_header_line(fixture_path):
    csv_file = Path(fixture_path, 'numeric_events', 'no_header_line.csv').as_posix()
    with CsvNumericEventReader(csv_file) as reader:
        initial = reader.get_initial()
        expected_initial = {
            reader.result_name: NumericEventList(np.empty([0, 3]))
        }
        assert initial == expected_initial

        # Read 32 lines...
        for t in range(32):
            result = reader.read_next()
            event_list = result[reader.result_name]
            expected_event_list = NumericEventList(np.array([[t, t + 100, t + 1000]]))
            assert event_list == expected_event_list

        # ...then be done.
        with raises(StopIteration) as exception_info:
            reader.read_next()
        assert exception_info.errisinstance(StopIteration)

    assert reader.file_stream is None


def test_numeric_events_skip_nonnumeric_lines(fixture_path):
    csv_file = Path(fixture_path, 'numeric_events', 'nonnumeric_lines.csv').as_posix()
    nonnumeric_lines = [1, 11, 15, 21, 28]
    with CsvNumericEventReader(csv_file) as reader:
        initial = reader.get_initial()
        expected_initial = {
            reader.result_name: NumericEventList(np.empty([0, 3]))
        }
        assert initial == expected_initial

        # Read 32 lines...
        for t in range(32):
            result = reader.read_next()
            if t in nonnumeric_lines:
                assert result is None
            else:
                event_list = result[reader.result_name]
                expected_event_list = NumericEventList(np.array([[t, t + 100, t + 1000]]))
                assert event_list == expected_event_list

        # ...then be done.
        with raises(StopIteration) as exception_info:
            reader.read_next()
        assert exception_info.errisinstance(StopIteration)

    assert reader.file_stream is None


def test_signals_safe_to_spam_exit(fixture_path):
    csv_file = Path(fixture_path, 'signals', 'empty.csv').as_posix()
    reader = CsvSignalReader(csv_file)
    reader.__exit__(None, None, None)
    reader.__enter__()
    reader.__exit__(None, None, None)
    reader.__exit__(None, None, None)

    assert reader.file_stream is None


def test_signals_empty_file(fixture_path):
    csv_file = Path(fixture_path, 'signals', 'empty.csv').as_posix()
    with CsvSignalReader(csv_file) as reader:
        initial = reader.get_initial()
        with raises(StopIteration) as exception_info:
            reader.read_next()

    expected_initial = {reader.result_name: SignalChunk(
        np.empty([0, 0]), reader.sample_frequency, first_sample_time=0.0, channel_ids=[])}
    assert initial == expected_initial
    assert exception_info.errisinstance(StopIteration)
    assert reader.file_stream is None


def test_signals_only_complete_chunks(fixture_path):
    csv_file = Path(fixture_path, 'signals', 'header_line.csv').as_posix()
    with CsvSignalReader(csv_file, lines_per_chunk=10) as reader:
        initial = reader.get_initial()
        expected_initial = {
            reader.result_name: SignalChunk(
                np.empty([0, 3]),
                reader.sample_frequency,
                first_sample_time=0.0,
                channel_ids=["a", "b", "c"]
            )
        }
        assert initial == expected_initial

        # Read 15 chunks of 10 lines each...
        for chunk_index in range(15):
            chunk_time = chunk_index * 10
            assert reader.next_sample_time == chunk_time

            result = reader.read_next()
            signal_chunk = result[reader.result_name]
            assert signal_chunk.sample_count() == 10

            sample_times = signal_chunk.get_times()
            assert np.array_equal(sample_times, np.array(range(chunk_time, chunk_time + 10)))
            assert np.array_equal(signal_chunk.get_channel_values("a"), sample_times)
            assert np.array_equal(signal_chunk.get_channel_values("b"), 100 - sample_times * 0.1)
            assert np.array_equal(signal_chunk.get_channel_values("c"), sample_times * 2 - 1000)

        # ...then be done.
        with raises(StopIteration) as exception_info:
            reader.read_next()
        assert exception_info.errisinstance(StopIteration)

    assert reader.file_stream is None


def test_signals_last_partial_chunk(fixture_path):
    csv_file = Path(fixture_path, 'signals', 'header_line.csv').as_posix()
    with CsvSignalReader(csv_file, lines_per_chunk=11) as reader:
        initial = reader.get_initial()
        expected_initial = {
            reader.result_name: SignalChunk(
                np.empty([0, 3]),
                reader.sample_frequency,
                first_sample_time=0.0,
                channel_ids=["a", "b", "c"]
            )
        }
        assert initial == expected_initial

        # Read 13 chunks of 11 lines each...
        for chunk_index in range(13):
            chunk_time = chunk_index * 11
            assert reader.next_sample_time == chunk_time

            result = reader.read_next()
            signal_chunk = result[reader.result_name]
            assert signal_chunk.sample_count() == 11

            sample_times = signal_chunk.get_times()
            assert np.array_equal(sample_times, np.array(range(chunk_time, chunk_time + 11)))
            assert np.array_equal(signal_chunk.get_channel_values("a"), sample_times)
            assert np.array_equal(signal_chunk.get_channel_values("b"), 100 - sample_times * 0.1)
            assert np.array_equal(signal_chunk.get_channel_values("c"), sample_times * 2 - 1000)

        # Read a last, partial chunk of 7 lines.
        chunk_time = 143
        assert reader.next_sample_time == chunk_time

        result = reader.read_next()
        signal_chunk = result[reader.result_name]
        assert signal_chunk.sample_count() == 7

        sample_times = signal_chunk.get_times()
        assert np.array_equal(sample_times, np.array(range(chunk_time, chunk_time + 7)))
        assert np.array_equal(signal_chunk.get_channel_values("a"), sample_times)
        assert np.array_equal(signal_chunk.get_channel_values("b"), 100 - sample_times * 0.1)
        assert np.array_equal(signal_chunk.get_channel_values("c"), sample_times * 2 - 1000)

        # ...then be done.
        with raises(StopIteration) as exception_info:
            reader.read_next()
        assert exception_info.errisinstance(StopIteration)

    assert reader.file_stream is None


def test_signals_skip_nonnumeric_lines(fixture_path):
    csv_file = Path(fixture_path, 'signals', 'nonnumeric_lines.csv').as_posix()
    with CsvSignalReader(csv_file, lines_per_chunk=10) as reader:
        initial = reader.get_initial()
        expected_initial = {
            reader.result_name: SignalChunk(
                np.empty([0, 3]),
                reader.sample_frequency,
                first_sample_time=0.0,
                channel_ids=["a", "b", "c"]
            )
        }
        assert initial == expected_initial

        # Read 15 chunks of 10 lines each...
        for chunk_index in range(15):
            chunk_time = chunk_index * 10
            assert reader.next_sample_time == chunk_time

            result = reader.read_next()
            signal_chunk = result[reader.result_name]
            assert signal_chunk.sample_count() == 10

            sample_times = signal_chunk.get_times()
            assert np.array_equal(sample_times, np.array(range(chunk_time, chunk_time + 10)))
            assert np.array_equal(signal_chunk.get_channel_values("a"), sample_times)
            assert np.array_equal(signal_chunk.get_channel_values("b"), 100 - sample_times * 0.1)
            assert np.array_equal(signal_chunk.get_channel_values("c"), sample_times * 2 - 1000)

        # ...then be done.
        with raises(StopIteration) as exception_info:
            reader.read_next()
        assert exception_info.errisinstance(StopIteration)

    assert reader.file_stream is None
