from pathlib import Path
import numpy as np

from pytest import fixture, raises
import cProfile
import pstats

from pyramid.file_finder import FileFinder
from pyramid.model.events import NumericEventList
from pyramid.model.signals import SignalChunk
from pyramid.neutral_zone.readers.plexon import PlexonPlxReader


@fixture
def fixture_path(request):
    this_file = Path(request.module.__file__)
    return Path(this_file.parent, 'fixture_files')


def test_default_to_all_channels(fixture_path):
    plx_file = Path(fixture_path, "plexon", "opx141ch1to3analogOnly003.plx")
    with PlexonPlxReader(plx_file, FileFinder()) as reader:
        initial = reader.get_initial()

    assert reader.raw_reader.plx_stream is None

    # The example .plx file has:
    #   - 8 spike channels
    #   - 43 event channels
    #   - 32 slow channels
    assert len(initial) == 8 + 43 + 32
    assert isinstance(initial["spike_SPK01"], NumericEventList)
    assert isinstance(initial["spike_SPK08"], NumericEventList)
    assert isinstance(initial["event_EVT01"], NumericEventList)
    assert isinstance(initial["event_EVT32"], NumericEventList)
    assert isinstance(initial["event_Start"], NumericEventList)
    assert isinstance(initial["event_Stop"], NumericEventList)
    assert isinstance(initial["event_Strobed"], NumericEventList)
    assert isinstance(initial["event_KBD1"], NumericEventList)
    assert isinstance(initial["event_KBD8"], NumericEventList)
    assert isinstance(initial["signal_WB01"], SignalChunk)
    assert isinstance(initial["signal_WB08"], SignalChunk)
    assert isinstance(initial["signal_SPKC01"], SignalChunk)
    assert isinstance(initial["signal_SPKC08"], SignalChunk)
    assert isinstance(initial["signal_FP01"], SignalChunk)
    assert isinstance(initial["signal_FP16"], SignalChunk)

    # First sample time None means wait for actual data to choose the first time.
    # This is necessary when the first sample time is not zero!
    for data in initial.values():
        if isinstance(data, SignalChunk):
            assert data.first_sample_time is None


def test_read_whole_plx_file_one_block_at_a_time(fixture_path):
    plx_file = Path(fixture_path, "plexon", "16sp_lfp_with_2coords.plx")
    with PlexonPlxReader(plx_file, FileFinder(), seconds_per_read=0) as reader:

        # The first result should be the "Start" event.
        next = reader.read_next()
        assert next == {
            "event_Start": NumericEventList(np.array([[0.0, 0.0]]))
        }

        # Sample arbitrary results throughout the file, every 10000 blocks.
        # These happen to touch on all three block types: spike events, other events, and signal chunks.

        while reader.raw_reader.block_count < 10000:
            next = reader.read_next()
        assert next == {
            "signal_FP07": SignalChunk(
                sample_data=np.array([
                    0.5987548828125,
                    0.5780029296875,
                    0.5872344970703125,
                    0.604705810546875,
                    0.6003570556640625,
                    0.578460693359375
                ]).reshape([-1, 1]),
                sample_frequency=1000,
                first_sample_time=3.160525,
                channel_ids=[134]
            )
        }

        while reader.raw_reader.block_count < 20000:
            next = reader.read_next()
        assert next == {
            "signal_FP13": SignalChunk(
                sample_data=np.array([
                    -0.01800537109375,
                    -0.020294189453125,
                    -0.0335693359375,
                    -0.0395965576171875,
                    -0.03997802734375,
                    -0.042877197265625,
                    -0.047760009765625
                ]).reshape([-1, 1]),
                sample_frequency=1000,
                first_sample_time=6.116525,
                channel_ids=[140]
            )
        }

        while reader.raw_reader.block_count < 30000:
            next = reader.read_next()
        assert next == {
            "signal_FP11": SignalChunk(
                sample_data=np.array([
                    -0.041961669921875,
                    -0.0548553466796875,
                    -0.06195068359375,
                    -0.0603485107421875,
                    -0.051727294921875,
                    -0.047149658203125
                ]).reshape([-1, 1]),
                sample_frequency=1000,
                first_sample_time=8.973525,
                channel_ids=[138]
            )
        }

        while reader.raw_reader.block_count < 40000:
            next = reader.read_next()
        assert next == {
            "spike_SPK03": NumericEventList(np.array([[12.069825, 3.0,  0.0]]))
        }

        while reader.raw_reader.block_count < 50000:
            next = reader.read_next()
        assert next == {
            "signal_FP01":
            SignalChunk(
                sample_data=np.array([
                    -0.1238250732421875,
                    -0.1308441162109375,
                    -0.1483154296875,
                    -0.1685333251953125,
                    -0.1929473876953125,
                    -0.2101898193359375
                ]).reshape([-1, 1]),
                sample_frequency=1000,
                first_sample_time=15.229525,
                channel_ids=[128]
            )
        }

        # The test file should have 52084 blocks.
        while reader.raw_reader.block_count < 52084:
            next = reader.read_next()

        # The last result should be the "Stop" event.
        assert next == {
            "event_Stop": NumericEventList(np.array([[16.12205, 0.0]]))
        }

        # Now the reader should tell us to stop iterating.
        with raises(StopIteration) as exception_info:
            reader.read_next()
        assert exception_info.errisinstance(StopIteration)

        # Calling read_next() and getting StopIteration should do nothing.
        assert reader.raw_reader.block_count == 52084


def test_read_whole_plx_file_several_seconds_at_a_time(fixture_path):
    plx_file = Path(fixture_path, "plexon", "16sp_lfp_with_2coords.plx")

    # Read through the file roughly 4 seconds at a time.
    with PlexonPlxReader(plx_file, FileFinder(), seconds_per_read=4.0) as reader:
        # The first result should contain the "Start" event.
        next = reader.read_next()
        assert reader.raw_reader.block_count == 12692
        assert next["event_Start"] == NumericEventList(np.array([[0.0, 0.0]]))

        # 4 more seconds.
        next = reader.read_next()
        assert reader.raw_reader.block_count == 26623

        # 4 more seconds.
        next = reader.read_next()
        assert reader.raw_reader.block_count == 39860

        # The sample files should have 52084 blocks total.
        next = reader.read_next()
        assert reader.raw_reader.block_count == 52084

        # The last result should contain the "Stop" event.
        assert next["event_Stop"] == NumericEventList(np.array([[16.12205, 0.0]]))

        # Now the reader should tell us to stop iterating.
        with raises(StopIteration) as exception_info:
            reader.read_next()
        assert exception_info.errisinstance(StopIteration)

        # Calling read_next() and getting StopIteration should do nothing.
        assert reader.raw_reader.block_count == 52084


# hatch run test:cov -k test_profile_read_whole_plx_file -s
def test_profile_read_whole_plx_file(fixture_path):
    plx_file = Path(fixture_path, "plexon", "16sp_lfp_with_2coords.plx")
    with PlexonPlxReader(plx_file, FileFinder()) as reader:
        with cProfile.Profile() as profiler:
            while True:
                try:
                    reader.read_next()
                except StopIteration:
                    break
            stats = pstats.Stats(profiler).sort_stats(pstats.SortKey.TIME)
            stats.print_stats()


def test_read_whole_plx_file_aliased_channels_only(fixture_path):

    plx_file = Path(fixture_path, "plexon", "16sp_lfp_with_2coords.plx")
    spikes = {"SPK03": "my_spikes"}
    events = {
        "Start": "my_start_event",
        "Stop": "my_stop_event"
        }
    signals = {"FP07": "my_signal"}
    expected_names = {*spikes.values(), *events.values(), *signals.values()}

    # Read through the file roughly 4 seconds at a time.
    with PlexonPlxReader(
        plx_file,
        FileFinder(),
        spikes=spikes,
        events=events,
        signals=signals,
        seconds_per_read=4.0
    ) as reader:

        next = reader.read_next()
        assert reader.raw_reader.block_count == 12692

        # Only selected, aliased data names should be returned.
        for name in next.keys():
            assert name in expected_names

        # The first result should contain the aliased "Start" event.
        assert next["my_start_event"] == NumericEventList(np.array([[0.0, 0.0]]))

        next = reader.read_next()
        assert reader.raw_reader.block_count == 26623
        for name in next.keys():
            assert name in expected_names

        next = reader.read_next()
        assert reader.raw_reader.block_count == 39860
        for name in next.keys():
            assert name in expected_names

        next = reader.read_next()
        assert reader.raw_reader.block_count == 52084
        for name in next.keys():
            assert name in expected_names

        # The last result should contain the aliased "Stop" event.
        assert next["my_stop_event"] == NumericEventList(np.array([[16.12205, 0.0]]))

        # Now the reader should tell us to stop iterating.
        with raises(StopIteration) as exception_info:
            reader.read_next()
        assert exception_info.errisinstance(StopIteration)

        # Calling read_next() and getting StopIteration should do nothing.
        assert reader.raw_reader.block_count == 52084
