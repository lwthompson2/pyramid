from datetime import datetime
from pathlib import Path
import json
import numpy as np

from pytest import fixture

from pyramid.neutral_zone.readers.plexon import PlexonPlxRawReader



# Load some Plexon .plx files and verify the contents.
# The .plx files and expected contents are from Plexon's "OmniPlex and MAP Offline SDK Bundle" / "Matlab Offline Files SDK".
# For details see: pyramid/tests/pyramid/neutral_zone/readers/fixture_files/plexon/README.txt


@fixture
def fixture_path(request):
    this_file = Path(request.module.__file__)
    return Path(this_file.parent, 'fixture_files')


def assert_global_header(header: dict, expected: dict) -> None:
    assert header['MagicNumber'] == 1480936528
    assert header['Version'] == expected['Version']
    assert header['Comment'] == expected['Comment']
    assert header['ADFrequency'] in expected['adfreqs']
    assert header['NumDSPChannels'] == len(expected['spk_names'])
    assert header['NumEventChannels'] == len(expected['evnames'])
    assert header['NumSlowChannels'] == len(expected['adnames'])
    assert header['NumPointsWave'] == expected['NPW']
    assert header['NumPointsPreThr'] == expected['PreThresh']
    date = datetime(header['Year'], header['Month'], header['Day'], header['Hour'], header['Minute'], header['Second'])
    expected_date = datetime.strptime(expected['DateTime'].strip(), '%m/%d/%Y %H:%M:%S')
    assert date == expected_date
    assert header['WaveformFreq'] == expected['Freq']
    assert header['LastTimestamp'] / header['WaveformFreq'] == expected['Duration']
    assert header['Trodalness'] == expected['Trodalness']
    assert header['BitsPerSpikeSample'] == expected['SpikeADResBits']
    assert header['BitsPerSlowSample'] == expected['SlowADResBits']
    assert header['SpikeMaxMagnitudeMV'] == expected['SpikePeakV']
    assert header['SlowMaxMagnitudeMV'] == expected['SlowPeakV']

    # Expected timestamp and waveform counts are weirdly shaped but the numbers are there.
    # To select and compare relevant chunks of data we need to know from mexPlex/Plexon.h:
    #  - channels are one-based, with nothing in channel 0
    #  - each channel has up to 5 units, as far as this global header knows
    channel_range = range(header["NumDSPChannels"] + 1)
    unit_range = range(5)

    ts_counts = header['TSCounts'][channel_range, :]
    expected_ts_counts = np.array(expected['tscounts'])[unit_range, :].T
    assert np.array_equal(ts_counts, expected_ts_counts)

    wf_counts = header['WFCounts'][channel_range, :]
    expected_wf_counts = np.array(expected['wfcounts'])[unit_range, :].T
    assert np.array_equal(wf_counts, expected_wf_counts)

    # Expected event counts are also weirdly shaped.
    # To build a comparable array of counts we need to know:
    #  - expected data have already been selected by channel index out of an original, big array of size 512
    #  - starting at index 300, this same array records continuous channel samples, not event samples!
    ev_counts = header['EVCounts']
    expected_ev_counts = np.zeros((512,), dtype=ev_counts.dtype)
    expected_ev_chans = expected['evchans']
    expected_chan_ev_counts = np.array(expected['evcounts'])
    expected_ev_counts[expected_ev_chans] = expected_chan_ev_counts
    expected_slow_counts = expected["slowcounts"]
    slow_range = range(300, 300 + len(expected_slow_counts))
    expected_ev_counts[slow_range] = expected_slow_counts
    assert np.array_equal(ev_counts, expected_ev_counts)


def assert_dsp_channel_headers(headers: list[dict], expected: dict) -> None:
    assert len(headers) == len(expected['spk_names'])
    for index, header in enumerate(headers):
        assert header["Name"] == expected["spk_names"][index].replace('\x00', '')
        assert header["Channel"] == index + 1
        assert header["SIG"] == index + 1
        assert header["Gain"] == expected["spk_gains"][index]
        assert header["Filter"] == expected["spk_filters"][index]
        assert header["Threshold"] == expected["spk_threshs"][index]
        assert header["Method"] in {1, 2}


def assert_event_channel_headers(headers: list[dict], expected: dict) -> None:
    assert len(headers) == len(expected['evnames'])
    for index, header in enumerate(headers):
        assert header["Name"] == expected['evnames'][index].replace('\x00', '')
        assert header["Channel"] == expected['evchans'][index]


def assert_slow_channel_headers(headers: list[dict], expected: dict) -> None:
    assert len(headers) == len(expected['adnames'])
    for index, header in enumerate(headers):
        assert header["Name"] == expected['adnames'][index].replace('\x00', '')
        assert header["Channel"] == index
        assert header["ADFreq"] == expected["adfreqs"][index]
        assert header["Gain"] == expected["adgains"][index]
        assert header["Enabled"] in {0, 1}
        assert header["PreampGain"] > 0
        assert header["SpikeChannel"] <= len(expected['spk_names'])


def read_all_blocks(raw_reader: PlexonPlxRawReader) -> dict[int, dict[int, list]]:
    all_blocks = {
        1: {},
        4: {},
        5: {},
    }
    block = raw_reader.next_block()
    while block:
        if block["channel"] not in all_blocks[block["type"]]:
            all_blocks[block["type"]][block["channel"]] = []

        all_blocks[block["type"]][block["channel"]].append(block)
        block = raw_reader.next_block()

    return all_blocks


def assert_sequential_block_timestamps(all_blocks: dict[int, dict[int, list]]):
    """Expect timestamps to be sequential within a channel type and id, otherwise can be ragged"""
    for channel_blocks in all_blocks.values():
        for blocks in channel_blocks.values():
            previous_timestamp = -1
            for block in blocks:
                assert block["timestamp"] > previous_timestamp
                previous_timestamp = block["timestamp"]


def assert_events(all_blocks: dict[int, dict[int, list]], expected: dict):
    event_channel_blocks = all_blocks[4]
    for channel_id, blocks in event_channel_blocks.items():
        event_times = [block["timestamp_seconds"] for block in blocks]
        # The expected data set started querying for channels at index -1, so it's off by one channel.
        expected_times = expected["tsevs"][channel_id + 1]

        # Awkward, expected data have single event times "unboxed" from their lists.
        if len(event_times) == 1:
            assert event_times[0] == expected_times
        else:
            assert event_times == expected_times


def assert_slow_waveforms(all_blocks: dict[int, dict[int, list]], expected: dict):
    slow_channel_blocks = all_blocks[5]
    for channel_id, blocks in slow_channel_blocks.items():
        # The expected data set started querying for channels at index -1, so it's off by one channel.
        expected_timestamp = expected["ad_v"]["ts"][channel_id + 1]
        first_timestamp = blocks[0]['timestamp_seconds']
        assert first_timestamp == expected_timestamp

        expected_frequency = expected["ad_v"]["freq"][channel_id + 1]
        for block in blocks:
            block_frequency = block['frequency']
            assert block_frequency == expected_frequency

        expected_sample_count = expected["ad_v"]["nad"][channel_id + 1]
        expected_waveform = expected["ad_v"]["val"][channel_id + 1]
        block_waveforms = [block["waveforms"] for block in blocks]
        channel_waveform = np.concatenate(block_waveforms)
        assert channel_waveform.shape[0] == expected_sample_count
        assert np.array_equal(channel_waveform, expected_waveform)


def assert_dsp_waveforms(all_blocks: dict[int, dict[int, list]], expected: dict):
    # Expected dsp waveform data are weirdly shaped but the numbers are there.
    # Here's an excerpt from mexPlex/tests/get_all_from_plx.m which generated the expected data:
    #
    # % try valid and invalid channels and units
    # % max unit is 26
    # % max spike channel number is 128
    # for iunit = -1:30
    #     for ich = -1:130
    #         [plx.ts.n{iunit+2,ich+2}, plx.ts.ts{iunit+2,ich+2}] = plx_ts(fileName, ich , iunit );
    #         [plx.wf.n{iunit+2,ich+2}, plx.wf.npw{iunit+2,ich+2}, plx.wf.ts{iunit+2,ich+2}, plx.wf.wf{iunit+2,ich+2}] = plx_waves(fileName, ich , iunit );
    #         [plx.wf_v.n{iunit+2,ich+2}, plx.wf_v.npw{iunit+2,ich+2}, plx.wf_v.ts{iunit+2,ich+2}, plx.wf_v.wf{iunit+2,ich+2}] = plx_waves_v(fileName, ich , iunit );
    #      end
    # end
    #
    # So, the channel-and-unit data should have shape (32,132), for units -1:30 and channels -1:130.
    # And, since the queries start at index -1, the channel_ids and unit_ids below will be off by 1.
    test_data_shape = (132, 32)
    expected_waveform_count = np.array(expected["wf_v"]["n"]).reshape(test_data_shape)
    expected_waveform_sample_count = np.array(expected["wf_v"]["npw"]).reshape(test_data_shape)
    expected_waveforms = np.array(expected["wf_v"]["wf"], dtype='object').reshape(test_data_shape)
    expected_timestamps = np.array(expected["wf_v"]["ts"], dtype='object').reshape(test_data_shape)
    expected_frequency = expected["Freq"]

    dsp_channel_blocks = all_blocks[1]
    for channel_id, blocks in dsp_channel_blocks.items():
        # Within each channel, group blocks by unit.
        blocks_by_unit = {}
        for block in blocks:
            unit_id = block["unit"]
            unit_blocks = blocks_by_unit.get(unit_id, [])
            unit_blocks.append(block)
            blocks_by_unit[unit_id] = unit_blocks

        # Compare to expected data per channel and unit.
        for unit_id, unit_blocks in blocks_by_unit.items():
            # The expected data set started querying for units and channels at index -1, so these are off by one.
            expected_unit_timestamps = expected_timestamps[channel_id + 1, unit_id + 1]
            for index, block in enumerate(unit_blocks):
                assert block["frequency"] == expected_frequency
                assert block["timestamp_seconds"] == expected_unit_timestamps[index]

            expected_unit_shape = (
                expected_waveform_count[channel_id + 1, unit_id + 1],
                expected_waveform_sample_count[channel_id + 1, unit_id + 1]
            )
            expected_unit_samples = expected_waveforms[channel_id + 1, unit_id + 1]

            unit_samples_per_block = [block["waveforms"] for block in unit_blocks]
            unit_samples = np.stack(unit_samples_per_block)
            assert unit_samples.shape == expected_unit_shape
            assert np.array_equal(unit_samples, expected_unit_samples)


def test_opx141spkOnly004(fixture_path):
    plx_file = Path(fixture_path, "plexon", "opx141spkOnly004.plx")
    json_file = Path(fixture_path, "plexon", "opx141spkOnly004.json")

    with open(json_file) as f:
        expected = json.load(f)
        with PlexonPlxRawReader(plx_file) as raw_reader:
            assert_global_header(raw_reader.global_header, expected)
            assert_dsp_channel_headers(raw_reader.dsp_channel_headers, expected)
            assert_event_channel_headers(raw_reader.event_channel_headers, expected)
            assert_slow_channel_headers(raw_reader.slow_channel_headers, expected)

            all_blocks = read_all_blocks(raw_reader)
            assert_sequential_block_timestamps(all_blocks)
            assert_events(all_blocks, expected)
            assert_slow_waveforms(all_blocks, expected)
            assert_dsp_waveforms(all_blocks, expected)


def test_opx141ch1to3analogOnly003(fixture_path):
    plx_file = Path(fixture_path, "plexon", "opx141ch1to3analogOnly003.plx")
    json_file = Path(fixture_path, "plexon", "opx141ch1to3analogOnly003.json")

    with open(json_file) as f:
        expected = json.load(f)
        with PlexonPlxRawReader(plx_file) as raw_reader:
            assert_global_header(raw_reader.global_header, expected)
            assert_dsp_channel_headers(raw_reader.dsp_channel_headers, expected)
            assert_event_channel_headers(raw_reader.event_channel_headers, expected)
            assert_slow_channel_headers(raw_reader.slow_channel_headers, expected)

            all_blocks = read_all_blocks(raw_reader)
            assert_sequential_block_timestamps(all_blocks)
            assert_events(all_blocks, expected)
            assert_slow_waveforms(all_blocks, expected)
            assert_dsp_waveforms(all_blocks, expected)


def test_16sp_lfp_with_2coords(fixture_path):
    plx_file = Path(fixture_path, "plexon", "16sp_lfp_with_2coords.plx")
    json_file = Path(fixture_path, "plexon", "16sp_lfp_with_2coords.json")

    with open(json_file) as f:
        expected = json.load(f)
        with PlexonPlxRawReader(plx_file) as raw_reader:
            assert_global_header(raw_reader.global_header, expected)
            assert_dsp_channel_headers(raw_reader.dsp_channel_headers, expected)
            assert_event_channel_headers(raw_reader.event_channel_headers, expected)
            assert_slow_channel_headers(raw_reader.slow_channel_headers, expected)

            all_blocks = read_all_blocks(raw_reader)
            assert_sequential_block_timestamps(all_blocks)
            assert_events(all_blocks, expected)
            assert_slow_waveforms(all_blocks, expected)
            assert_dsp_waveforms(all_blocks, expected)


def test_strobed_negative(fixture_path):
    # We don't have expected data for this file, but we can still sanity check header and block parsing.
    plx_file = Path(fixture_path, "plexon", "strobed_negative.plx")
    with PlexonPlxRawReader(plx_file) as raw_reader:
        all_blocks = read_all_blocks(raw_reader)
        # Expect one event on the 257/"strobed" channel with value 0xFFFF -- uint16 65535 or sint16 -1.
        assert all_blocks[4][257][0]['unit'] == 65535
        assert_sequential_block_timestamps(all_blocks)


def test_ts_freq_zero(fixture_path):
    # We don't have expected data for this file, but we can still sanity check header and block parsing.
    plx_file = Path(fixture_path, "plexon", "ts_freq_zero.plx")
    with PlexonPlxRawReader(plx_file) as raw_reader:
        # Expect 0Hz timestamp frequency -- probably an example of misconfiguration?
        assert raw_reader.global_header["ADFrequency"] == 0
        assert raw_reader.global_header["WaveformFreq"] == 40000
        all_blocks = read_all_blocks(raw_reader)
        assert_sequential_block_timestamps(all_blocks)


def test_waveform_freq_zero(fixture_path):
    plx_file = Path(fixture_path, "plexon", "waveform_freq_zero.plx")
    with PlexonPlxRawReader(plx_file) as raw_reader:
        # Expect 0Hz waveforms frequency -- probably an example of misconfiguration?
        assert raw_reader.global_header["ADFrequency"] == 40000
        assert raw_reader.global_header["WaveformFreq"] == 0
        all_blocks = read_all_blocks(raw_reader)
        assert_sequential_block_timestamps(all_blocks)
