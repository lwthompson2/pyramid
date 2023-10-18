from pathlib import Path

from pytest import fixture, raises
import numpy as np

from pyramid.file_finder import FileFinder
from pyramid.model.events import NumericEventList
from pyramid.neutral_zone.readers.phy import PhyClusterEventReader


@fixture
def fixture_path(request):
    this_file = Path(request.module.__file__)
    return Path(this_file.parent, 'fixture_files')


def test_gold_phy(fixture_path):
    params_file = Path(fixture_path, 'phy', 'gold-phy', 'params.py')
    with PhyClusterEventReader(params_file, FileFinder()) as reader:
        assert reader.get_initial() == {
            "spikes": NumericEventList(np.empty([0, 2]))
        }

        # Expect Plexon 40k sample rate
        assert reader.sample_rate == 40000

        # By default, keep all clusters in the file.
        assert reader.clusters_to_keep == None

        # Expect 510863 [time, cluster] events in this file.
        spike_count = 0
        while reader.current_row < reader.spikes_times.size:
            result = reader.read_next()
            assert result.keys() == {"spikes"}
            spikes = result["spikes"]
            assert spikes.values_per_event() == 1
            spike_count += spikes.event_count()
        assert reader.current_row == 510863
        assert spike_count == 510863

        with raises(StopIteration) as exception_info:
            reader.read_next()
        assert exception_info.errisinstance(StopIteration)


def test_gold_phy_permissive_filter(fixture_path):
    params_file = Path(fixture_path, 'phy', 'gold-phy', 'params.py')
    filter_expression = "True"
    with PhyClusterEventReader(params_file, FileFinder(), cluster_filter=filter_expression) as reader:
        # Expect all clusters and spikes for this permissive filter.
        assert reader.clusters_to_keep == [0, 1, 2, 3, 4, 5, 6, 7]
        spike_count = 0
        while reader.current_row < reader.spikes_times.size:
            result = reader.read_next()
            spikes = result["spikes"]
            spike_count += spikes.event_count()
        assert spike_count == 510863


def test_gold_phy_reasonable_filter(fixture_path):
    params_file = Path(fixture_path, 'phy', 'gold-phy', 'params.py')
    filter_expression = "Amplitude > 5000"
    with PhyClusterEventReader(params_file, FileFinder(), cluster_filter=filter_expression) as reader:
        # Expect some but not all clusters and spikes for this reasonable filter.
        assert reader.clusters_to_keep == [5, 6]
        spike_count = 0
        while reader.current_row < reader.spikes_times.size:
            result = reader.read_next()
            if result:
                spikes = result["spikes"]
                spike_count += spikes.event_count()
        assert spike_count == 31220


def test_gold_phy_harsh_filter(fixture_path):
    params_file = Path(fixture_path, 'phy', 'gold-phy', 'params.py')
    filter_expression = "ContamPct < 100"
    with PhyClusterEventReader(params_file, FileFinder(), cluster_filter=filter_expression) as reader:
        # Expect no clusters or spikes for this harsh filter.
        assert reader.clusters_to_keep == []
        while reader.current_row < reader.spikes_times.size:
            result = reader.read_next()
            assert result is None


def test_phy_data_master(fixture_path):
    params_file = Path(fixture_path, 'phy', 'phy-data-master', 'template', 'params.py')
    with PhyClusterEventReader(params_file, FileFinder()) as reader:
        assert reader.get_initial() == {
            "spikes": NumericEventList(np.empty([0, 2]))
        }

        # Expect 25k sample rate from who knows where.
        assert reader.sample_rate == 25000

        # By default, keep all clusters in the file.
        assert reader.clusters_to_keep == None

        # Expect 314 spikes in this file.
        spike_count = 0
        while reader.current_row < reader.spikes_times.size:
            result = reader.read_next()
            assert result.keys() == {"spikes"}
            spikes = result["spikes"]
            assert spikes.values_per_event() == 1
            spike_count += spikes.event_count()
        assert reader.current_row == 314
        assert spike_count == 314

        with raises(StopIteration) as exception_info:
            reader.read_next()
        assert exception_info.errisinstance(StopIteration)


def test_phy_data_master_reasonable_filter(fixture_path):
    params_file = Path(fixture_path, 'phy', 'phy-data-master', 'template', 'params.py')
    filter_expression = "group == 'good'"
    with PhyClusterEventReader(params_file, FileFinder(), cluster_filter=filter_expression) as reader:
        # Expect some but not all clusters and spikes for this reasonable filter.
        assert reader.clusters_to_keep == [4]
        spike_count = 0
        while reader.current_row < reader.spikes_times.size:
            result = reader.read_next()
            if result:
                spikes = result["spikes"]
                spike_count += spikes.event_count()
        assert spike_count == 6


def test_phy_data_master_harsh_filter(fixture_path):
    params_file = Path(fixture_path, 'phy', 'phy-data-master', 'template', 'params.py')
    filter_expression = "False"
    with PhyClusterEventReader(params_file, FileFinder(), cluster_filter=filter_expression) as reader:
        # Expect no clusters or spikes for this harsh filter.
        assert reader.clusters_to_keep == []
        while reader.current_row < reader.spikes_times.size:
            result = reader.read_next()
            assert result is None


def test_phy_data_master_invalid_filter(fixture_path):
    params_file = Path(fixture_path, 'phy', 'phy-data-master', 'template', 'params.py')
    filter_expression = "invalid=='no way'"
    with PhyClusterEventReader(params_file, FileFinder(), cluster_filter=filter_expression) as reader:
        # Expect no clusters or spikes for this invalid filter.
        assert reader.clusters_to_keep == []
        while reader.current_row < reader.spikes_times.size:
            result = reader.read_next()
            assert result is None
