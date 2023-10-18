from types import TracebackType
from typing import Self
from pathlib import Path
import csv
import numpy as np

from pyramid.file_finder import FileFinder
from pyramid.model.model import BufferData
from pyramid.model.events import NumericEventList
from pyramid.neutral_zone.readers.readers import Reader


class PhyClusterEventReader(Reader):
    """Read and filter spike/cluster time numeric events from a folder of Phy files."""

    def __init__(
        self,
        params_file: str,
        file_finder: FileFinder,
        sample_rate_param_name: str = "sample_rate",
        spike_times_name: str = "spike_times.npy",
        spike_clusters_name: str = "spike_clusters.npy",
        cluster_glob: str = "cluster_*",
        cluster_delimiters= {'.tsv': '\t', '.csv': ','},
        cluster_id_column="cluster_id",
        cluster_filter: str = None,
        result_name: str = "spikes",
        rows_per_read: int = 2000,
        csv_dialect: str = 'excel',
        **csv_fmtparams
    ) -> None:
        """Create a new PhyClusterEventReader.

        Args:
            params_file:            Path to the Phy params.py file to read from, along with data files in the same dir.
            file_finder:            Utility to find() files in the conigured Pyramid configured search path.
                                    Pyramid will automatically create and pass in the file_finder for you.
            sample_rate_param_name: Name of the variable in params_file that holds spike event sample rate.
                                    Default is "sample_rate".
            spike_times_name:       File name with spike times in same dir as params_file. 
                                    Default is "spike_times.npy".
            spike_clusters_name:    File name with spike cluster ids in same dir as params_file.
                                    Default is "spike_clusters.npy".
            cluster_glob:           File "glob" pattern matching CSV/TSV files in same dir as params_file.
                                    Default is "cluster_*".
            cluster_delimiters:     Dictionary of CSV/TSV file extensions to delimiter characters.
                                    Default is {'.tsv': '\t', '.csv': ','}.
            cluster_id_column:      Column name in CSV/TSV cluster files that contains the int cluster id.
                                    Default is "cluster_id".
            cluster_filter:         String Python expression used to filter spikes by cluster info.
                                    The expression must return True or False, whether to take or ignore spikes from a given cluster.
                                    Any column names from cluster CSV/TSV files may be used as local variables in the expression.
                                    For example: 'Amplitude > 10000', 'KSLabel != "mua"', 'group = "good"', etc.
                                    Default is None, to take all clusters. 
            result_name:            Name of the Pyramid reader results (and default buffer name) to use.
                                    Default is "spikes".
            rows_per_read:          How many rows of spike_times_name and spike_clusters_name to read per call to read_next().
                                    This reader will read spike and cluster files incrementally to limit memory usage.
                                    Default is 2000 rows.
            csv_dialect:            Python csv module "dialect" to use when reading cluster CSV/TSV files
                                    Default is "excel".
            **csv_fmtparams         Python csv module "fmtparams" kwargs to use when reading cluster CSV/TSV files
                                    Default is {}.
        """

        self.params_file = file_finder.find(params_file)

        phy_folder = Path(self.params_file).parent
        self.sample_rate_param_name = sample_rate_param_name
        self.spike_times_file = Path(phy_folder, spike_times_name)
        self.spike_clusters_file = Path(phy_folder, spike_clusters_name)

        self.custer_files = phy_folder.glob(cluster_glob)
        self.cluster_delimiters = cluster_delimiters
        self.cluster_id_column = cluster_id_column
        self.cluster_filter = cluster_filter

        self.result_name = result_name
        self.rows_per_read = rows_per_read

        self.csv_dialect = csv_dialect
        self.csv_fmtparams = csv_fmtparams

        self.current_row = None
        self.spikes_times = None
        self.spike_clusters = None
        self.sample_rate = None
        self.clusters_to_keep = None

    def __enter__(self) -> Self:
        # Start reading spikes and clusters at the beginning.
        self.current_row = 0
        self.spikes_times = np.load(self.spike_times_file, mmap_mode="r")
        self.spike_clusters = np.load(self.spike_clusters_file, mmap_mode="r")

        # Parse the spike sample rate to convert samples to seconds.
        with open(self.params_file, "r") as f:
            for line in f:
                (name, value) = line.split("=")
                if name.strip() == self.sample_rate_param_name:
                    self.sample_rate = float(value.strip())

        if self.sample_rate is None:  # pragma: no cover
            raise ValueError(f"Params file {self.params_file} has no entry for {self.sample_rate_param_name}.")

        # Parse cluster info files and decide which clusters to keep.
        if self.cluster_filter:
            # Read in info about each cluster.
            cluster_info = {}
            for cluster_file in self.custer_files:
                # See https://docs.python.org/3/library/csv.html#id3 for why this has newline=''
                with open(cluster_file, mode='r', newline='') as f:
                    delimiter = self.cluster_delimiters.get(cluster_file.suffix.lower(), ',')
                    csv_reader = csv.DictReader(f, delimiter=delimiter, dialect=self.csv_dialect, **self.csv_fmtparams)
                    for row in csv_reader:
                        cluster_id = int(row[self.cluster_id_column])
                        info = cluster_info.get(cluster_id, {})

                        # Convert cluster entries to numbers, when possible.
                        for name, value in row.items():
                            try:
                                info[name] = float(value)
                            except:
                                info[name] = value

                        cluster_info[cluster_id] = info

            # Decide which clusters to keep, based on info and given expression.
            filter_compiled = compile(self.cluster_filter, '<string>', 'eval')
            self.clusters_to_keep = []
            for cluster_id, info in cluster_info.items():
                try:
                    result = eval(filter_compiled, {}, info)
                    keep = bool(result)
                except:
                    keep = False
                if keep:
                    self.clusters_to_keep.append(cluster_id)

        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        self.spikes_times = None
        self.spike_clusters = None

    def read_next(self) -> dict[str, BufferData]:
        if self.current_row >= self.spikes_times.size:
            # Reached the end of the spikes, all done.
            raise StopIteration

        # Read the next increment of spike times and corresponding cluster ids.
        until_row = min(self.spikes_times.size, self.current_row + self.rows_per_read)
        times = self.spikes_times[self.current_row:until_row] / self.sample_rate
        clusters = self.spike_clusters[self.current_row:until_row]
        self.current_row = until_row

        if self.clusters_to_keep is None:
            # Take spikes from all clusters.
            selected_times = times
            selected_clusters = clusters
        else:
            # Take spikes from select clusters, only.
            selector = np.in1d(clusters, self.clusters_to_keep)
            selected_times = times[selector]
            selected_clusters = clusters[selector]

        if selected_times.size > 0:
            # [time, cluster_id]
            event_data = np.concatenate([selected_times, selected_clusters], axis=1)
            return {
                self.result_name: NumericEventList(event_data)
            }
        else:
            return None

    def get_initial(self) -> dict[str, BufferData]:
        return {
            # [time, cluster_id]
            self.result_name: NumericEventList(np.empty([0, 2]))
        }
