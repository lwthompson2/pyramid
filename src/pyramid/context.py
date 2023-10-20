from typing import Any, Self
from pathlib import Path
import time
import logging
from contextlib import ExitStack
from dataclasses import dataclass
import yaml
import graphviz

from pyramid.file_finder import FileFinder
from pyramid.model.model import Buffer, DynamicImport
from pyramid.model.events import NumericEventList
from pyramid.model.signals import SignalChunk
from pyramid.neutral_zone.readers.readers import Reader, ReaderRoute, ReaderRouter, Transformer, ReaderSyncConfig, ReaderSyncRegistry
from pyramid.neutral_zone.readers.delay_simulator import DelaySimulatorReader
from pyramid.trials.trials import TrialDelimiter, TrialExtractor, TrialEnhancer, TrialExpression
from pyramid.trials.trial_file import TrialFile
from pyramid.plotters.plotters import Plotter, PlotFigureController


def graphviz_format(text: str) -> str:
    """Escape special characters in text used in GraphViz labels."""
    escaped_text = text
    for char in ["<", ">", "{", "}", "|"]:
        escaped_text = escaped_text.replace(char, f"\\{char}")
    if len(escaped_text) > 30:
        escaped_text = escaped_text[0:13] + "..." + escaped_text[-13:]
    return escaped_text


def graphviz_record_label(title: str, info: dict[str, Any]):
    """Format a GraphViz "record" label with multiple lines."""
    label = graphviz_format(title)
    for key, value in info.items():
        if isinstance(value, dict) and len(value) > 0:
            escaped_items = [f"{graphviz_format(str(k))}: {graphviz_format(str(v))}" for k, v in value.items()]
            item_rows = "|".join(escaped_items)
            label += f"|{{{graphviz_format(key)}: |{{ {item_rows} }}}}"
        elif isinstance(value, list) and len(value) > 0:
            escaped_items = [graphviz_format(str(item)) for item in value]
            item_rows = "|".join(escaped_items)
            label += f"|{{{graphviz_format(key)}: |{{ {item_rows} }}}}"
        else:
            label += f"|{graphviz_format(key)}: {graphviz_format(str(value))}\l"
    return label


def graphviz_label(object: DynamicImport):
    """Format a GraphViz "record" label for a Pyramid DynamicImport object."""
    return graphviz_record_label(object.__class__.__name__, object.kwargs)


@dataclass
class PyramidContext():
    """Pyramid context holds everything needed to run Pyramid including experiment YAML, CLI args, etc."""
    subject: dict[str, Any]
    experiment: dict[str, Any]
    readers: dict[str, Reader]
    named_buffers: dict[str, Buffer]
    start_router: ReaderRouter
    routers: dict[str, ReaderRouter]
    trial_delimiter: TrialDelimiter
    trial_extractor: TrialExtractor
    sync_registry: ReaderSyncRegistry
    plot_figure_controller: PlotFigureController
    file_finder: FileFinder

    @classmethod
    def from_yaml_and_reader_overrides(
        cls,
        experiment_yaml: str,
        subject_yaml: str = None,
        reader_overrides: list[str] = [],
        allow_simulate_delay: bool = False,
        plot_positions_yaml: str = None,
        search_path: list[str] = []
    ) -> Self:
        """Load a context the way it comes from the CLI, with a YAML files etc."""
        file_finder = FileFinder(search_path)

        with open(file_finder.find(experiment_yaml)) as f:
            experiment_config = yaml.safe_load(f)

        # For example, command line might have "--readers start_reader.csv_file=real.csv",
        # which should be equivalent to start_reader kwargs "csv_file=real.csv".
        if reader_overrides:
            for override in reader_overrides:
                (reader_name, assignment) = override.split(".", maxsplit=1)
                (property, value) = assignment.split("=", maxsplit=1)
                reader_config = experiment_config["readers"][reader_name]
                reader_args = reader_config.get("args", {})
                reader_args[property] = value
                reader_config["args"] = reader_args

        if subject_yaml:
            with open(file_finder.find(subject_yaml)) as f:
                subject_config = yaml.safe_load(f)
        else:
            subject_config = {}

        pyramid_context = cls.from_dict(
            experiment_config,
            subject_config,
            allow_simulate_delay,
            plot_positions_yaml,
            file_finder
        )
        return pyramid_context

    @classmethod
    def from_dict(
        cls,
        experiment_config: dict[str, Any],
        subject_config: dict[str, Any],
        allow_simulate_delay: bool = False,
        plot_positions_yaml: str = None,
        file_finder: FileFinder = FileFinder()
    ) -> Self:
        """Load a context after things like YAML files are already read into memory."""
        (readers, named_buffers, reader_routers, reader_sync_registry) = configure_readers(
            experiment_config["readers"],
            allow_simulate_delay,
            file_finder
        )
        (trial_delimiter, trial_extractor, start_buffer_name) = configure_trials(
            experiment_config["trials"],
            named_buffers,
            file_finder
        )

        # Rummage around in the configured reader routers for the one associated with the trial "start" delimiter.
        start_router = None
        for router in reader_routers.values():
            for buffer_name in router.named_buffers.keys():
                if buffer_name == start_buffer_name:
                    start_router = router

        plotters = configure_plotters(
            experiment_config.get("plotters", []),
            file_finder
        )
        subject = subject_config.get("subject", {})
        experiment = experiment_config.get("experiment", {})
        plot_figure_controller = PlotFigureController(
            plotters=plotters,
            experiment_info=experiment,
            subject_info=subject,
            plot_positions_yaml=file_finder.find(plot_positions_yaml)
        )
        return PyramidContext(
            subject=subject,
            experiment=experiment,
            readers=readers,
            named_buffers=named_buffers,
            start_router=start_router,
            routers=reader_routers,
            trial_delimiter=trial_delimiter,
            trial_extractor=trial_extractor,
            sync_registry=reader_sync_registry,
            plot_figure_controller=plot_figure_controller,
            file_finder=file_finder
        )

    def run_without_plots(self, trial_file: str) -> None:
        """Run without plots as fast as the data allow.

        Similar to run_with_plots(), below.
        It seemed nicer to have separate code paths, as opposed to lots of conditionals in one uber-function.
        run_without_plots() should run without touching any GUI code, avoiding potential host graphics config issues.
        """
        with ExitStack() as stack:
            # All these "context managers" will clean up automatically when the "with" exits.
            writer = stack.enter_context(TrialFile.for_file_suffix(self.file_finder.find(trial_file)))
            for reader in self.readers.values():
                stack.enter_context(reader)

            # Extract trials indefinitely, as they come.
            while self.start_router.still_going():
                got_start_data = self.start_router.route_next()
                if got_start_data:
                    new_trials = self.trial_delimiter.next()
                    for trial_number, new_trial in new_trials.items():
                        # Let all readers catch up to the trial end time.
                        for router in self.routers.values():
                            router.route_until(new_trial.end_time)

                        # Re-estimate clock drift for all readers using latest events from reference and other readers.
                        for router in self.routers.values():
                            router.update_drift_estimate(new_trial.end_time)

                        self.trial_extractor.populate_trial(new_trial, trial_number, self.experiment, self.subject)
                        writer.append_trial(new_trial)
                        self.trial_delimiter.discard_before(new_trial.start_time)
                        self.trial_extractor.discard_before(new_trial.start_time)

            # Make a best effort to catch the last trial -- which would have no "next trial" to delimit it.
            for router in self.routers.values():
                router.route_next()
            # Re-estimate clock drift for all readers using last events from reference and other readers.
            for router in self.routers.values():
                router.update_drift_estimate()
            (last_trial_number, last_trial) = self.trial_delimiter.last()
            if last_trial:
                self.trial_extractor.populate_trial(last_trial, last_trial_number, self.experiment, self.subject)
                writer.append_trial(last_trial)

        # TODO: reopen the trial file and run the trial_extractor collecters
        #       open the original file to read from
        #       iterate the original file and collect() over all trials, in memory
        #
        #       open the original file to read from
        #       open a temp file to write to
        #       iterate the original file, and for each trial enhance() in memory and write to the temp file
        #       if all went well, replace the original file with the temp file

    def run_with_plots(self, trial_file: str, plot_update_period: float = 0.025) -> None:
        """Run with plots and interactive GUI updates.

        Similar to run_without_plots(), above.
        It seemed nicer to have separate code paths, as opposed to lots of conditionals in one uber-function.
        run_without_plots() should run without touching any GUI code, avoiding potential host graphics config issues.
        """
        with ExitStack() as stack:
            # All these "context managers" will clean up automatically when the "with" exits.
            writer = stack.enter_context(TrialFile.for_file_suffix(self.file_finder.find(trial_file)))
            for reader in self.readers.values():
                stack.enter_context(reader)
            stack.enter_context(self.plot_figure_controller)

            # Extract trials indefinitely, as they come.
            next_gui_update = time.time()
            while self.start_router.still_going() and self.plot_figure_controller.stil_going():
                if time.time() > next_gui_update:
                    self.plot_figure_controller.update()
                    next_gui_update += plot_update_period

                got_start_data = self.start_router.route_next()

                if got_start_data:
                    new_trials = self.trial_delimiter.next()
                    for trial_number, new_trial in new_trials.items():
                        # Let all readers catch up to the trial end time.
                        for router in self.routers.values():
                            router.route_until(new_trial.end_time)

                        # Re-estimate clock drift for all readers using latest events from reference and other readers.
                        for router in self.routers.values():
                            router.update_drift_estimate(new_trial.end_time)

                        self.trial_extractor.populate_trial(new_trial, trial_number, self.experiment, self.subject)
                        writer.append_trial(new_trial)
                        self.plot_figure_controller.plot_next(new_trial, trial_number)
                        self.trial_delimiter.discard_before(new_trial.start_time)
                        self.trial_extractor.discard_before(new_trial.start_time)

            # Make a best effort to catch the last trial -- which would have no "next trial" to delimit it.
            for router in self.routers.values():
                router.route_next()
            # Re-estimate clock drift for all readers using last events from reference and other readers.
            for router in self.routers.values():
                router.update_drift_estimate()
            (last_trial_number, last_trial) = self.trial_delimiter.last()
            if last_trial:
                self.trial_extractor.populate_trial(last_trial, last_trial_number, self.experiment, self.subject)
                writer.append_trial(last_trial)
                self.plot_figure_controller.plot_next(last_trial, last_trial_number)

        # TODO: reopen the trial file and run the trial_extractor collecters
        #       open the original file to read from
        #       iterate the original file and collect() over all trials, in memory
        #
        #       open the original file to read from
        #       open a temp file to write to
        #       iterate the original file, and for each trial enhance() in memory and write to the temp file
        #       if all went well, replace the original file with the temp file

    def to_graphviz(self, graph_name: str, out_file: str):
        """Do introspection of loaded config and write out a graphviz "dot" file and overview image for viewing."""

        # Set up a directed graph and some visual styling.
        dot = graphviz.Digraph(
            name=graph_name,
            graph_attr={
                "rankdir": "LR",
                "label": graph_name,
                "labeljust": "l",
                "splines": "false",
                "overlap": "scale",
                "outputorder": "edgesfirst",
                "fontname": "Arial"
            },
            node_attr={
                "penwidth": "2.0",
                "shape": "record",
                "style": "filled",
                "fillcolor": "white",
                "fontname": "Arial"
            },
            edge_attr={
                "penwidth": "2.0",
                "fontname": "Arial"
            }
        )
        subgraph_attr = {
            "color": "transparent",
            "bgcolor": "lightgray",
            "rank": "same",
            "margin": "20",
            "fontname": "Arial"
        }

        # Start the graph with a node for each buffer.
        start_buffer_name = None
        wrt_buffer_name = None
        with dot.subgraph(name="cluster_buffers", graph_attr={"label": "buffers", **subgraph_attr}) as buffers:
            event_list_name = "event_list"
            event_list_label = ""
            signal_chunk_name = "signal_chunk"
            signal_chunk_label = ""
            for name, buffer in self.named_buffers.items():
                if buffer is self.trial_delimiter.start_buffer:
                    start_buffer_name = name
                if buffer is self.trial_extractor.wrt_buffer:
                    wrt_buffer_name = name

                if isinstance(buffer.data, NumericEventList):
                    event_list_label += f"|<{name}>{name}"
                elif isinstance(buffer.data, SignalChunk):
                    signal_chunk_label += f"|<{name}>{name}"
            if event_list_label:
                buffers.node(name=event_list_name, label="NumericEventList" + event_list_label)
            if signal_chunk_label:
                buffers.node(name=signal_chunk_name, label="SignalChunk" + signal_chunk_label)

        # Note which buffer will be used for delimiting trials in time.
        delimiter_name = "trial_delimiter"
        delimiter_label = f"{self.trial_delimiter.__class__.__name__}|start = {self.trial_delimiter.start_value}"
        dot.node(name=delimiter_name, label=delimiter_label)
        dot.edge(f"{event_list_name}:{start_buffer_name}:e", delimiter_name)

        # Note which buffer will be used for aligning trials in time.
        extractor_name = "trial_extractor"
        extractor_label = f"{self.trial_extractor.__class__.__name__}|wrt = {self.trial_extractor.wrt_value}"
        dot.node(name=extractor_name, label=extractor_label)
        dot.edge(f"{event_list_name}:{wrt_buffer_name}:e", extractor_name)

        # Show how each trial will get enhanced after delimiting and alignment.
        with dot.subgraph(name="cluster_enhancers", graph_attr={"label": "enhancers", **subgraph_attr}) as enhancers:
            for index, (enhancer, when) in enumerate(self.trial_extractor.enhancers.items()):
                enhancer_name = f"enhancer_{index}"
                enhancer_label = graphviz_label(enhancer)
                if when is not None:
                    enhancer_label += f"|when {graphviz_format(when.expression)}"
                enhancers.node(name=enhancer_name, label=enhancer_label)
                dot.edge(f"{extractor_name}:e", f"{enhancer_name}:w")

        # TODO: also show collecters

        # Show each reader and its configuration.
        with dot.subgraph(name="cluster_readers", graph_attr={"label": "readers", **subgraph_attr}) as readers:
            for name, router in self.routers.items():
                reader_label = f"{name}|{graphviz_label(router.reader)}"
                if router.sync_config:
                    if router.sync_config.event_value:
                        # This reader will read events to keep track of clock sync.
                        sync_info = f"{router.sync_config.reader_result_name}[{router.sync_config.event_value_index}] == {router.sync_config.event_value}"
                        if router.sync_config.is_reference:
                            reader_label += f"| sync ref {sync_info}\l"
                        else:
                            reader_label += f"| sync on {sync_info}\l"
                    elif router.sync_config.reader_name != name:
                        # This reader will borrow clock sync results from another reader.
                        reader_label += f"| sync like {router.sync_config.reader_name}\l"
                readers.node(name=name, label=reader_label)

        # Show the configured results coming from each reader.
        with dot.subgraph(name="cluster_results", graph_attr={"label": "results", **subgraph_attr}) as results:
            for name, router in self.routers.items():
                results_name = f"{name}_results"
                results_labels = [f"<{key}>{key}" for key in router.reader.get_initial().keys()]
                results_label = "|".join(results_labels)
                results.node(name=results_name, label=results_label)
                dot.edge(name, results_name)

        # Continue the graph with a node for each reader and its configured result names.
        # Connect the reader results to the configured buffers.
        for name, router in self.routers.items():
            results_name = f"{name}_results"
            for index, route in enumerate(router.routes):
                route_name = f"{name}_route_{index}"
                buffer = self.named_buffers[route.buffer_name]
                if isinstance(buffer.data, NumericEventList):
                    buffer_node_name = event_list_name
                elif isinstance(buffer.data, SignalChunk):
                    buffer_node_name = signal_chunk_name

                if route.transformers:
                    labels = [graphviz_label(transformer) for transformer in route.transformers]
                    route_label = "|".join(labels)
                    dot.node(name=route_name, label=route_label)
                    dot.edge(f"{results_name}:{route.reader_result_name}:e", f"{route_name}:w")
                    dot.edge(f"{route_name}:e", f"{buffer_node_name}:{route.buffer_name}:w")
                else:
                    dot.edge(f"{results_name}:{route.reader_result_name}:e", f"{buffer_node_name}:{route.buffer_name}:w")

        # Render the graph and write to disk.
        out_path = Path(out_file)
        file_name = f"{out_path.stem}.dot"
        dot.render(directory=out_path.parent, filename=file_name, outfile=out_path)


def configure_readers(
    readers_config: dict[str, dict],
    allow_simulate_delay: bool = False,
    file_finder: FileFinder = FileFinder()
) -> tuple[dict[str, Reader], dict[str, Buffer], dict[str, ReaderRouter]]:
    """Load the "readers:" section of an experiment YAML file."""

    readers = {}
    named_buffers = {}
    routers = {}

    # We'll update the reference_reader_name below based on individual reader sync config.
    reader_sync_registry = ReaderSyncRegistry(reference_reader_name=None)

    logging.info(f"Using {len(readers_config)} readers.")
    for (reader_name, reader_config) in readers_config.items():
        # Instantiate the reader by dynamic import.
        reader_class = reader_config["class"]
        logging.info(f"  {reader_class}")
        package_path = reader_config.get("package_path", None)
        reader_args = reader_config.get("args", {})
        simulate_delay = allow_simulate_delay and reader_config.get("simulate_delay", False)
        reader = Reader.from_dynamic_import(
            reader_class,
            file_finder,
            external_package_path=package_path,
            **reader_args
        )
        if simulate_delay:
            reader = DelaySimulatorReader(reader)
        readers[reader_name] = reader

        # Configure default, pass-through routes for the reader.
        initial_results = reader.get_initial()
        named_routes = {buffer_name: ReaderRoute(buffer_name, buffer_name) for buffer_name in initial_results.keys()}

        # Update default routes with explicitly configured aliases and transformations.
        buffers_config = reader_config.get("extra_buffers", {})
        for buffer_name, buffer_config in buffers_config.items():

            # Instantiate transformers by dynamic import.
            transformers = []
            transformers_config = buffer_config.get("transformers", [])
            logging.info(f"Buffer {buffer_name} using {len(transformers_config)} transformers.")
            for transformer_config in transformers_config:
                transformer_class = transformer_config["class"]
                logging.info(f"  {transformer_class}")
                package_path = transformer_config.get("package_path", None)
                transformer_args = transformer_config.get("args", {})
                transformer = Transformer.from_dynamic_import(
                    transformer_class,
                    file_finder,
                    external_package_path=package_path,
                    **transformer_args
                )
                transformers.append(transformer)

            reader_result_name = buffer_config.get("reader_result_name", buffer_name)
            route = ReaderRoute(reader_result_name, buffer_name, transformers)
            named_routes[buffer_name] = route

        # Create a buffer to receive data from each route.
        reader_buffers = {}
        for route in named_routes.values():
            initial_data = initial_results[route.reader_result_name]
            if initial_data is not None:
                data_copy = initial_data.copy()
                for transformer in route.transformers:
                    data_copy = transformer.transform(data_copy)
                reader_buffers[route.buffer_name] = Buffer(data_copy)

        # Configure sync events for correcting clock drift for this reader.
        sync_config = reader_config.get("sync", {})
        if sync_config:
            sync_config_plus_default = {"reader_name": reader_name, **sync_config}
            reader_sync_config = ReaderSyncConfig(**sync_config_plus_default)

            # Fill in the reference reader name which had a None placeholder, above.
            if reader_sync_config.is_reference:
                reader_sync_registry.reference_reader_name = reader_name
        else:
            reader_sync_config = None

        # Create a router to route data from the reader along each configured route to its buffer.
        empty_reads_allowed = reader_config.get("empty_reads_allowed", 3)
        router = ReaderRouter(
            reader=reader,
            routes=list(named_routes.values()),
            named_buffers=reader_buffers,
            empty_reads_allowed=empty_reads_allowed,
            sync_config=reader_sync_config,
            sync_registry=reader_sync_registry
        )
        routers[reader_name] = router
        named_buffers.update(router.named_buffers)

    logging.info(f"Using {len(named_buffers)} named buffers.")
    for name in named_buffers.keys():
        logging.info(f"  {name}")

    return (readers, named_buffers, routers, reader_sync_registry)


def configure_trials(
    trials_config: dict[str, Any],
    named_buffers: dict[str, Buffer],
    file_finder: FileFinder = FileFinder()
) -> tuple[TrialDelimiter, TrialExtractor, str]:
    """Load the "trials:" section of an experiment YAML file."""

    start_buffer_name = trials_config.get("start_buffer", "start")
    start_value = trials_config.get("start_value", 0.0)
    start_value_index = trials_config.get("start_value_index", 0)
    trial_start_time = trials_config.get("trial_start_time", 0.0)
    trial_count = trials_config.get("trial_count", 0)
    trial_delimiter = TrialDelimiter(
        start_buffer=named_buffers[start_buffer_name],
        start_value=start_value,
        start_value_index=start_value_index,
        start_time=trial_start_time,
        trial_count=trial_count
    )

    wrt_buffer_name = trials_config.get("wrt_buffer", "wrt")
    wrt_value = trials_config.get("wrt_value", 0.0)
    wrt_value_index = trials_config.get("wrt_value_index", 0)

    other_buffers = {name: buffer for name, buffer in named_buffers.items()
                     if name != start_buffer_name and name != wrt_buffer_name}

    enhancers = {}
    enhancers_config = trials_config.get("enhancers", [])
    logging.info(f"Using {len(enhancers_config)} per-trial enhancers.")
    for enhancer_config in enhancers_config:
        enhancer_class = enhancer_config["class"]
        package_path = enhancer_config.get("package_path", None)
        enhancer_args = enhancer_config.get("args", {})
        enhancer = TrialEnhancer.from_dynamic_import(
            enhancer_class,
            file_finder,
            external_package_path=package_path,
            **enhancer_args
        )

        when_string = enhancer_config.get("when", None)
        if when_string is not None:
            logging.info(f"  {enhancer_class} when {when_string}")
            when_expression = TrialExpression(expression=when_string, default_value=False)
        else:
            logging.info(f"  {enhancer_class}")
            when_expression = None

        enhancers[enhancer] = when_expression

    # TODO: also look for collecters with when expressions.

    # TODO: also pass in collectors with when expressions.
    trial_extractor = TrialExtractor(
        wrt_buffer=named_buffers[wrt_buffer_name],
        wrt_value=wrt_value,
        wrt_value_index=wrt_value_index,
        named_buffers=other_buffers,
        enhancers=enhancers
    )

    return (trial_delimiter, trial_extractor, start_buffer_name)


def configure_plotters(
    plotters_config: list[dict[str, str]],
    file_finder: FileFinder = FileFinder()
) -> list[Plotter]:
    """Load the "plotters:" section of an experiment YAML file."""

    if not plotters_config:
        logging.info(f"No plotters.")
        return []

    logging.info(f"Using {len(plotters_config)} plotters.")
    plotters = []
    for plotter_config in plotters_config:
        plotter_class = plotter_config["class"]
        logging.info(f"  {plotter_class}")
        package_path = plotter_config.get("package_path", None)
        plotter_args = plotter_config.get("args", {})
        plotter = Plotter.from_dynamic_import(
            plotter_class,
            file_finder,
            external_package_path=package_path,
            **plotter_args
        )
        plotters.append(plotter)

    return plotters
