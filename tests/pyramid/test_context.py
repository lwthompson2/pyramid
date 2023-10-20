from pathlib import Path
from pytest import fixture
import yaml
import numpy as np

from pyramid.model.model import Buffer
from pyramid.model.events import NumericEventList
from pyramid.neutral_zone.readers.readers import ReaderRoute, ReaderRouter, ReaderSyncConfig, ReaderSyncRegistry
from pyramid.neutral_zone.readers.delay_simulator import DelaySimulatorReader
from pyramid.neutral_zone.readers.csv import CsvNumericEventReader
from pyramid.neutral_zone.transformers.standard_transformers import OffsetThenGain

from pyramid.trials.trials import TrialDelimiter, TrialExtractor, TrialExpression
from pyramid.trials.standard_enhancers import TrialDurationEnhancer

from pyramid.plotters.plotters import PlotFigureController
from pyramid.plotters.standard_plotters import BasicInfoPlotter, NumericEventsPlotter, SignalChunksPlotter

from pyramid.file_finder import FileFinder
from pyramid.context import PyramidContext, configure_readers, configure_trials, configure_plotters, graphviz_format, graphviz_record_label


@fixture
def fixture_path(request):
    this_file = Path(request.module.__file__)
    return Path(this_file.parent, 'fixture_files')


def test_graphviz_format():
    assert graphviz_format("a b c") == "a b c"
    assert graphviz_format("<a> {b} |c|") == "\\<a\\> \\{b\\} \\|c\\|"
    assert graphviz_format("1234567890_1234567890_1234567890") == "1234567890_12...90_1234567890"


def test_graphviz_record_label():
    title = "Test"
    info = {
        "number": 10/3,
        "string": "abc",
        "empty_dict": {},
        "empty_list": [],
        "dict": {"a": 1, "b": 2},
        "list": ["a", 1, "b", 2]
    }
    label = graphviz_record_label(title, info)
    expected_label_parts = [
        "Test",
        "number: 3.3333333333333335\\l",
        "string: abc\\l",
        "empty_dict: \\{\\}\\l",
        "empty_list: []\\l",
        "{dict: |{ a: 1|b: 2 }}",
        "{list: |{ a|1|b|2 }}"
    ]
    expected_label = "|".join(expected_label_parts)
    assert label == expected_label


def test_configure_readers():
    readers_config = {
        "start_reader": {
            "class": "pyramid.neutral_zone.readers.csv.CsvNumericEventReader",
            "args": {
                "csv_file": "default.csv",
                "result_name": "start"
            },
            "simulate_delay": True,
            "sync": {
                "is_reference": True,
                "reader_result_name": "start",
                "event_value": 1010
            }
        },
        "wrt_reader": {
            "class": "pyramid.neutral_zone.readers.csv.CsvNumericEventReader",
            "args": {"result_name": "wrt"},
            "sync": {
                "reader_name": "start_reader"
            }
        },
        "foo_reader": {
            "class": "pyramid.neutral_zone.readers.csv.CsvNumericEventReader",
            "args": {"result_name": "foo"}
        },
        "bar_reader": {
            "class": "pyramid.neutral_zone.readers.csv.CsvNumericEventReader",
            "args": {"result_name": "bar"},
            "extra_buffers": {
                "bar_2": {
                    "reader_result_name": "bar",
                    "transformers": [
                        {
                            "class": "pyramid.neutral_zone.transformers.standard_transformers.OffsetThenGain",
                            "args": {
                                "offset": 10,
                                "gain": -2
                            }
                        }
                    ]
                }
            },
        }
    }

    # TODO: add a trial collecter to the test config.

    allow_simulate_delay = True
    (readers, named_buffers, reader_routers, sync_registry) = configure_readers(readers_config, allow_simulate_delay)

    expected_readers = {
        "start_reader": DelaySimulatorReader(CsvNumericEventReader("default.csv", result_name="start")),
        "wrt_reader": CsvNumericEventReader(result_name="wrt"),
        "foo_reader": CsvNumericEventReader(result_name="foo"),
        "bar_reader": CsvNumericEventReader(result_name="bar"),
    }
    assert readers == expected_readers

    expected_named_buffers = {
        "start": Buffer(NumericEventList(np.empty([0, 2]))),
        "wrt": Buffer(NumericEventList(np.empty([0, 2]))),
        "foo": Buffer(NumericEventList(np.empty([0, 2]))),
        "bar": Buffer(NumericEventList(np.empty([0, 2]))),
        "bar_2": Buffer(NumericEventList(np.empty([0, 2]))),
    }
    assert named_buffers == expected_named_buffers

    sync = ReaderSyncConfig(is_reference=True, reader_result_name="start", event_value=1010, reader_name="start_reader")
    expected_reader_routers = {
        "start_reader": ReaderRouter(
            expected_readers["start_reader"],
            [ReaderRoute("start", "start")],
            {"start": expected_named_buffers["start"]},
            sync_config=sync
        ),
        "wrt_reader": ReaderRouter(
            expected_readers["wrt_reader"],
            [ReaderRoute("wrt", "wrt")],
            {"wrt": expected_named_buffers["wrt"]},
            sync_config=ReaderSyncConfig(reader_name="start_reader")
        ),
        "foo_reader": ReaderRouter(
            expected_readers["foo_reader"],
            [ReaderRoute("foo", "foo")],
            {"foo": expected_named_buffers["foo"]}
        ),
        "bar_reader": ReaderRouter(
            expected_readers["bar_reader"],
            [
                ReaderRoute("bar", "bar"),
                ReaderRoute("bar", "bar_2", transformers=[OffsetThenGain(offset=10, gain=-2)])
            ],
            {
                "bar": expected_named_buffers["bar"],
                "bar_2": expected_named_buffers["bar_2"]
            }
        ),
    }
    assert reader_routers == expected_reader_routers

    expected_sync_registry = ReaderSyncRegistry("start_reader")
    assert sync_registry == expected_sync_registry


def test_configure_trials():
    trials_config = {
        "start_buffer": "start",
        "start_value": 1010,
        "wrt_reader": "wrt",
        "wrt_value": 42,
        "enhancers": [
            {
                "class": "pyramid.trials.standard_enhancers.TrialDurationEnhancer"
            },
            {
                "class": "pyramid.trials.standard_enhancers.TrialDurationEnhancer",
                "args": {"default_duration": 1.0},
                "when": "1==2"
            }
        ]
    }

    # TODO: add a trial collecter to the test config.

    named_buffers = {
        "start": Buffer(NumericEventList(np.empty([0, 2]))),
        "wrt": Buffer(NumericEventList(np.empty([0, 2])))
    }
    (trial_delimiter, trial_extractor, start_buffer_name) = configure_trials(trials_config, named_buffers)

    expected_trial_delimiter = TrialDelimiter(named_buffers["start"], start_value=1010)
    assert trial_delimiter == expected_trial_delimiter

    expected_other_buffers = {
        name: value
        for name, value in named_buffers.items()
        if name != "start" and name != "wrt"
    }
    expected_enhancers = {
        TrialDurationEnhancer(): None,
        TrialDurationEnhancer(default_duration=1.0): TrialExpression(expression="1==2", default_value=False)
    }
    expected_trial_extractor = TrialExtractor(
        named_buffers["wrt"],
        wrt_value=42,
        named_buffers=expected_other_buffers,
        enhancers=expected_enhancers)
    assert trial_extractor == expected_trial_extractor

    assert start_buffer_name == trials_config["start_buffer"]


def test_configure_plotters():
    plotters_config = [
        {"class": "pyramid.plotters.standard_plotters.BasicInfoPlotter"},
        {"class": "pyramid.plotters.standard_plotters.NumericEventsPlotter"},
        {"class": "pyramid.plotters.standard_plotters.SignalChunksPlotter"}
    ]
    plotters = configure_plotters(plotters_config)

    expected_plotters = [BasicInfoPlotter(), NumericEventsPlotter(), SignalChunksPlotter()]

    assert len(plotters) == len(expected_plotters)
    plotter_types_equal = [isinstance(a, b.__class__) for a, b in zip(plotters, expected_plotters)]
    assert all(plotter_types_equal)


def test_from_yaml_and_reader_overrides(fixture_path):
    experiment_yaml = Path(fixture_path, "experiment.yaml").as_posix()
    subject_yaml = Path(fixture_path, "subject.yaml").as_posix()
    delimiter_csv = Path(fixture_path, "delimiter.csv").as_posix()

    reader_overrides = [
        f"start_reader.csv_file={delimiter_csv}"
    ]

    allow_simulate_delay = True
    search_path = ["test/path"]
    context = PyramidContext.from_yaml_and_reader_overrides(
        experiment_yaml,
        subject_yaml,
        reader_overrides,
        allow_simulate_delay,
        search_path=search_path
    )

    with open(subject_yaml) as f:
        expected_subject = yaml.safe_load(f)

    with open(experiment_yaml) as f:
        expected_experiment = yaml.safe_load(f)

    expected_readers = {
        "start_reader": DelaySimulatorReader(CsvNumericEventReader(delimiter_csv, result_name="start")),
        "wrt_reader": CsvNumericEventReader(result_name="wrt"),
        "foo_reader": CsvNumericEventReader(result_name="foo"),
        "bar_reader": CsvNumericEventReader(result_name="bar"),
    }

    expected_named_buffers = {
        "start": Buffer(NumericEventList(np.empty([0, 2]))),
        "wrt": Buffer(NumericEventList(np.empty([0, 2]))),
        "foo": Buffer(NumericEventList(np.empty([0, 2]))),
        "bar": Buffer(NumericEventList(np.empty([0, 2]))),
        "bar_2": Buffer(NumericEventList(np.empty([0, 2]))),
    }

    sync = ReaderSyncConfig(is_reference=True, reader_result_name="start", event_value=1010, reader_name="start_reader")
    expected_reader_routers = {
        "start_reader": ReaderRouter(
            expected_readers["start_reader"],
            [ReaderRoute("start", "start")],
            {"start": expected_named_buffers["start"]},
            sync_config=sync
        ),
        "wrt_reader": ReaderRouter(
            expected_readers["wrt_reader"],
            [ReaderRoute("wrt", "wrt")],
            {"wrt": expected_named_buffers["wrt"]},
            sync_config=ReaderSyncConfig(reader_name="start_reader")
        ),
        "foo_reader": ReaderRouter(
            expected_readers["foo_reader"],
            [ReaderRoute("foo", "foo")],
            {"foo": expected_named_buffers["foo"]}
        ),
        "bar_reader": ReaderRouter(
            expected_readers["bar_reader"],
            [
                ReaderRoute("bar", "bar"),
                ReaderRoute("bar", "bar_2", transformers=[OffsetThenGain(offset=10, gain=-2)])
            ],
            {
                "bar": expected_named_buffers["bar"],
                "bar_2": expected_named_buffers["bar_2"]
            }
        ),
    }

    expected_trial_delimiter = TrialDelimiter(expected_named_buffers["start"], start_value=1010)

    expected_other_buffers = {
        name: value
        for name, value in expected_named_buffers.items()
        if name != "start" and name != "wrt"
    }
    expected_enhancers = {
        TrialDurationEnhancer(): None,
        TrialDurationEnhancer(default_duration=1.0): TrialExpression(expression="1==2", default_value=False)
    }
    expected_trial_extractor = TrialExtractor(
        expected_named_buffers["wrt"],
        wrt_value=42,
        named_buffers=expected_other_buffers,
        enhancers=expected_enhancers
    )

    expected_sync_registry = ReaderSyncRegistry(reference_reader_name="start_reader")

    expected_plot_figure_controller = PlotFigureController(
        plotters=[BasicInfoPlotter(), NumericEventsPlotter(), SignalChunksPlotter()],
        subject_info=expected_subject["subject"],
        experiment_info=expected_experiment["experiment"]
    )

    expected_file_finder = FileFinder(search_path)

    expected_context = PyramidContext(
        subject=expected_subject["subject"],
        experiment=expected_experiment["experiment"],
        readers=expected_readers,
        named_buffers=expected_named_buffers,
        start_router=expected_reader_routers["start_reader"],
        routers=expected_reader_routers,
        trial_delimiter=expected_trial_delimiter,
        trial_extractor=expected_trial_extractor,
        sync_registry=expected_sync_registry,
        plot_figure_controller=expected_plot_figure_controller,
        file_finder=expected_file_finder
    )
    assert context == expected_context
