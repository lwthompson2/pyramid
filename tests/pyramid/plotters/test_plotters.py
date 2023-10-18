import sys
from pathlib import Path

import yaml

from pytest import fixture

import matplotlib.pyplot as plt

from pyramid.file_finder import FileFinder
from pyramid.trials.trials import Trial
from pyramid.plotters.plotters import Plotter, PlotFigureController, get_figure_position, set_figure_position, looks_like_tkinter
from pyramid.plotters.standard_plotters import NumericEventsPlotter, SignalChunksPlotter


@fixture
def tests_path(request):
    this_file = Path(request.module.__file__)
    return this_file.parent


def test_installed_plotter_dynamic_import():
    # Import a plotter that was installed in the usual way (eg by pip) along with pyramid itself.
    import_spec = "pyramid.plotters.standard_plotters.NumericEventsPlotter"
    plotter = Plotter.from_dynamic_import(import_spec, FileFinder())
    assert isinstance(plotter, Plotter)
    assert isinstance(plotter, NumericEventsPlotter)


def test_another_installed_plotter_dynamic_import():
    import_spec = "pyramid.plotters.standard_plotters.SignalChunksPlotter"
    plotter = Plotter.from_dynamic_import(import_spec, FileFinder())
    assert isinstance(plotter, Plotter)
    assert isinstance(plotter, SignalChunksPlotter)


def test_external_plotter_dynamic_import(tests_path):
    # Import a plotter from a local file that was not installed in a standard location (eg by pip).
    # We don't want to litter the sys.path, so check we cleaned up after importing.
    original_sys_path = sys.path.copy()
    plotter = Plotter.from_dynamic_import(
        'external_package.plotter_module.ExternalPlotter1',
        FileFinder(),
        tests_path.as_posix()
    )
    assert isinstance(plotter, Plotter)
    assert sys.path == original_sys_path


def test_another_external_plotter_dynamic_import(tests_path):
    original_sys_path = sys.path.copy()
    plotter = Plotter.from_dynamic_import(
        'external_package.plotter_module.ExternalPlotter2',
        FileFinder(),
        tests_path.as_posix()
    )
    assert isinstance(plotter, Plotter)
    assert sys.path == original_sys_path


def test_single_figure():
    trial = Trial(0.0, 1.0)
    plotter = NumericEventsPlotter()
    with PlotFigureController(plotters=[plotter]) as controller:
        assert len(controller.get_open_figures()) == 1
        assert len(plotter.history) == 0

        controller.plot_next(trial, None)
        assert len(plotter.history) == 1

        controller.plot_next(trial, None)
        assert len(plotter.history) == 2

    assert len(plotter.history) == 0
    assert len(controller.get_open_figures()) == 0


def test_multiple_figures():
    trial = Trial(0.0, 1.0)
    plotters = [NumericEventsPlotter(), SignalChunksPlotter(), NumericEventsPlotter()]
    with PlotFigureController(plotters) as controller:
        assert len(controller.get_open_figures()) == len(plotters)
        for plotter in plotters:
            assert len(plotter.history) == 0

        controller.plot_next(trial, None)
        for plotter in plotters:
            assert len(plotter.history) == 1

        controller.plot_next(trial, None)
        for plotter in plotters:
            assert len(plotter.history) == 2

    for plotter in plotters:
        assert len(plotter.history) == 0
    assert len(controller.get_open_figures()) == 0


def test_close_figure_early():
    trial = Trial(0.0, 1.0)
    plotters = [NumericEventsPlotter(), SignalChunksPlotter(), NumericEventsPlotter()]
    with PlotFigureController(plotters) as controller:
        assert len(controller.get_open_figures()) == len(plotters)
        for plotter in plotters:
            assert len(plotter.history) == 0

        controller.plot_next(trial, None)
        for plotter in plotters:
            assert len(plotter.history) == 1

        # As if user closed a figure unexpectedly.
        victim_figure = controller.figures[plotters[1]]
        plt.close(victim_figure)
        assert len(controller.get_open_figures()) == len(plotters) - 1

        controller.plot_next(trial, None)
        assert len(controller.get_open_figures()) == len(plotters) - 1
        assert len(plotters[0].history) == 2
        assert len(plotters[1].history) == 1
        assert len(plotters[2].history) == 2

    for plotter in plotters:
        assert len(plotter.history) == 0
    assert len(controller.get_open_figures()) == 0


def test_close_all_figures():
    plotters = [NumericEventsPlotter(), SignalChunksPlotter(), NumericEventsPlotter()]
    with PlotFigureController(plotters) as controller:
        assert len(controller.get_open_figures()) == len(plotters)
        assert len(controller.get_open_figures()) == 3
        assert controller.stil_going()

        plt.close(controller.figures[plotters[0]])
        controller.update()
        assert len(controller.get_open_figures()) == 2
        assert controller.stil_going()

        plt.close(controller.figures[plotters[1]])
        controller.update()
        assert len(controller.get_open_figures()) == 1
        assert controller.stil_going()

        plt.close(controller.figures[plotters[2]])
        controller.update()
        assert len(controller.get_open_figures()) == 0
        assert not controller.stil_going()


def test_please_quit():
    plotters = [NumericEventsPlotter(), SignalChunksPlotter(), NumericEventsPlotter()]
    with PlotFigureController(plotters) as controller:
        assert len(controller.get_open_figures()) == len(plotters)
        assert len(controller.get_open_figures()) == 3
        assert controller.stil_going()

        plotters[0].quit()
        assert not controller.stil_going()


def test_restore_figure_positions(tmp_path):
    plot_positions = {
        '1': {
            "height": 100,
            "width": 101,
            "x": 1,
            "y": 51,
        },
        '2': {
            "height": 200,
            "width": 202,
            "x": 2,
            "y": 52,
        },
        '3': {
            "height": 300,
            "width": 303,
            "x": 3,
            "y": 53,
        }
    }
    plot_positions_yaml = Path(tmp_path, "plot_positions.yaml").as_posix()
    with open(plot_positions_yaml, "w") as f:
        yaml.safe_dump(plot_positions, f)

    plotters = [NumericEventsPlotter(), SignalChunksPlotter(), NumericEventsPlotter()]
    with PlotFigureController(plotters, plot_positions_yaml=plot_positions_yaml) as controller:
        assert len(controller.get_open_figures()) == len(plotters)

        for fig in controller.figures.values():
            fig.canvas.flush_events()
            position = get_figure_position(fig)
            figure_key = str(fig.number)
            expected_position = plot_positions[figure_key]

            if looks_like_tkinter(fig):
                assert position == expected_position
            else:  # pragma: no cover
                # This case happens on the headless test server.
                # There, the Matplotlib backend is "agg" for writing image files,
                # so there's no figure window to position.
                assert position is None


def test_record_figure_positions(tmp_path):
    expected_plot_positions = {
        '1': {
            "height": 100,
            "width": 101,
            "x": 1,
            "y": 51,
        },
        '2': {
            "height": 200,
            "width": 202,
            "x": 2,
            "y": 52,
        },
        '3': {
            "height": 300,
            "width": 303,
            "x": 3,
            "y": 53,
        }
    }

    plot_positions_yaml = Path(tmp_path, "plot_positions.yaml").as_posix()
    plotters = [NumericEventsPlotter(), SignalChunksPlotter(), NumericEventsPlotter()]
    with PlotFigureController(plotters, plot_positions_yaml=plot_positions_yaml) as controller:
        assert len(controller.get_open_figures()) == len(plotters)

        for fig in controller.figures.values():
            figure_key = str(fig.number)
            plot_position = expected_plot_positions[figure_key]
            set_figure_position(fig, plot_position)
            fig.canvas.flush_events()
            is_tkinter = looks_like_tkinter(fig)

    with open(plot_positions_yaml, "r") as f:
        plot_positions = yaml.safe_load(f)

    if is_tkinter:
        assert plot_positions == expected_plot_positions
    else:  # pragma: no cover
        # This case happens on the headless test server.
        # There, the Matplotlib backend is "agg" for writing image files,
        # so there's no figure window to position.
        for position in plot_positions.values():
            assert position is None
