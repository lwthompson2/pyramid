from pathlib import Path
import numpy as np

from pyramid.file_finder import FileFinder
from pyramid.model.events import NumericEventList
from pyramid.trials.trials import Trial
from pyramid.trials.standard_enhancers import PairedCodesEnhancer, EventTimesEnhancer, ExpressionEnhancer


def test_paired_codes_enhancer(tmp_path):
    # Write out a .csv file with rules in it.
    rules_csv = Path(tmp_path, "rules.csv")
    with open(rules_csv, 'w') as f:
        f.write('type,value,name,base,min,max,scale,comment\n')
        f.write('id,42,foo,3000,2000,4000,0.25,this is just a comment\n')
        f.write('id,43,bar,3000,2000,4000,0.25,this is just a comment\n')
        f.write('value,44,baz,3000,2000,4000,0.25,this is just a comment\n')
        f.write('value,45,quux,3000,2000,4000,0.025,this is just a comment\n')
        f.write('ignore,777,ignore_me,3000,2000,4000,0.25,this is just a comment\n')

    enhancer = PairedCodesEnhancer(
        buffer_name="propcodes",
        rules_csv=rules_csv,
        file_finder=FileFinder()
    )

    # The "id" and "value" rows should be included.
    assert 42 in enhancer.rules.keys()
    assert 43 in enhancer.rules.keys()
    assert 44 in enhancer.rules.keys()
    assert 45 in enhancer.rules.keys()

    # Other rows should ne ignored.
    assert 777 not in enhancer.rules.keys()

    paired_code_data = [
        [0.0, 42.0],    # code for property "foo"
        [1, 3000],      # value 0
        [2, 43],        # code for property "bar"
        [3, 3005],      # value 1.25
        [4, 13],        # irrelevant
        [5, 44],        # code for property "baz"
        [6, 10000],     # irrelevant
        [7, 3600],      # value 150
        [8, 44],        # code for property "baz" (again)
        [9, 13],        # irrelevant
        [10, 3604],     # value 151
        [11, 45],       # code for property "quux"
        [12, 14],       # irrelevant
        [13, 20002],    # irrelevant
        [14, 15],       # irrelevant
        [15, 16],       # irrelevant
        [16, 3101],     # value 2.525 (quux has scale 10 time finer than the others)
    ]
    event_list = NumericEventList(event_data=np.array(paired_code_data))
    trial = Trial(
        start_time=0,
        end_time=20,
        wrt_time=0,
        numeric_events={
            "propcodes": event_list
        }
    )

    enhancer.enhance(trial, 0, {}, {})
    expected_enhancements = {
        "foo": 0.0,
        "bar": 1.25,
        "baz": 151.0,
        "quux": 2.5250000000000004,
    }
    assert trial.enhancements == expected_enhancements

    expected_categories = {
        "id": ["foo", "bar"],
        "value": ["baz", "quux"]
    }
    assert trial.enhancement_categories == expected_categories


def test_paired_codes_enhancer_multiple_csvs(tmp_path):
    # Write some .csv files with overlapping / overriding rules in them.
    rules_1_csv = Path(tmp_path, "rules_1.csv")
    with open(rules_1_csv, 'w') as f:
        f.write('type,value,name,base,min,max,scale,comment\n')
        f.write('id,42,foo,3000,2000,4000,0.25,this is just a comment\n')
        f.write('id,43,bar,3000,2000,4000,0.25,this is just a comment\n')
        f.write('value,44,baz,3000,2000,4000,0.25,this is just a comment\n')
    rules_2_csv = Path(tmp_path, "rules_2.csv")
    with open(rules_2_csv, 'w') as f:
        f.write('type,value,name,base,min,max,scale,comment\n')
        f.write('value,44,baz,3000,2000,4000,0.25,this is just a comment\n')
        f.write('value,45,quux,3000,2000,4000,0.025,this is just a comment\n')
    rules_3_csv = Path(tmp_path, "rules_3.csv")
    with open(rules_3_csv, 'w') as f:
        f.write('type,value,name,base,min,max,scale,comment\n')
        f.write('id,43,bar_2,3000,2000,4000,0.25,this is just a comment\n')
        f.write('value,45,quux_2,3000,2000,4000,0.025,this is just a comment\n')

    enhancer = PairedCodesEnhancer(
        buffer_name="propcodes",
        rules_csv=[rules_1_csv, rules_2_csv, rules_3_csv],
        file_finder=FileFinder()
    )

    # Only the "id" and "value" rows should be kept.
    # Expect the union of the first two csvs, with partial overrides from the last csv.
    expected_rules = {
        42: {'type': 'id', 'name': 'foo', 'base': 3000, 'min': 2000, 'max': 4000, 'scale': 0.25},
        43: {'type': 'id', 'name': 'bar_2', 'base': 3000, 'min': 2000, 'max': 4000, 'scale': 0.25},
        44: {'type': 'value', 'name': 'baz', 'base': 3000, 'min': 2000, 'max': 4000, 'scale': 0.25},
        45: {'type': 'value', 'name': 'quux_2', 'base': 3000, 'min': 2000, 'max': 4000, 'scale': 0.025}
    }
    assert enhancer.rules == expected_rules


def test_event_times_enhancer(tmp_path):
    # Write out a .csv file with rules in it.
    rules_csv = Path(tmp_path, "rules.csv")
    with open(rules_csv, 'w') as f:
        f.write('type,value,name,comment\n')
        f.write('time,42,foo,this is just a comment\n')
        f.write('time,43,bar,this is just a comment\n')
        f.write('time,44,baz,this is just a comment\n')
        f.write('ignore,777,this is just a comment\n')

    enhancer = EventTimesEnhancer(
        buffer_name="events",
        rules_csv=rules_csv,
        file_finder=FileFinder()
    )

    # The "time" rows should be included.
    assert 42 in enhancer.rules.keys()
    assert 43 in enhancer.rules.keys()
    assert 44 in enhancer.rules.keys()

    # Other rows should ne ignored.
    assert 777 not in enhancer.rules.keys()

    event_data = [
        [0.0, 42.0],    # code for event "foo"
        [1, 3000],      # irrelevant
        [2, 43],        # code for event "bar"
        [3, 3005],      # irrelevant
        [4, 13],        # irrelevant
        [5, 42.0],      # code for event "foo" (again)
        [6, 10000],     # irrelevant
    ]
    event_list = NumericEventList(event_data=np.array(event_data))
    trial = Trial(
        start_time=0,
        end_time=20,
        wrt_time=0,
        numeric_events={
            "events": event_list
        }
    )

    enhancer.enhance(trial, 0, {}, {})
    expected_enhancements = {
        "foo": [0.0, 5.0],
        "bar": [2.0],
        "baz": []
    }
    assert trial.enhancements == expected_enhancements

    expected_categories = {
        "time": ["foo", "bar", "baz"]
    }
    assert trial.enhancement_categories == expected_categories


def test_event_times_enhancer_multiple_csvs(tmp_path):
    # Write some .csv files with overlapping / overriding rules in them.
    rules_1_csv = Path(tmp_path, "rules_1.csv")
    with open(rules_1_csv, 'w') as f:
        f.write('type,value,name,comment\n')
        f.write('time,42,foo,this is just a comment\n')
        f.write('time,43,bar,this is just a comment\n')
    rules_2_csv = Path(tmp_path, "rules_2.csv")
    with open(rules_2_csv, 'w') as f:
        f.write('type,value,name,comment\n')
        f.write('time,43,bar,this is just a comment\n')
        f.write('time,44,baz,this is just a comment\n')
    rules_3_csv = Path(tmp_path, "rules_3.csv")
    with open(rules_3_csv, 'w') as f:
        f.write('type,value,name,comment\n')
        f.write('time,42,foo_2,this is just a comment\n')
        f.write('time,44,baz_2,this is just a comment\n')

    enhancer = EventTimesEnhancer(
        buffer_name="events",
        rules_csv=[rules_1_csv, rules_2_csv, rules_3_csv],
        file_finder=FileFinder()
    )

    # Only the "time" rows should be kept.
    # Expect the union of the first two csvs, with partial overrides from the last csv.
    expected_rules = {
        42: {'type': 'time', 'name': 'foo_2'},
        43: {'type': 'time', 'name': 'bar'},
        44: {'type': 'time', 'name': 'baz_2'}
    }
    assert enhancer.rules == expected_rules


def test_expression_enhancer(tmp_path):
    enhancer = ExpressionEnhancer(
        expression="foo + bar > 42",
        value_name="greater",
        value_category="id",
        default_value="No way!"
    )

    greater_trial = Trial(
        start_time=0,
        end_time=20,
        wrt_time=0,
        enhancements={
            "foo": 41,
            "bar": 41
        }
    )
    enhancer.enhance(greater_trial, 0, {}, {})
    assert greater_trial.enhancements == {
        "foo": 41,
        "bar": 41,
        "greater": True
    }

    lesser_trial = Trial(
        start_time=0,
        end_time=20,
        wrt_time=0,
        enhancements={
            "foo": 41,
            "bar": 0
        }
    )
    enhancer.enhance(lesser_trial, 0, {}, {})
    assert lesser_trial.enhancements == {
        "foo": 41,
        "bar": 0,
        "greater": False
    }

    # The expected enchancements "foo" and "bar" are missing, fall back to default value.
    error_trial = Trial(
        start_time=0,
        end_time=20,
        wrt_time=0
    )
    enhancer.enhance(error_trial, 0, {}, {})
    assert error_trial.enhancements == {
        "greater": "No way!"
    }


def test_expression_enhancer_bool_conversion(tmp_path):
    enhancer = ExpressionEnhancer(
        expression="foo > 0",
        value_name="nonzero"
    )

    trial = Trial(
        start_time=0,
        end_time=20,
        wrt_time=0,
        enhancements={
            "foo": np.array(42),
        }
    )

    # The expression "foo > 0" expands to "np.array(42) > 0".
    # This produces a numpy.bool_ rather than a standard Python bool.
    # Check that the enhancer converts the result to standard (ie json-serializable) types only.
    enhancer.enhance(trial, 0, {}, {})
    assert trial.enhancements == {
        "foo": np.array(42),
        "nonzero": True
    }

    nonzero = trial.get_enhancement("nonzero")
    assert type(nonzero) == bool
