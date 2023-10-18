#!/bin/sh

# This script is intended to generate fixture files for testing the Pyramid trial file reading.
# It grabs copies of the same files used to test Pyramid Python trial file readers.
# It does this by:
#   - running the Pyramid trial file tests vis hatch and pytest
#   - grabbing the temp files generated and used during the tests and copying them here

# Stop on errors.
set -e

# Run the Pyramid tests to generate trial files.
hatch run test:cov -k test_trial_file

# Grab the generated trial files, by magically knowing where the temp files end up.
tmp_dir=/tmp/pytest-of-$(whoami)/pytest-current
cp $tmp_dir/test_hdf5_empty_trial_filecurrent/trial_file.hdf5 ./empty_trials.hdf5
cp $tmp_dir/test_hdf5_sample_trialscurrent/trial_file.hdf5 ./sample_trials.hdf5
cp $tmp_dir/test_json_empty_trial_filecurrent/trial_file.json ./empty_trials.json
cp $tmp_dir/test_json_sample_trialscurrent/trial_file.json ./sample_trials.json

# We can commit these results to the repo in this folder, to support automated Matlab testing.
# We can regenerate and update these files as needed, when the Python trial file code changes.
# Not a seamless process, but simple and documented right here.
