%% Empty trial file
emptyTrialFile = 'fixture_files/empty_trials.hdf5';
trialFile = TrialFile(emptyTrialFile);
assert(isequal(class(trialFile.openIterator()), 'Hdf5TrialIterator'));
assert(isempty(trialFile.read()), 'Empty trial file should produce empty trial struct.');


%% Empty trial file with filter
emptyTrialFile = 'fixture_files/empty_trials.hdf5';
trialFile = TrialFile(emptyTrialFile);
assert(isequal(class(trialFile.openIterator()), 'Hdf5TrialIterator'));
filterFun = @(trial) ~isempty(trial.enhancements);
assert(isempty(trialFile.read(filterFun)), 'Empty trial file should produce empty trial struct with filter.');


%% Sample Trial File
sampleTrialFile = 'fixture_files/sample_trials.hdf5';
trialFile = TrialFile(sampleTrialFile);
assert(isequal(class(trialFile.openIterator()), 'Hdf5TrialIterator'));
expectedTrials = sampleTrials();
assert(isequal(trialFile.read(), expectedTrials), 'Sample trial file should produce expected trials.');

% Repeat read from same trial file instance should also work.
assert(isequal(trialFile.read(), expectedTrials), 'Sample trial file should produce expected trials.');


%% Sample Trial File with filter
sampleTrialFile = 'fixture_files/sample_trials.hdf5';
trialFile = TrialFile(sampleTrialFile);
assert(isequal(class(trialFile.openIterator()), 'Hdf5TrialIterator'));
expectedTrials = sampleTrials();
filterFun = @(trial) ~isempty(trial.enhancements);
assert(isequal(trialFile.read(filterFun), expectedTrials(4:5)), 'Sample trial file should produce expected trials with filter.');
