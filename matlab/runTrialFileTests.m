% This is an entrypoint for running tests of Pyramid trial file reading.
% It adds trial file code to the Matlab path, then uses Matlab's built-in
% test runner to run all the tests here in the "test/" subdir.
%
% This should be handy when developing and testing locally.
% The same tests will also run automatically as part of GitHub actions,
% when code is pushed to GitHub.
function results = runTrialFileTests()

% Add trial file code to the Matlab path, and restore the path when done.
oldPath = path();
cleanup = onCleanup(@()path(oldPath));
trialFileDir = fileparts(mfilename('fullpath'));
addpath(genpath(trialFileDir));

% Run all the tests in the "test/" subdir located next to this script.
testDir = fullfile(trialFileDir, 'test');
results = runtests(testDir);
