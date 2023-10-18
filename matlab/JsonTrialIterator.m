classdef JsonTrialIterator < handle
    % Read a Pyramid JSON trial file, one line/trial at a time.

    properties (SetAccess = private)
        % JSON file handle whose position holds iteration state.
        fid
    end

    methods

        function obj = JsonTrialIterator(trialFile)
            % Set up to read JSON lines as trials.
            arguments
                trialFile {mustBeFile}
            end
            obj.fid = fopen(trialFile, 'r');
        end

        function delete(obj)
            % Let Matlab clean this up when done.
            fclose(obj.fid);
        end

        function trial = next(obj)
            % Read one trial from the next JSON line.
            trialJson = fgetl(obj.fid);
            if ~ischar(trialJson) || isempty(trialJson)
                % Empty result signals end of file.
                trial = [];
                return
            end
            trial = jsondecode(trialJson);
        end
    end
end
