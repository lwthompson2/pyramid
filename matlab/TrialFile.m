classdef TrialFile
    % Read Pyramid trials from HDF5 or JSON into a Matlab struct array.

    properties (SetAccess = private)
        % The Pyramid trial file to read.
        trialFile

        % Standard list of fields for all trials to share.
        trialFields = { ...
            'start_time', ...
            'end_time', ...
            'wrt_time', ...
            'numeric_events', ...
            'signals', ...
            'enhancements', ...
            'enhancement_categories'}
    end

    methods

        function obj = TrialFile(trialFile)
            % Set up to read Pyramid trials from HDF5 or JSON trialFile.
            arguments
                trialFile {mustBeFile}
            end
            obj.trialFile = trialFile;
        end

        function iterator = openIterator(obj)
            % Use trialFile extension to pick a format.
            [~, ~, extension] = fileparts(obj.trialFile);
            switch extension
                case {".hdf", ".h5", ".hdf5", ".he5"}
                    iterator = Hdf5TrialIterator(obj.trialFile);
                case {".json", ".jsonl"}
                    iterator = JsonTrialIterator(obj.trialFile);
                otherwise
                    error("Unknown format %s for trial file %s.", extension, obj.trialFile);
            end
        end

        function trials = read(obj, filterFun)
            % Read trials into a Matlab struct array, one at a time.
            %
            % By default, this will read all trials from the trial file
            % into memory and return them as a struct array.
            %
            % You can provide a filterFun to decide which trials to keep or
            % ignore. The filterFun should expect one argument: the current
            % trial being read as a scalar struct.  When filterFun returns
            % true the trial will be kept, otherwise it will be ignored.
            arguments
                obj TrialFile
                filterFun function_handle = @(trial) true
            end

            % Append trials one at a time to a list.
            iterator = obj.openIterator();
            trialsSoFar = {};
            wildTrial = iterator.next();
            while ~isempty(wildTrial)
                trial = obj.standardize(wildTrial);
                if filterFun(trial)
                    trialsSoFar{end + 1} = trial; %#ok<AGROW>
                end

                wildTrial = iterator.next();
            end

            % Reshape the list as a uniform struct array.
            trials = [trialsSoFar{:}];
        end

        function trial = standardize(obj, wildTrial)
            % Format a wild trial to have all and only the expected fields.
            fieldValues = cell(size(obj.trialFields));
            for ii = 1:numel(obj.trialFields)
                fieldName = obj.trialFields{ii};
                if isfield(wildTrial, fieldName)
                    fieldValues{ii} = wildTrial.(fieldName);
                end
            end
            trial = cell2struct(fieldValues, obj.trialFields, 2);
        end
    end
end
