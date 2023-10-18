The following .plx files were copied from the Plexon "OmniPlex and MAP Offline SDK Bundle" / "Matlab Offline Files SDK".
They were accessed on 2022-11-30 from https://plexon.com/software-downloads/#software-downloads-SDKs.

 - OmniPlex and MAP Offline SDK Bundle/Matlab Offline Files SDK/mexPlex/tests/16sp_lfp_with_2coords.plx
 - OmniPlex and MAP Offline SDK Bundle/Matlab Offline Files SDK/mexPlex/tests/opx141ch1to3analogOnly003.plx
 - OmniPlex and MAP Offline SDK Bundle/Matlab Offline Files SDK/mexPlex/tests/opx141spkOnly004.plx
 - OmniPlex and MAP Offline SDK Bundle/Matlab Offline Files SDK/mexPlex/tests/strobed_negative.plx
 - OmniPlex and MAP Offline SDK Bundle/Matlab Offline Files SDK/mexPlex/tests/ts_freq_zero.plx
 - OmniPlex and MAP Offline SDK Bundle/Matlab Offline Files SDK/mexPlex/tests/waveform_freq_zero.plx

These .plx files are included here with the Pyramid project as fixtures to support automated tests of Pyramid Plexon file reading.

Expected data for three of those files is contained in a .dat file which is part of the same SDK:

 - OmniPlex and MAP Offline SDK Bundle/Matlab Offline Files SDK/mexPlex/tests/mexPlexData1.dat

This .dat file was used to derive three .json files using the following Matlab code:

>> cd('OmniPlex and MAP Offline SDK Bundle/Matlab Offline Files SDK/mexPlex/tests');
>> load('mexPlexData1.dat', '-mat'); % creates "data" variable
>> writelines(jsonencode(data.plxs{1}), 'opx141spkOnly004.json')
>> writelines(jsonencode(data.plxs{2}), '16sp_lfp_with_2coords.json')
>> writelines(jsonencode(data.plxs{3}), 'opx141ch1to3analogOnly003.json')

This produced the following .json files:

 - 16sp_lfp_with_2coords.json
 - opx141ch1to3analogOnly003.json
 - opx141spkOnly004.json

These derived .json files are included here with the Pyramid project as fixtures to support automated tests of Pyramid Plexon file reading.

