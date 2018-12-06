% This script computes browses through a folder containing snapshots of PS2 data as subfolder
% This script was tested with Octave 4.4.0.
% This script required JSONLab (https://www.mathworks.com/matlabcentral/fileexchange/68159-jsonlab) to be located in the same directory as these scripts. 
%% The JSONLab folder MUST be renamed to 'jsonlab'. 
%% This is done to avoid requiring each user to individually install the package, and instead allow exchange of the entire directory.
%% Tested with version 1.8.
% This script depends on some default Octave packages. The below listed versions were tested:
%% io-2.4.11
%% image-2.6.2

close all



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% load dark adapted PSII data %%%%%%%%%%%%%%%
msgbox("select Folder to analyse");
[PathName] = uigetdir;
D=dir(PathName);


for i=3: size(D,1)
  
 if D(i).isdir

ComputeFvFm_dark([PathName '\' D(i).name])%, ImageTruncateBound_y_upper, ImageTruncateBound_y_lower, ImageTruncateBound_x_upper, ImageTruncateBound_x_lower); 
 
 end
  
  
end