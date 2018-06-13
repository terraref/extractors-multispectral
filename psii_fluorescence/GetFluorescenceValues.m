function [F0,Fm,Fv,Ft,Fmean,Fm_frame]= GetFluorescenceValues(FolderName)
% This Script computes
% F0       - 1936-by-1216 double - Zero fluorescence level for plants just after the excitation light pulse is applied
% Fm       - 1936-by-1216 double - Max. fluorescence level for dark-adapted plants following the staturation pulse typically after 0.5s reached
% Fv       - 1936-by-1216 double - Fm - F0
% Ft       - 1936-by-1216-by-101 uint8 - Fluorescence for each frame
% Fmean    - 1-by-50 matrix     - mean intensity per frame
% Fm_frame -  int8               - frame where Fm is found

% Input argument
% FolderName - string -  raw measurement data from gantry containing 101 frames
% n          - int16  - multiplier to reduce image size
%
% Important note:
% 1st frame represents dark or reference image without light applied Fmin
% in all subsequent frames, Fmin is already subtracted from raw data
% frames are captured at 50ms and red light pulse is active for 1s
%
% in oder to save computation time only first 50 frames with light treatment are considered
%
% Note that frame 101 in file list is an XML file with timeindices for each frame

pkg image load, pkg nan load

D=dir(FolderName);

% make an empty matrix with NaNs
Ft=int8(zeros(1936,1216,50));

j=1;
for i=1:size(D,1)-1

  if ~isempty(findstr(D(i).name,'bin')) & j<=50
    count(j)=i;
    fileID = fopen([FolderName "/" D(i).name]);
    Ft(:,:,j) = int8(fread(fileID,[1936,1216],'uint8')); % double precision
    % second frame is to F0_dark
    if j==2
      F0=double(Ft(:,:,j))./255;
    end

      Ar=reshape(Ft(:,:,j),1,1936*1216);
      % compute mean intesity for Fm calculation
      Fmean(j)=mean(Ar(Ar>0));
      j=j+1;

  end

end

% find frame with max intensity using Fmean computed for each frame2im
% I use the second highest intensity to avoid outliers
[Fsort, idx] = sort (Fmean);
Fm_frame=int8(idx(end-1));
ImageIndex=count(idx(end-1));

% reload image
fileID = fopen([FolderName "/" D(ImageIndex).name]);
Fm = fread(fileID,[1936,1216],'uint8')./255; % double precision

% compute Fv_dark
Fv=Fm-F0;
% Correct for noise where intensity in Fv is higher than Fm
Fv(Fv<0)=0;
