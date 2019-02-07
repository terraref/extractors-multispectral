function nargout =  ComputeFvFm_dark(PathName_dark)%, ImageTruncateBound_y_upper, ImageTruncateBound_y_lower, ImageTruncateBound_x_upper, ImageTruncateBound_x_lower)

% This script computes all characterisitc fluorescence features for
% dark and light-adapted plants

% Variables used here
% F0_dark        - 1936-by-1216          double - F0 Zero fluorescence level for plants just after the excitation light pulse is applied
% Fm_dark        - 1936-by-1216          double - Fm Max. fluorescence level for dark-adapted plants following the staturation pulse typically after 0.5s reached
% Fv_dark        - 1936-by-1216          double - Fv Fm_dark - F0_dark 
% Fmask_dark     - 1936-by-1216          bool  - Mask to exclude background
% Fm_dark_frame  -                       int8   - frame where Fm_dark is found 

% F0_light       - 1936-by-1216         double - F0' Zero fluorescence level for plants after returning to dark state
% F0_light_adapt - 1936-by-1216         double - F0' Zero fluorescence level for plants just after the excitation light pulse is applied
% Fm_light       - 1936-by-1216         double - Fm' Max. fluorescence level for dark-adapted plants following the staturation pulse typically after 0.5s reached
% Fv_light       - 1936-by-1216         double - Fv' Fm_dark - F0_dark 
% Ft_light       - 1936-by-1216         double - Ft  steady-state flourescence in the light 
% Fmask_dark     - 1936-by-1216         bool   - Mask to exclude background
% Fm_light_frame -                      int8   - frame where Fm_dark is found 

% computed values
% FvFm_dark      -  1936-by-1216        double  Fv_dark/Fm_dark The maximal photochemical effiency of PSII
% FvFm_light     -  1936-by-1216        double  Fv_dark/Fm_dark The maximal photochemical effiency of PSII
% Phi_PSII       -  1936-by-1216        double  Quantum yield of photosynthesis
% NPQ            -  1936-by-1216        double  Non-photochemical quenching, absorbed light energy that is dissipated (mostly by thermal radiation)
% qN             -  1936-by-1216        double  Proportion of closed PSII reaction centers
% qP             -  1936-by-1216        double  Proportion of open PSII reaction centers
% Rfd            -  1936-by-1216        double  ratio of chlorophyll decrease to steady state Chlorophyll

pkg image load
pkg io load
addpath('./jsonlab')
close all

ImageTruncateBound_y_upper = 1936;%upper limit
ImageTruncateBound_y_lower = 1;%must be 1
ImageTruncateBound_x_upper = 1150;%upper limit
ImageTruncateBound_x_lower = 301;%lower limit + 1 

if nargin==0


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% load dark adapted PSII data %%%%%%%%%%%%%%%
msgbox("select Filter with Darkadapted plants");
[PathName_dark] = uigetdir;
end


D=dir(PathName_dark);

% read all frames to compute mean intensity per frame
for i=1:size(D,1)-1 % frame 101 is metadata

  if ~isempty(findstr(D(i).name,'bin'))
    % read frames
    fileID = fopen([PathName_dark '\' D(i).name]);
    % FrameIndex from Filename
    FrameIndex(i)=str2num(D(i).name(end-7:end-4));
      if FrameIndex(i)<=100
         A = fread(fileID,[1936,1216],'uint8');
         A = double(A)./255;
         A_truncate = A(ImageTruncateBound_y_lower:ImageTruncateBound_y_upper , ImageTruncateBound_x_lower:ImageTruncateBound_x_upper);
         fclose(fileID);
         % Mean intensity
         M(i)=mean(mean(A_truncate));
      end 
    
  elseif ~isempty(findstr(D(i).name,'data.json'))
  JsonName=[PathName_dark '\' D(i).name];
  FrameIndex(i)=NaN;
  else
  FrameIndex(i)=NaN;
  end
end



%Exclude the FrameIndex 101 bin file - contains some metadata
FrameIndex(FrameIndex ==101)=NaN;

% Fbase = intensity of first frame (without red flash) as base line to subtract
% Note that Baseline correction is already done in the camera before image saving
Fbase_i=find(FrameIndex==0);
fileID = fopen([PathName_dark '\' D(Fbase_i).name]);
F_base = fread(fileID,[1936,1216],'uint8');
fclose(fileID);
F_base = double(F_base)./255; % convert to double
F_base_truncate = F_base(ImageTruncateBound_y_lower:ImageTruncateBound_y_upper,ImageTruncateBound_x_lower:ImageTruncateBound_x_upper);

% chose frame for Fmax as second highest max value to avoid outlier
[M_sort,SortID]=sort(M);
Fm_i=SortID(end-1);

fileID = fopen([PathName_dark '\' D(Fm_i).name]);
% Fm subtracted by F_base
Fm_dark = fread(fileID,[1936,1216],'uint8');
fclose(fileID);
Fm_dark = double(Fm_dark)./255; % convert to double
Fm_dark_truncate = Fm_dark(ImageTruncateBound_y_lower:ImageTruncateBound_y_upper,ImageTruncateBound_x_lower:ImageTruncateBound_x_upper);

Fm_dark_frame = FrameIndex(Fm_i);

% F0
F0_i=find(FrameIndex==1);
fileID = fopen([PathName_dark '\' D(F0_i).name]);
F0_dark = fread(fileID,[1936,1216],'uint8');
fclose(fileID);
F0_dark = double(F0_dark)./255; % convert to double
F0_dark_truncate = F0_dark(ImageTruncateBound_y_lower:ImageTruncateBound_y_upper,ImageTruncateBound_x_lower:ImageTruncateBound_x_upper);

% Compute mask from Fm Frame to exclude background
%FmHist=reshape(Fm_dark,1,1936*1216);
FmHist=reshape(Fm_dark_truncate,1,(size(Fm_dark_truncate)(1)*size(Fm_dark_truncate)(2)));

% take 99%tile as max intensity as max value
Fsort=sort(FmHist);
%Fmax=Fsort(int32(1936*1216*0.99));
Fmax=Fsort(int32((size(Fm_dark_truncate)(1)*size(Fm_dark_truncate)(2))*0.99));

% set threshold to 15% of found max value
Fmask_dark=Fm_dark_truncate>0.15*Fmax;
%figure(2), hold on, plot([Fsort(int32(1936*1216*0.99)) Fsort(int32(1936*1216*0.99))],[1 50000],'-r')

% fill small areas
se = strel ("square", 3);
% remove background
Fmask_dark=imerode (Fmask_dark, se);
% fill holes
%Fmask_dark=imdilate(Fmask_dark, se);





%%%%%%%% Fv_dark    %%%%%%%
Fv_dark = (Fm_dark_truncate - F0_dark_truncate).*Fmask_dark;


%%%%%%%% FvFm_dark  %%%%%%%
FvFm_dark = (Fv_dark./Fm_dark_truncate).*Fmask_dark;
FvFm_dark(isnan(FvFm_dark))=0;

figure(1), imagesc(FvFm_dark),colorbar,caxis([0 1]), axis('off')
saveas (gcf, [PathName_dark "\\\Fig_ResultImage.png"]);
pause(2)

% Reshape the image matrix FvFm to arrayfun
%FvFm_hist=reshape(FvFm_dark,1,1936*1216);
FvFm_hist=reshape(FvFm_dark,1,(size(Fm_dark_truncate)(1)*size(Fm_dark_truncate)(2)));
FvFm_hist(FvFm_hist==0)=NaN;

figure(2),hist(FvFm_hist,50);
xlabel("FvFm"), ylabel("n")
saveas (gcf, [PathName_dark "\\\Fig_Histogram.png"]);
pause(2)


iFrame=~isnan(FrameIndex);
figure(3),plot(FrameIndex(iFrame),M(iFrame));
xlabel("Frameindex"), ylabel("mean intensity")
saveas (gcf, [PathName_dark "\\\Fig_MeanIntensity.png"]);
pause(2)
% build up csv file

OutputCSV{1,1} = "timestamp";

% find date and time Parse date to match with json file
i=findstr(PathName_dark,'\');

Date=    [PathName_dark(i(end)+6:i(end)+7) "/" PathName_dark(i(end)+9:i(end)+10) "/" PathName_dark(i(end)+1:i(end)+4)];
Time=    [PathName_dark(i(end)+13:i(end)+14) ":" PathName_dark(i(end)+16:i(end)+17) ":" PathName_dark(i(end)+19:i(end)+20)];

OutputCSV{2,1}= [Date " " Time];

OutputCSV{1,2} = "mean";
OutputCSV{2,2} = mean(FvFm_hist(~isnan(FvFm_hist)));

OutputCSV{1,3} = "standard deviation";
OutputCSV{2,3} = std(FvFm_hist(~isnan(FvFm_hist)));


cell2csv([PathName_dark "\\\Result.csv"],OutputCSV);
pause(2)


% load Json file to directly write result data
 ResultJson=loadjson(JsonName);
 
 ResultJson.result.mean=mean(FvFm_hist(~isnan(FvFm_hist)));
ResultJson.result.std=std(FvFm_hist(~isnan(FvFm_hist)));

JsonSaveName=[JsonName(1:end-5) "_Result" JsonName(end-4:end)];
savejson('ResultJson',ResultJson,JsonSaveName);
pause(2)

% close all figures
close all


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

endfunction