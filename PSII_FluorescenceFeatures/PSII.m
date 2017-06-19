function PSII(path_dark, path_light, outputfilename)

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
outputfilename=''

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% load dark adapted PSII data %%%%%%%%%%%%%%%
D=dir(path_dark);
IsItFile=1;
[~,list_folders]=system(sprintf('find %s -type d -name "*.bin*"',path_dark));
if ~isempty(list_folders)
   IsItFile=0;
end
%[~,list_files]=system(sprintf('find %s -type f -name "*.bin"',path));
[~,list_folders]=system(sprintf('find %s -type d -name "*.bin*"',path_light));
if ~isempty(list_folders)
   IsItFile=0;
end

% read all frames to compute mean intensity per frame

for i=1:size(D,1)-1 % frame 101 is metadata
  if ~isempty(findstr(D(i).name,'bin')) 
    % read frames
    A= Read_FileOrFolder(path_dark,i,IsItFile);
    A=double(A)./255;
    % Mean intensity
    M(i)=mean(mean(A));
    % FrameIndex from Filename
    FrameIndex(i)=str2num(D(i).name(end-7:end-4));
  end
end

% Fbase = intensity of first frame (without red flash) as base line to subtract
Fbase_i=find(FrameIndex==1);
F_base= Read_FileOrFolder(path_dark,Fbase_i,IsItFile);
F_base = double(F_base)./255; % convert to double

% chose frame for Fmax as second highest max value to avoid outlier
[M_sort,SortID]=sort(M);
Fm_i=SortID(end-1);


Fm_dark= Read_FileOrFolder(path_dark,Fm_i,IsItFile);
Fm_dark = double(Fm_dark)./255-F_base; % convert to double

Fm_dark_frame = FrameIndex(Fm_i);

% F0
F0_i=find(FrameIndex==2);
F0_dark= Read_FileOrFolder(path_dark,F0_i,IsItFile);
F0_dark = double(F0_dark)./255-F_base; % convert to double

% Compute mask from Fm Frame to exclude background
FmHist=reshape(Fm_dark,1,1936*1216);

% take 99%tile as max intensity as max value
Fsort=sort(FmHist);
Fmax=Fsort(int32(1936*1216*0.99));

% set threshold to 10% of found max value
Fmask_dark=Fm_dark>0.1*Fmax;
%figure(2), hold on, plot([Fsort(int32(1936*1216*0.99)) Fsort(int32(1936*1216*0.99))],[1 50000],'-r')



%%%%%%%% Fv_dark    %%%%%%%
Fv_dark = (Fm_dark - F0_dark).*Fmask_dark;


%%%%%%%% FvFm_dark  %%%%%%%
FvFm_dark = (Fv_dark./Fm_dark).*Fmask_dark;
FvFm_dark(isnan(FvFm_dark))=0;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%





%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% load light adapted PSII data %%%%%%%%%%%%%%%
clear  FrameIndex M

%msgbox("select Filter with light adapted plants");
%[PathName_light] = uigetdir;



D=dir(path_light);
% read all frames to compute mean intensity per frame
for i=1:size(D,1)-1 % frame 101 is metadata

  if ~isempty(findstr(D(i).name,'bin'))
    % read frames

    A = Read_FileOrFolder(path_light,i,IsItFile);
    A=double(A)./255;
    
    % Mean intensity
    M(i)=mean(mean(A));
    % FrameIndex from Filename
    FrameIndex(i)=str2num(D(i).name(end-7:end-4));

  end
end


% Fbase = intensity of first frame (without red flash) as base line to subtract
Fbase_i=find(FrameIndex==1);
F_base = Read_FileOrFolder(path_light,Fbase_i,IsItFile);
F_base = double(F_base)./255; % convert to double



% chose frame for Fmax as second highest max value to avoid outlier
[M_sort,SortID]=sort(M);
Fm_i=SortID(end-1);

% Fm subtracted by F_base

Fm_light = Read_FileOrFolder(path_light,Fm_i,IsItFile);
Fm_light = double(Fm_light)./255-F_base; % convert to double

Fm_light_frame = FrameIndex(Fm_i);

% F0
F0_i=find(FrameIndex==2);
F0_light = Read_FileOrFolder(path_light,F0_i,IsItFile);
F0_light_adapt = double(F0_light)./255-F_base; % convert to double

% Computation of F0_light after Oxborough & Baker 1997: Photosynthesis research, 54: 135-142.
F0_light=F0_dark./((Fv_dark./Fm_dark)+F0_dark./Fm_light);


% under light condition we asume the flurescence at frame 1 (without red flash) as Ft
Ft_light = F0_light-F0_dark;




% Compute mask from Fm Frame to exclude background
FmHist=reshape(Fm_light,1,1936*1216);

% take 99%tile as max intensity as max value
Fsort=sort(FmHist);
Fmax=Fsort(int32(1936*1216*0.99));

% set threshold to 10% of found max value
Fmask_light=Fm_light>0.1*Fmax;
%figure(2), hold on, plot([Fsort(int32(1936*1216*0.99)) Fsort(int32(1936*1216*0.99))],[1 50000],'-r')



%%%%%%%% Fv_light    %%%%%%%
Fv_light = (Fm_light - F0_light_adapt).*Fmask_light;


%%%%%%%% FvFm_light  %%%%%%%
FvFm_light = (Fv_light./Fm_light).*Fmask_light;
FvFm_light(isnan(FvFm_light))=0;
FvFm_light(FvFm_light<0)=0;
 
 
 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%%%%%%%%%%%% Compute values dependend on dark and light measurements %%%%%%%%%%%



%%%%% Phi_PSII %%%%%%
Phi_PSII = (Fm_light-Ft_light)./Fm_light.*Fmask_light;

%%%%% NPQ  %%%%%%
NPQ = (Fm_dark-Fm_light)./Fm_light.*Fmask_light;

%%%%% qN %%%%%%%
qN=(Fm_dark-Fm_light)./(Fm_dark-F0_dark).*Fmask_light;

%%%%% qP %%%%%%%
qP=(Fm_light-Ft_light)./(Fm_dark-F0_dark).*Fmask_light;

%%%%% rfd %%%%%%%
Rfd= (Fm_dark./Fm_light-1).*Fmask_light;

 


out(:,:,1)=Fm_dark;
image_name = [outputfilename '_Fm_dark.jpg'];
imwrite(Fm_dark,image_name,'jpg');
out(:,:,2)=Fv_dark;
image_name = [outputfilename '_Fv_dark.jpg'];
imwrite(Fv_dark,image_name,'jpg');
out(:,:,3)=FvFm_dark;
image_name = [outputfilename '_FvFm_dark.jpg'];
imwrite(FvFm_dark,image_name,'jpg');
out(:,:,4)=Fm_light;
image_name = [outputfilename '_Fm_light.jpg'];
imwrite(Fm_light,image_name,'jpg');
out(:,:,5)=Fv_light;
image_name = [outputfilename '_Fv_light.jpg'];
imwrite(Fv_light,image_name,'jpg');
out(:,:,6)=FvFm_light;
image_name = [outputfilename '_FvFm_light.jpg'];
imwrite(FvFm_light,image_name,'jpg');
out(:,:,7)=Phi_PSII;
image_name = [outputfilename '_Phi_PSII.jpg'];
imwrite(Phi_PSII,image_name,'jpg');
out(:,:,8)=NPQ;
image_name = [outputfilename '_NPQ.jpg'];
imwrite(NPQ,image_name,'jpg');
out(:,:,9)=qN;
image_name = [outputfilename '_qN.jpg'];
imwrite(qN,image_name,'jpg');
out(:,:,10)=qP;
image_name = [outputfilename '_qP.jpg'];
imwrite(qP,image_name,'jpg');
out(:,:,11)=Rfd;
image_name = [outputfilename '_Rfd.jpg'];
imwrite(Rfd,image_name,'jpg');

end




function output= Read_FileOrFolder(path,index,IsItFile)

output=0;

  if IsItFile
    D=dir(path);
    fileID = fopen(fullfile(path,D(index).name));
    A = fread(fileID,[1936,1216],'uint8');
    fclose(fileID);
    output=A;	
  else
    listefolders=dir(strcat(path,'*'));
    for i = 1:length(listefolders) 
      dirName = listefolders(i).name;
      if findstr(dirName,num2str(index-1,'%-5.4d'))
	files = dir( fullfile(path,dirName,'*.bin') );
	files = {files.name};
	fname = fullfile(path,dirName,files{1});
        fileID = fopen(fname);
        A = fread(fileID,[1936,1216],'uint8');
        fclose(fileID);
	output = A;
      end
    end
  end

end
