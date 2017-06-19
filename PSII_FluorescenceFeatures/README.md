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
