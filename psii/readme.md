# PSII Converter

This extractor processes binary files into PNG files. 

_Input_

  - Evaluation is triggered whenever a file is added to a dataset
  - Checks whether 0000 ~ 0101 bin files and json file were uploaded
    
_Output_

  - 0000 ~ 0100 png files 
  - a copy of metadata
  - PNG file is a gray scale image


# PSII Analysis processer

This extractor processes png files into Fv/Fm histogram and pseudocolored image

_Input_

  - Evaluation is triggered whenever a file is added to a dataset
  - Checks whether 0000 ~ 0100 png files and json file were uploaded
  
_Output_

  - hist.png is the Fv/Fm histogram
  - imgMap.png is the pseudocolored image
