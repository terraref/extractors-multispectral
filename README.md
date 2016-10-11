# Multispectral extractors

This repository contains extractors that process data originating from:
- FLIR SC 615 Thermal Infrared sensor
- Skye PRI sensor
- Skye NDVI sensors
- PSII Fluorescence sensor
- Crop Circle ACS430P Active Reflectance sensor


### FLIR extractor
This extractor processes binary files into PNG and TIFF files. 

_Input_

  - Evaluation is triggered whenever a file is added to a dataset
  - Checks whether the file is a _ir.bin file
    
_Output_

  - The dataset containing the _ir file will get corresponding .png and .tif files
  - PNG file is a heatmap
  - TIF file consist of geospatial attributes and temperature in degree C in each pixel
  