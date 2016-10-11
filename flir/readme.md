FLIR Converter
=======================

# Dependencies

* All the Python scripts syntactically support Python 2.7 and above. Please make sure that the Python in the running environment is in appropriate version.

* All the Python scripts also rely on the third-party library including: PIL, scipy, numpy, matplotlib and osgeo.

# Usage

* flir_test.sh is an example of using this python script. using '-i' to indicates the input directory that contains of one day's flir data, using '-o' to indicates the output directory

* output png file is a heatmap according to raw data

* output geotiff file is consist of geospatial attributes and temperature in degree C in each pixel. If there is a parameter "calibrated": "true" in the json file, the script use the method that provided in FlirRawToTemperature.m to get a temperature in degree C, otherwise the script consider raw data as 100 mk in unit
