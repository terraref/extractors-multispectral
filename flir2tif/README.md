# FLIR Converter

This extractor processes binary files into PNG and TIFF files. 

_Input_

  - Evaluation is triggered whenever a file is added to a dataset
  - Checks whether the file is a _ir.bin file
    
_Output_

  - The dataset containing the _ir file will get corresponding .png and .tif files
  - PNG file is a heatmap
  - TIF file consist of geospatial attributes and temperature in degree C in each pixel
  - If "calibrated" is "true" in metadata, this will use FlirRawToTemperature.m in degrees C, otherwise will consider raw data as 100 mk in unit
  - Data are inserted into trait database with the name 'surface_temperature'
  

Plot level summaries are named ['surface_temperature'](http://mmisw.org/ont/cf/parameter/surface_temperature) in the trait database. This name from the Climate Forecast (CF) conventions, and is used instead of 'canopy_temperature' for two reasons. First, because we do not (currently) filter soil in this pipeline. Second, because the CF definition of surface_temperature distinguishes the surface from the medium: "The surface temperature is the temperature at the interface, not the bulk temperature of the medium above or below."   http://cfconventions.org/Data/cf-standard-names/48/build/cf-standard-name-table.html
  
## Temperature Conversion
Written by Andy French.

FLIRgantry2TC_direct.r works by interpolation between cubic polynomials that were fit to the temperature/DN data sets collected in the constant temperature room. You ran the tests at 5,15,25,35,40 and 45C, so there are 6 sets of cubic polynomials. The coefficients are embedded in the R script. The R script reads each DN value one at a time, computes 6 target temperatures corresponding to the 6 ambient room temperatures, then linearly interpolates the target temperature based on the observed ambient temperature. The rest of the routine writes the binary float data plus a header file."

FLIRgantry2TC.r reads the raw FLIR data and associated ambient temp data and reads an associated csv file (copied at same directory) that's a table of temps and estimated spectral radiances for the camera and converts raw DN to temps as a 4-byte float binary output file.

### Dependencies

* All the Python scripts syntactically support Python 2.7 and above. Please make sure that the Python in the running environment is in appropriate version.

* All the Python scripts also rely on the third-party library including: PIL, scipy, numpy, matplotlib and osgeo.

### Notice

* flir_test.sh is an example of using this python script. using '-i' to indicates the input directory that contains of one day's flir data, using '-o' to indicates the output directory
