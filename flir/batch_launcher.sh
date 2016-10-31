#!/bin/bash
# this script is called to invoke one instance of flir extractor.

# Load necessary modules
module purge
module load python/2.7.10 pythonlibs/2.7.10 gdal-stack-2.7.10 gdal netcdf

# Activate python virtualenv
source /home/mburnet2/extractors/pyenv/bin/activate

# Run extractor script
python /home/mburnet2/extractors/extractors-multispectral/flir/terra.flir.py
