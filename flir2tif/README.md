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
  
### Docker
The Dockerfile included in this directory can be used to launch this extractor in a container.

_Building the Docker image_
```
docker build -f Dockerfile -t terra-ext-flir2tif .
```

_Running the image locally_
```
docker run \
  -p 5672 -p 9000 --add-host="localhost:{LOCAL_IP}" \
  -v /local/raw_data/folder:/home/extractor/sites/ua-mac/raw_data \
  -v /local/output/root/folder:/home/extractor/sites/ua-mac/Level_1/flir2tif \
  -e RABBITMQ_URI=amqp://{RMQ_USER}:{RMQ_PASSWORD}@localhost:5672/%2f \
  -e RABBITMQ_EXCHANGE=clowder \
  -e REGISTRATION_ENDPOINTS=http://localhost:9000/clowder/api/extractors?key={SECRET_KEY} \
  -e INFLUXDB_PASSWORD={INFLUX_PASS} \
  terra-ext-flir2tif
```
Note that by default RabbitMQ will not allow "guest:guest" access to non-local addresses, which includes Docker. You may need to create an additional local RabbitMQ user for testing.

_Running the image remotely_
```
docker run \
  -v /sites/ua-mac/raw_data:/home/extractor/sites/ua-mac/raw_data \
  -v /sites/ua-mac/Level_1/flir2tif:/home/extractor/sites/ua-mac/Level_1/flir2tif \
  -e RABBITMQ_URI=amqp://{RMQ_USER}:{RMQ_PASSWORD}@rabbitmq.ncsa.illinois.edu/clowder \
  -e RABBITMQ_EXCHANGE=terra \
  -e REGISTRATION_ENDPOINTS=http://terraref.ncsa.illinosi.edu/clowder//api/extractors?key={SECRET_KEY} \
  -e INFLUXDB_PASSWORD={INFLUX_PASS} \
  terra-ext-flir2tif
```

### Dependencies

* All the Python scripts syntactically support Python 2.7 and above. Please make sure that the Python in the running environment is in appropriate version.

* All the Python scripts also rely on the third-party library including: PIL, scipy, numpy, matplotlib and osgeo.

### Notice

* flir_test.sh is an example of using this python script. using '-i' to indicates the input directory that contains of one day's flir data, using '-o' to indicates the output directory
