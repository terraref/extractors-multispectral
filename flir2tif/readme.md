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
  -e RABBITMQ_URI=amqp://{RMQ_USER}:{RMQ_PASSWORD}@rabbitmq.ncsa.illinois.edu/clowder \
  -e RABBITMQ_EXCHANGE=terra \
  -e REGISTRATION_ENDPOINTS=http://terraref.ncsa.illinosi.edu/clowder//api/extractors?key={SECRET_KEY} \
  terra-ext-flir2tif
```

### Dependencies

* All the Python scripts syntactically support Python 2.7 and above. Please make sure that the Python in the running environment is in appropriate version.

* All the Python scripts also rely on the third-party library including: PIL, scipy, numpy, matplotlib and osgeo.

### Notice

* flir_test.sh is an example of using this python script. using '-i' to indicates the input directory that contains of one day's flir data, using '-o' to indicates the output directory
