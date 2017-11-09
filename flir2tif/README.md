# FLIR GeoTIFF Generator

Generates a Thermal IR Temperature GeoTIFF, with units of Kelvin (K)

## Authors

* Zongyang Li, Donald Danforth Plant Science Center, St. Louis, MO
* Tino Dornbusch, Lemnatec GmbH, Aachen, Germany

## Overview

Converts FLIR raw data into geotiff raster data product of observed temperature in Kelvin.

_Input_

  - FLIR raw data in .bin file
  - reference metadata in .json file
    
_Output_

  - A heat map with a scale of min-max value in the input .bin file, save as a png file
  - A TIFF file consist of geospatial attributes and temperature in Kelvin Degree in each pixel

## Algorithm Description

### Algorithm

GetFlir.py is a re-implementation of the matlab code ([terraref/computing-pipeline/scripts/FLIR/FlirRawToTemperature.m](https://github.com/terraref/computing-pipeline/blob/3b8b3e1c76e51b768fca9d99f99ced33e9fc78d1/scripts/FLIR/FlirRawToTemperature.m)) that has been converted into python.

1. Atmospheric transmission - correction factor from air temperature, relative humidity and distance of sensor to object.

2. Correct raw pixel values from external factors,

  1. General equation : Total Radiation = Object Radiation + Atmosphere Radiation + Ambient Reflection Radiation

  2. Object Radiation = Theoretical object radiation * emissivity * atmospheric transmission

  3. Atmosphere Radiation= (1 - atmospheric transmission) * Theoretical atmospheric radiation

  4. Ambient Reflection Radiation = (1 - emissivity) * atmospheric transmission * Theoretical Ambient Reflection Radiation

3. RBF equation: transformation of pixel intensity in radiometric temperature from raw values or corrected values

4.  Algorithm parameters:

| Parameter | Name | Description |
|:---|:---|:----|
|R | Planck constant	|	function of integration time and wavelength |
|B |Planck constant	|	function of wavelength |
|F|Planck constant | positive value (0 - 1) |
|J0 | global offset | |
|J1 | global gain | |
|X | | Constant Atmospheric transmission parameters from FLIR |
|a1| | |
|b1| | | 
|a2| | | 
| b2 | |  |
|H2O_K1| | Constants for computing VPD (sqtrH2O)  |
|H2O_K2| | |
|H2O_K3| | |
|H2O_K4 | | |

5. Environmental drivers:

| Parameter | Name | Units | Description |
|:---|:---|:----|:----|
| H | Relative Humidity | fraction (0 - 1) | From Gantry Met Station | 
| T | Air temperature | $\degree$Celsius | From Gantry Met Station |
| D | Object Distance | m | distance from camera to canopy |
| E | object emissivity | ratio | assume vegetation = 0.98, bare soil = 0.93 |

6. Empirical Magic Numbers

  * HEIGHT_MAGIC_NUMBER: To obtain a well stitched map, a HEIGHT_MAGIC_NUMBER was applied when we estimate the field of view, this is a testing based number.

  *  The geo-reference bounding box based on an assumption of image axis is in a same direction of lat-lon axis.


### Failure Conditions

1. If the sensor is moved within the sensor box, the HEIGHT_MAGIC_NUMBER might need to change in order to correctly stitch the images.
2. Because of a failure of weatherproof system, data from 05-01-2017 to 10-01-2017 can not be used. 
   * discovery of the issue is documented here: https://github.com/terraref/reference-data/issues/182
   * attempts to recover these data are documented in https://github.com/terraref/reference-data/issues/190

These figures show the contamination and how this affected imaging during the summer 2017 (MAC Field Scanner Season 4):

![Contaminated FLIR Camera](figures/FLIR_failure_camera.jpg)

![Contaminated Image 1](figures/flir_issues.png)

![Contaminated Image 2](figures/flir_issues2.png)


## Implementation

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

### Example

`flir_test.sh` is an example of using this python script. 

The '-i' flag indicates the input directory that contains of one day's flir data, using '-o' to indicates the output directory.

```sh
cd sites/ua-mac/raw_data/flirIrCamera/2017-11-09/2017-11-09__15-08-28-574/
./flir_test.sh -i 0f9e8e55-28db-480f-80cf-6e188a1438ea_ir.bin -o /tmp/
```