'''
Created on Aug 23, 2016

@author: Zongyang Li
'''

import os, json, sys, math, argparse, utm, shutil
import matplotlib.pyplot as plt
from matplotlib import cm
from glob import glob
import numpy as np
from PIL import Image
from math import cos, pi
from osgeo import gdal, osr
from numpy.matlib import repmat
from datetime import date
import multiprocessing

ZERO_ZERO = (33.0745,-111.97475)

mode_date = date(2016, 9, 15)

# Scanalyzer -> MAC formular @ https://terraref.gitbooks.io/terraref-documentation/content/user/geospatial-information.html
# Mx = ax + bx * Gx + cx * Gy
# My = ay + by * Gx + cy * Gy
SE_latlon = (33.07451869,-111.97477775)
ay = 3659974.971; by = 1.0002; cy = 0.0078;
ax = 409012.2032; bx = 0.009; cx = - 0.9986;
lon_shift = 0.000020308287
lat_shift = 0.000015258894
SE_utm = utm.from_latlon(SE_latlon[0], SE_latlon[1])

TILE_FOLDER_NAME = 'tif_list'

class calibParam:
    def __init__(self):
        self.calibrated = False
        self.calibrationR = 0.0
        self.calibrationB = 0.0
        self.calibrationF = 0.0
        self.calibrationJ1 = 0.0
        self.calibrationJ0 = 0.0
        self.calibrationa1 = 0.0
        self.calibrationa2 = 0.0
        self.calibrationX = 0.0
        self.calibrationb1 = 0.0
        self.calibrationb2 = 0.0


def options():
    
    parser = argparse.ArgumentParser(description='Convert FLIR raw data into pngs and temperature in geotiff',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
    parser.add_argument("-i", "--in_dir", help="input directory, that contains of one day's flir data", required = True)
    parser.add_argument("-o", "--out_dir", help="output directory", required = True)
    
    
    args = parser.parse_args()
    if not os.path.exists(args.in_dir):
        raise IOError("Path does not exist: {0}".format(args.in_dir))
    
    if not os.path.exists(args.out_dir):
        try:
            os.makedirs(args.out_dir)
        except Exception as ex:
            fail('Could not create a directory for output: ' + str(ex))

    return args

def main():
    
    args = options()
    
    # If there is a pre-existing tiles folder with this name, delete it (failing to do so can result in some weirdness when you load tiles later)
    if os.path.exists(args.out_dir):
        shutil.rmtree(args.out_dir)
    
    os.makedirs(args.out_dir)
    
    print "Starting binary to image conversion..."
    full_day_convert(args.in_dir, args.out_dir)
    print "Completed binary to image conversion..."
    
    createVrt(args.out_dir, os.path.join(args.out_dir, 'tif_list.txt'))
    
    # Generate tiles from VRT
    print "Starting map tile creation..."
    createMapTiles(args.out_dir,multiprocessing.cpu_count())
    print "Completed map tile creation..."
    
    generate_googlemaps(args.out_dir)
    
    return

def full_day_convert(in_dir, out_dir):
    
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
        
    tif_list_file = os.path.join(out_dir, 'tif_list.txt')
    # Create a file to write the paths for all of the TIFFs. This will be used create the VRT.
    try:
        os.remove(tif_list_file) # start from a fresh list of TIFFs for the day
    except OSError:
        pass
    
    list_dirs = os.walk(in_dir)
    
    for root, dirs, files in list_dirs:
        for d in dirs:
            input_path = os.path.join(in_dir, d)
            output_path = os.path.join(out_dir, d)
            if not os.path.isdir(input_path):
                continue
            
            try:
                get_flir(input_path, output_path, tif_list_file)
            except Exception as ex:
                fail('Error processing flir data in: ' + input_path + str(ex))
    
    return


def get_flir(in_dir, out_dir, tif_list_file):
    
    if not os.path.exists(out_dir):
        try:
            os.mkdir(out_dir)
        except:
            fail('Failed to create directory in ' + out_dir)
    
    metafile, binfile = find_files(in_dir)
    if metafile == [] or binfile == [] :
        return
    
    metadata = lower_keys(load_json(metafile)) # load json file
    
    center_position, scan_time, fov = parse_metadata(metadata) # get information from json file
    
    fix_fov = get_new_fov(center_position[2], fov)
    
    gps_bounds = get_bounding_box_with_formula(center_position, fix_fov) # get bounding box using gantry position and fov of camera
    
    raw_data = load_flir_data(binfile) # get raw data from bin file
    
    filename = os.path.basename(binfile)
    temp_name = os.path.join(out_dir, filename)
    out_png = temp_name[:-3] + 'png'
    
    gmin = 13000
    gmax = 18000
    im_color = flir_data_visualization(raw_data, out_png, gmin, gmax) # create png
    
    tc = rawData_to_temperature(raw_data, scan_time, metadata) # get temperature
    
    tif_path = temp_name[:-3] + 'tif'
    
    create_geotiff_with_temperature(im_color, tc, gps_bounds, tif_path) # create geotiff
    
    # once we've saved the image, make sure to append this path to our list of TIFs
    f = open(tif_list_file,'a+')
    f.write(tif_path + '\n')
    
    return


def rawData_to_temperature(rawData, metadata):
    try:
        calibP = get_calibrate_param(metadata)
        tc = np.zeros((640, 480))
        
        if not calibP.calibrated:
            tc = rawData/10 - 273.15
        else:
            tc = flirRawToTemperature(rawData, calibP)

        return tc
    except Exception as ex:
        fail('raw to temperature fail:' + str(ex))
        
def get_calibrate_param(metadata):
    calibparameter = calibParam()

    try:
        if 'lemnatec_measurement_metadata' in metadata:
            sensor_fixed_meta = metadata['lemnatec_measurement_metadata']['sensor_fixed_metadata']
            calibrated = sensor_fixed_meta['calibrated']

            if calibrated == 'false':
                return calibparameter
            if calibrated == 'true':
                calibparameter.calibrated = True
                calibparameter.calibrationR = float(sensor_fixed_meta['calibration r'])
                calibparameter.calibrationB = float(sensor_fixed_meta['calibration b'])
                calibparameter.calibrationF = float(sensor_fixed_meta['calibration f'])
                calibparameter.calibrationJ1 = float(sensor_fixed_meta['calibration j1'])
                calibparameter.calibrationJ0 = float(sensor_fixed_meta['calibration j0'])
                calibparameter.calibrationa1 = float(sensor_fixed_meta['calibration alpha1'])
                calibparameter.calibrationa2 = float(sensor_fixed_meta['calibration alpha2'])
                calibparameter.calibrationX = float(sensor_fixed_meta['calibration x'])
                calibparameter.calibrationb1 = float(sensor_fixed_meta['calibration beta1'])
                calibparameter.calibrationb2 = float(sensor_fixed_meta['calibration beta2'])
                return calibparameter

        elif 'sensor_fixed_metadata' in metadata:
            fixedmd = metadata['sensor_fixed_metadata']
            if fixedmd['is_calibrated'] == 'False':
                return calibparameter
            else:
                calibparameter.calibrated = True
                calibparameter.calibrationR = float(fixedmd['calibration_R'])
                calibparameter.calibrationB = float(fixedmd['calibration_B'])
                calibparameter.calibrationF = float(fixedmd['calibration_F'])
                calibparameter.calibrationJ1 = float(fixedmd['calibration_J1'])
                calibparameter.calibrationJ0 = float(fixedmd['calibration_J0'])
                calibparameter.calibrationa1 = float(fixedmd['calibration_alpha1'])
                calibparameter.calibrationa2 = float(fixedmd['calibration_alpha2'])
                calibparameter.calibrationX = float(fixedmd['calibration_X'])
                calibparameter.calibrationb1 = float(fixedmd['calibration_beta1'])
                calibparameter.calibrationb2 = float(fixedmd['calibration_beta2'])
                return calibparameter

    except KeyError as err:
        return calibparameter
    

def create_geotiff_with_temperature(np_arr, temp_arr, gps_bounds, out_file_path):
    try:
        nrows, ncols, channels = np.shape(np_arr)
        xres = (gps_bounds[3] - gps_bounds[2])/float(ncols)
        yres = (gps_bounds[1] - gps_bounds[0])/float(nrows)
        geotransform = (gps_bounds[2],xres,0,gps_bounds[1],0,-yres) #(top left x, w-e pixel resolution, rotation (0 if North is up), top left y, rotation (0 if North is up), n-s pixel resolution)

        output_raster = gdal.GetDriverByName('GTiff').Create(out_file_path, ncols, nrows, 1, gdal.GDT_Float32)

        output_raster.SetGeoTransform(geotransform) # specify coordinates
        srs = osr.SpatialReference() # establish coordinate encoding
        srs.ImportFromEPSG(4326) # specifically, google mercator
        output_raster.SetProjection( srs.ExportToWkt() ) # export coordinate system to file

        '''
        # TODO: Something wonky w/ uint8s --> ending up w/ lots of gaps in data (white pixels)
        output_raster.GetRasterBand(1).WriteArray(np_arr[:,:,0].astype('uint8')) # write red channel to raster file
        output_raster.GetRasterBand(1).FlushCache()
        output_raster.GetRasterBand(1).SetNoDataValue(-99)
        
        output_raster.GetRasterBand(2).WriteArray(np_arr[:,:,1].astype('uint8')) # write green channel to raster file
        output_raster.GetRasterBand(2).FlushCache()
        output_raster.GetRasterBand(2).SetNoDataValue(-99)

        output_raster.GetRasterBand(3).WriteArray(np_arr[:,:,2].astype('uint8')) # write blue channel to raster file
        output_raster.GetRasterBand(3).FlushCache()
        output_raster.GetRasterBand(3).SetNoDataValue(-99)
        '''
        
        output_raster.GetRasterBand(1).WriteArray(temp_arr) # write temperature information to raster file
        output_raster = None

    except Exception as ex:
        fail('Error creating GeoTIFF: ' + str(ex))
    
    
    return

def create_geotiff(np_arr, gps_bounds, out_file_path):
    try:
        nrows,ncols = np.shape(np_arr)
        # gps_bounds: (lat_min, lat_max, lng_min, lng_max)
        xres = (gps_bounds[3] - gps_bounds[2])/float(ncols)
        yres = (gps_bounds[1] - gps_bounds[0])/float(nrows)
        geotransform = (gps_bounds[2],xres,0,gps_bounds[1],0,-yres) #(top left x, w-e pixel resolution, rotation (0 if North is up), top left y, rotation (0 if North is up), n-s pixel resolution)

        output_path = out_file_path

        output_raster = gdal.GetDriverByName('GTiff').Create(output_path, ncols, nrows, 3, gdal.GDT_Byte)

        output_raster.SetGeoTransform(geotransform) # specify coordinates
        srs = osr.SpatialReference() # establish coordinate encoding
        srs.ImportFromEPSG(4326) # specifically, google mercator
        output_raster.SetProjection( srs.ExportToWkt() ) # export coordinate system to file
        
        # TODO: Something wonky w/ uint8s --> ending up w/ lots of gaps in data (white pixels)
        output_raster.GetRasterBand(1).WriteArray(np_arr.astype('uint8')) # write red channel to raster file
        output_raster.GetRasterBand(1).FlushCache()
        output_raster.GetRasterBand(1).SetNoDataValue(-99)
        
        output_raster.GetRasterBand(2).WriteArray(np_arr.astype('uint8')) # write green channel to raster file
        output_raster.GetRasterBand(2).FlushCache()
        output_raster.GetRasterBand(2).SetNoDataValue(-99)

        output_raster.GetRasterBand(3).WriteArray(np_arr.astype('uint8')) # write blue channel to raster file
        output_raster.GetRasterBand(3).FlushCache()
        output_raster.GetRasterBand(3).SetNoDataValue(-99)

    except Exception as ex:
        fail('Error creating GeoTIFF: ' + str(ex))

def load_flir_data(file_path):
    
    try:
        im = np.fromfile(file_path, np.dtype('<u2')).reshape([480, 640])
        im = im.astype('float')
        im = np.rot90(im, 3)   # rotate 90 degree to fit camera position
        return im
    except Exception as ex:
        fail('Error loading bin file' + str(ex))
        
def flir_data_visualization(im, outfile_path, Gmin, Gmax):
    
    #Gmin = im.min()
    #Gmax = im.max()
    At = (im-Gmin)/(Gmax - Gmin)
    
    my_cmap = cm.get_cmap('jet')
    color_array = my_cmap(At)
    
    plt.imsave(outfile_path, color_array)
    
    img_data = Image.open(outfile_path)
    
    return np.array(img_data)
        
# convert flir raw data into temperature C degree, for date after September 15th
def flirRawToTemperature(rawData, calibP):
    
    R = calibP.calibrationR
    B = calibP.calibrationB
    F = calibP.calibrationF
    J0 = calibP.calibrationJ0
    J1 = calibP.calibrationJ1
    
    X = calibP.calibrationX
    a1 = calibP.calibrationa1
    b1 = calibP.calibrationb1
    a2 = calibP.calibrationa2
    b2 = calibP.calibrationb2
    
    H2O_K1 = 1.56
    H2O_K2 = 0.0694
    H2O_K3 = -0.000278
    H2O_K4 = 0.000000685
    
    H = 0.1
    T = 22.0
    D = 2.5
    E = 0.98
    
    K0 = 273.15
    
    im = rawData
        
    AmbTemp = T + K0
    AtmTemp = T + K0
        
    H2OInGperM2 = H*math.exp(H2O_K1 + H2O_K2*T + H2O_K3*math.pow(T, 2) + H2O_K4*math.pow(T, 3))
    a1b1sqH2O = (a1+b1*math.sqrt(H2OInGperM2))
    a2b2sqH2O = (a2+b2*math.sqrt(H2OInGperM2))
    exp1 = math.exp(-math.sqrt(D/2)*a1b1sqH2O)
    exp2 = math.exp(-math.sqrt(D/2)*a2b2sqH2O)
        
    tao = X*exp1 + (1-X)*exp2
        
    obj_rad = im*E*tao
        
    theo_atm_rad = (R*J1/(math.exp(B/AtmTemp)-F)) + J0
    atm_rad = repmat((1-tao)*theo_atm_rad, 480, 640)
        
    theo_amb_refl_rad = (R*J1/(math.exp(B/AmbTemp)-F)) + J0
    amb_refl_rad = repmat((1-E)*tao*theo_amb_refl_rad, 480, 640)
        
    corr_pxl_val = obj_rad + atm_rad + amb_refl_rad
        
    pxl_temp = B/np.log(R/(corr_pxl_val-J0)*J1+F) - K0
    
    return pxl_temp

def get_bounding_box(center_position, fov):
    # NOTE: ZERO_ZERO is the southeast corner of the field. Position values increase to the northwest (so +y-position = +latitude, or more north and +x-position = -longitude, or more west)
    # We are also simplifying the conversion of meters to decimal degrees since we're not close to the poles and working with small distances.

    # NOTE: x --> latitude; y --> longitude
    try:
        r = 6378137 # earth's radius

        x_min = center_position[1] - fov[1]/2
        x_max = center_position[1] + fov[1]/2
        y_min = center_position[0] - fov[0]/2
        y_max = center_position[0] + fov[0]/2

        lat_min_offset = y_min/r* 180/pi
        lat_max_offset = y_max/r * 180/pi
        lng_min_offset = x_min/(r * cos(pi * ZERO_ZERO[0]/180)) * 180/pi
        lng_max_offset = x_max/(r * cos(pi * ZERO_ZERO[0]/180)) * 180/pi

        lat_min = ZERO_ZERO[0] - lat_min_offset
        lat_max = ZERO_ZERO[0] - lat_max_offset
        lng_min = ZERO_ZERO[1] - lng_min_offset
        lng_max = ZERO_ZERO[1] - lng_max_offset
    except Exception as ex:
        fail('Failed to get GPS bounds from center + FOV: ' + str(ex))
    return (lat_max, lat_min, lng_max, lng_min)

# Scanalyzer -> MAC formular @ https://terraref.gitbooks.io/terraref-documentation/content/user/geospatial-information.html
# Mx = ax + bx * Gx + cx * Gy
# My = ay + by * Gx + cy * Gy
def get_bounding_box_with_formula(center_position, fov):
    
    y_w = center_position[1] + fov[1]/2
    y_e = center_position[1] - fov[1]/2
    x_n = center_position[0] + fov[0]/2
    x_s = center_position[0] - fov[0]/2
    
    Mx_nw = ax + bx * x_n + cx * y_w
    My_nw = ay + by * x_n + cy * y_w
    
    Mx_se = ax + bx * x_s + cx * y_e
    My_se = ay + by * x_s + cy * y_e
    
    fov_nw_latlon = utm.to_latlon(Mx_nw, My_nw, SE_utm[2],SE_utm[3])
    fov_se_latlon = utm.to_latlon(Mx_se, My_se, SE_utm[2],SE_utm[3])
    
    return (fov_se_latlon[0] - lat_shift, fov_nw_latlon[0] - lat_shift, fov_nw_latlon[1] + lon_shift, fov_se_latlon[1] + lon_shift)

def parse_metadata(metadata):
    
    try:
        gantry_meta = metadata['lemnatec_measurement_metadata']['gantry_system_variable_metadata']
        gantry_x = gantry_meta["position x [m]"]
        gantry_y = gantry_meta["position y [m]"]
        gantry_z = gantry_meta["position z [m]"]
        
        scan_time = gantry_meta["time"]
        
        cam_meta = metadata['lemnatec_measurement_metadata']['sensor_fixed_metadata']
        cam_x = cam_meta["location in camera box x [m]"]
        cam_y = cam_meta["location in camera box y [m]"]
        
        fov_x = cam_meta["field of view x [m]"]
        fov_y = cam_meta["field of view y [m]"]
        
        if "location in camera box z [m]" in cam_meta: # this may not be in older data
            cam_z = cam_meta["location in camera box z [m]"]
        else:
            cam_z = 0

    except KeyError as err:
        fail('Metadata file missing key: ' + err.args[0])
        
    position = [float(gantry_x), float(gantry_y), float(gantry_z)]
    center_position = [position[0]+float(cam_x), position[1]+float(cam_y), position[2]+float(cam_z)]
    fov = [float(fov_x), float(fov_y)]
    
    return center_position, scan_time, fov

def get_new_fov(camHeight, fov):
    fov_x = fov[0]
    fov_y = fov[1]
        
    HEIGHT_MAGIC_NUMBER = 1.0
    camH_fix = camHeight + HEIGHT_MAGIC_NUMBER
    fix_fov_x = fov_x*(camH_fix/2)
    fix_fov_y = fov_y*(camH_fix/2)
        
    return (fix_fov_x, fix_fov_y)
    
def lower_keys(in_dict):
    if type(in_dict) is dict:
        out_dict = {}
        for key, item in in_dict.items():
            out_dict[key.lower()] = lower_keys(item)
        return out_dict
    elif type(in_dict) is list:
        return [lower_keys(obj) for obj in in_dict]
    else:
        return in_dict
    
def load_json(meta_path):
    try:
        with open(meta_path, 'r') as fin:
            return json.load(fin)
    except Exception as ex:
        fail('Corrupt metadata file, ' + str(ex))

def find_files(in_dir):
    json_suffix = os.path.join(in_dir, '*_metadata.json')
    jsons = glob(json_suffix)
    if len(jsons) == 0:
        fail('Could not find .json file')
        return [], []
        
        
    bin_suffix = os.path.join(in_dir, '*_ir.bin')
    bins = glob(bin_suffix)
    if len(bins) == 0:
        fail('Could not find .bin file')
        return [], []
    
    
    return jsons[0], bins[0]

def createVrt(base_dir,tif_file_list):
    # Create virtual tif for the files in this folder
    # Build a virtual TIF that combines all of the tifs that we just created
    print "\tCreating virtual TIF..."
    try:
        vrtPath = os.path.join(base_dir,'virtualTif.vrt')
        cmd = 'gdalbuildvrt -srcnodata "-99 -99 -99" -overwrite -input_file_list ' + tif_file_list +' ' + vrtPath
        os.system(cmd)
    except Exception as ex:
        fail("\tFailed to create virtual tif: " + str(ex))

def createMapTiles(base_dir,NUM_THREADS):
    # Create map tiles from the virtual tif
    # For now, just creating w/ local coordinate system. In the future, can make these actually georeferenced.
    print "\tCreating map tiles..."
    try:
        vrtPath = os.path.join(base_dir,'virtualTif.vrt')
        cmd = 'python gdal2tiles_parallel.py --processes=' + str(NUM_THREADS) + ' -l -n -e -f JPEG -z "18-26" -s EPSG:4326 ' + vrtPath + ' ' + os.path.join(base_dir,TILE_FOLDER_NAME)
        os.system(cmd)
    except Exception as ex:
        fail("Failed to generate map tiles: " + str(ex))

def generate_googlemaps(base_dir):
        args = os.path.join(base_dir, TILE_FOLDER_NAME)

        s = """
            <!DOCTYPE html>
                <html>
                  <head>
                    <title>Map Create By Left Sensor</title>
                    <meta name="viewport" content="initial-scale=1.0">
                    <meta charset="utf-8">
                    <style>
                      html, body {
                        height: 100%%;
                        margin: 0;
                        padding: 0;
                      }
                      #map {
                        height: 100%%;
                      }
                    </style>
                  </head>
                  <body>
                    <div id="map"></div>
                    <script>
                      function initMap() {
                          var MyCenter = new google.maps.LatLng(33.0726220351,-111.974918861);
                  var map = new google.maps.Map(document.getElementById('map'), {
                    center: MyCenter,
                    zoom: 18,
                    streetViewControl: false,
                    mapTypeControlOptions: {
                      mapTypeIds: ['Terra']
                    }
                  });
                  
                
                
                  var terraMapType = new google.maps.ImageMapType({
                    getTileUrl: function(coord, zoom) {
                        var bound = Math.pow(2, zoom);
                        var y = bound-coord.y-1;
                       return '%s' +'/' + zoom + '/' + coord.x + '/' + y + '.jpg';
                    },
                    tileSize: new google.maps.Size(256, 256),
                    maxZoom: 28,
                    minZoom: 18,
                    radius: 1738000,
                    name: 'Terra'
                  });
                  
                  map.mapTypes.set('Terra', terraMapType);
                  map.setMapTypeId('Terra');
                }
                
                    </script>
                    <script src="https://maps.googleapis.com/maps/api/js?key=AIzaSyDJW9xwkAN3sfZE4FvGGLcgufJO9oInIHk&callback=initMap"async defer></script>
                  </body>
                </html>
            """ % args
        
        f = open(os.path.join(base_dir, 'opengooglemaps.html'), 'w')
        f.write(s)
        f.close()

        return s

def fail(reason):
    print >> sys.stderr, reason


if __name__ == "__main__":

    main()
