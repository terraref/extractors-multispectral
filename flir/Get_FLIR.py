'''
Created on Aug 23, 2016

@author: Zongyang Li
'''

import os, json, sys, math, argparse
import matplotlib.pyplot as plt
from matplotlib import cm
from glob import glob
import numpy as np
from PIL import Image
from math import cos, pi
from osgeo import gdal, osr
from numpy.matlib import repmat
from datetime import date

ZERO_ZERO = (33.0745,-111.97475)

mode_date = date(2016, 9, 15)

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
    
    print "Starting binary to image conversion..."
    full_day_convert(args.in_dir, args.out_dir)
    print "Completed binary to image conversion..."
    
    
    return

def full_day_convert(in_dir, out_dir):
    
    list_dirs = os.walk(in_dir)
    
    for root, dirs, files in list_dirs:
        for d in dirs:
            input_path = os.path.join(in_dir, d)
            output_path = os.path.join(out_dir, d)
            if not os.path.isdir(input_path):
                continue
            
            try:
                get_flir(input_path, output_path)
            except Exception as ex:
                fail('Error processing flir data in: ' + input_path + str(ex))
    
    return


def get_flir(in_dir, out_dir):
    
    metafile, binfile = find_files(in_dir)
    if metafile == [] or binfile == [] :
        return
    
    if not os.path.exists(out_dir):
        try:
            os.mkdir(out_dir)
        except:
            fail('Failed to create directory in ' + out_dir)
    
    metadata = lower_keys(load_json(metafile)) # load json file
    
    center_position, scan_time, fov = parse_metadata(metadata) # get information from json file
    
    gps_bounds = get_bounding_box(center_position, fov) # get bounding box using gantry position and fov of camera
    
    raw_data = load_flir_data(binfile) # get raw data from bin file
    
    filename = os.path.basename(binfile)
    temp_name = os.path.join(out_dir, filename)
    out_png = temp_name[:-3] + 'png'
    
    im_color = create_png(raw_data, out_png) # create png
    
    tc = rawData_to_temperature(raw_data, scan_time, metadata) # get temperature
    
    tif_path = temp_name[:-3] + 'tif'
    
    create_geotiff_with_temperature(im_color, tc, gps_bounds, tif_path) # create geotiff
    
    return


def rawData_to_temperature(rawData, scan_time, metadata):
    
    try:
        calibP = get_calibrate_param(metadata)
        tc = np.zeros((480, 640))
        
        if not calibP.calibrated:
            tc = rawData/10 - 273.15
        else:
            tc = flirRawToTemperature(rawData, calibP)
    
        return tc
    except Exception as ex:
        fail('raw to temperature fail:' + str(ex))
        
def get_calibrate_param(metadata):
    
    try:
        sensor_fixed_meta = metadata['lemnatec_measurement_metadata']['sensor_fixed_metadata']
        calibrated = sensor_fixed_meta['calibrated']
        calibparameter = calibParam()
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
        

    except KeyError as err:
        return calibParam()
    

def create_geotiff_with_temperature(np_arr, temp_arr, gps_bounds, out_file_path):
    try:
        nrows, ncols, channels = np.shape(np_arr)
        xres = (gps_bounds[3] - gps_bounds[2])/float(ncols)
        yres = (gps_bounds[1] - gps_bounds[0])/float(nrows)
        geotransform = (gps_bounds[2],xres,0,gps_bounds[1],0,-yres) #(top left x, w-e pixel resolution, rotation (0 if North is up), top left y, rotation (0 if North is up), n-s pixel resolution)

        output_raster = gdal.GetDriverByName('GTiff').Create(out_file_path, ncols, nrows, 1, gdal.GDT_Byte)

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
        return im
    except Exception as ex:
        fail('Error loading bin file' + str(ex))
        
def create_png(im, outfile_path):
    
    Gmin = im.min()
    Gmax = im.max()
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


def fail(reason):
    print >> sys.stderr, reason


if __name__ == "__main__":

    main()
