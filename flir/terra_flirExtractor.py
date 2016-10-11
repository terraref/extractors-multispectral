'''
Created on Oct 6, 2016

@author: Zongyang Li
'''

import os
import logging
import imp

from config import *
import pyclowder.extractors as extractors

def main():
    global extractorName, messageType, rabbitmqExchange, rabbitmqURL, registrationEndpoints, mountedPaths

    #set logging
    logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
    logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)
    logger = logging.getLogger('extractor')
    logger.setLevel(logging.DEBUG)

    # setup
    extractors.setup(extractorName=extractorName,
                     messageType=messageType,
                     rabbitmqURL=rabbitmqURL,
                     rabbitmqExchange=rabbitmqExchange)

    # register extractor info
    extractors.register_extractor(registrationEndpoints)

    #connect to rabbitmq
    extractors.connect_message_bus(extractorName=extractorName,
                                   messageType=messageType,
                                   processFileFunction=process_dataset,
                                   checkMessageFunction=check_message,
                                   rabbitmqExchange=rabbitmqExchange,
                                   rabbitmqURL=rabbitmqURL)

def check_message(parameters):
    # Check for a left and right file before beginning processing
    found_ir = False
    found_json = False
    for f in parameters['filelist']:
        if 'filename' in f and f['filename'].endswith('_ir.bin'):
            found_ir = True
        elif 'filename' in f and f['filename'].endswith('_metadata.json'):
            found_json = True

    if not (found_ir and found_json):
        return False

    # TODO: re-enable once this is merged into Clowder: https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/clowder/pull-requests/883/overview
    # fetch metadata from dataset to check if we should remove existing entry for this extractor first
    md = extractors.download_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
    found_meta = False
    for m in md:
        if 'agent' in m and 'name' in m['agent']:
            if m['agent']['name'].find(extractorName) > -1:
                print("skipping dataset %s, already processed" % parameters['datasetId'])
                return False
                #extractors.remove_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
        # Check for required metadata before beginning processing
        if 'content' in m and 'lemnatec_measurement_metadata' in m['content']:
            found_meta = True

    if found_ir and found_json:
        return True
    else:
        return False

def process_dataset(parameters):
    global outputDir

    metafile, bin_file, metadata = None, None, None

    # Get left/right files and metadata
    for f in parameters['files']:
        # First check metadata attached to dataset in Clowder for item of interest
        if f.endswith('_dataset_metadata.json'):
            all_dsmd = getFlir.load_json(f)
            for curr_dsmd in all_dsmd:
                if 'content' in curr_dsmd and 'lemnatec_measurement_metadata' in curr_dsmd['content']:
                    metafile = f
                    metadata = curr_dsmd['content']
        # Otherwise, check if metadata was uploaded as a .json file
        elif f.endswith('_metadata.json') and f.find('/_metadata.json') == -1 and metafile is None:
            metafile = f
            metadata = getFlir.load_json(metafile)
        elif f.endswith('_ir.bin'):
            bin_file = f
    if None in [metafile, bin_file, metadata]:
        getFlir.fail('Could not find all of ir.bin/metadata.')
        return

    print("...bin_file: %s" % bin_file)
    print("...metafile: %s" % metafile)
    dsname = parameters["datasetInfo"]["name"]
    if dsname.find(" - ") > -1:
        timestamp = dsname.split(" - ")[1]
    else:
        timestamp = "dsname"
    if timestamp.find("__") > -1:
        datestamp = timestamp.split("__")[0]
    else:
        datestamp = ""
    out_dir = os.path.join(outputDir, datestamp, timestamp)
    print("...output directory: %s" % out_dir)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    #Determine output paths
    binbase = os.path.basename(bin_file)[:-7]
    png_path = os.path.join(out_dir, binbase+'.png')
    tiff_path = os.path.join(out_dir, binbase+'.tif')
    print("...png: %s" % (png_path))
    print("...tif: %s" % (tiff_path))

    print("getting information from json file")
    center_position, scan_time, fov = getFlir.parse_metadata(metadata) 
    gps_bounds = getFlir.get_bounding_box(center_position, fov) # get bounding box using gantry position and fov of camera
    
    print("Creating png image")
    raw_data = getFlir.load_flir_data(bin_file) # get raw data from bin file
    im_color = getFlir.create_png(raw_data, png_path) # create png
    print("Uploading output PNGs to dataset")
    extractors.upload_file_to_dataset(png_path, parameters)
    
    print("Creating geoTIFF images")
    tc = getFlir.rawData_to_temperature(raw_data, scan_time, metadata) # get temperature
    getFlir.create_geotiff_with_temperature(im_color, tc, gps_bounds, tiff_path) # create geotiff
    print("Uploading output geoTIFFs to dataset")
    extractors.upload_file_to_dataset(tiff_path, parameters)
    

    # Tell Clowder this is completed so subsequent file updates don't daisy-chain
    metadata = {
        "@context": {
            "@vocab": "https://clowder.ncsa.illinois.edu/clowder/assets/docs/api/index.html#!/files/uploadToDataset"
        },
        "dataset_id": parameters["datasetId"],
        "content": {"status": "COMPLETED"},
        "agent": {
            "@type": "cat:extractor",
            "extractor_id": parameters['host'] + "/api/extractors/" + extractorName
        }
    }
    extractors.upload_dataset_metadata_jsonld(mdata=metadata, parameters=parameters)

if __name__ == "__main__":
    global getFlirScript

    # Import demosaic script from configured location
    getFlir = imp.load_source('Get_FLIR', getFlirScript)

    main()