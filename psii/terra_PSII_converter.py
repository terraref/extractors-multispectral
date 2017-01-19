'''
Created on Jan 12, 2017

@author: Zongyang
'''
import os
import logging
import imp
import requests

from config import *
import pyclowder.extractors as extractors
import shutil

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
    # Check for 0000-0101 bin files before beginning processing
    if len(parameters['filelist']) < 103:
        return False
    
    ind_add = 0
    for ind in range(0, 102):
        file_ends = "{0:0>4}".format(ind)+'.bin'
        for f in parameters['filelist']:
            if 'filename' in f and f['filename'].endswith(file_ends):
                ind_add = ind_add + 1
                break
    
    if ind_add < 102:
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
            
    for f in parameters['filelist']:
        if 'filename' in f and f['filename'].endswith('_metadata.json'):
            found_meta = True

    return found_meta
    
def process_dataset(parameters):
    global outputDir

    metafile, metadata = None, None

    # Get bin files and metadata
    for f in parameters['files']:
        # First check metadata attached to dataset in Clowder for item of interest
        if f.endswith('_dataset_metadata.json'):
            all_dsmd = psiiCore.load_json(f)
            for curr_dsmd in all_dsmd:
                if 'content' in curr_dsmd and 'lemnatec_measurement_metadata' in curr_dsmd['content']:
                    metafile = f
                    metadata = curr_dsmd['content']
        # Otherwise, check if metadata was uploaded as a .json file
        elif f.endswith('_metadata.json') and f.find('/_metadata.json') == -1 and metafile is None:
            metafile = f
            metadata = psiiCore.load_json(metafile)
    
    frames = {}
    for ind in range(0, 101):
        file_ends = "{0:0>4}".format(ind)+'.bin'
        for f in parameters['files']:
            if f.endswith(file_ends):
                frames[ind] = f
    
    if None in [metafile, metadata] or len(frames) < 101:
        psiiCore.fail('Could not find all of frames/metadata.')
        return
    
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
        
    img_width = 1936
    img_height = 1216
    # skip 0101.bin since 101 is an XML file that lists the frame times
    for ind in range(0, 101):
        binbase = os.path.basename(frames[ind])[:-4]
        png_path = os.path.join(out_dir, binbase+'.png')
        print("...png: %s" % (png_path))
        psiiCore.load_PSII_data(frames[ind], img_height, img_width, png_path)
        
        print("Uploading output PNGs to dataset")
        extractors.upload_file_to_dataset(png_path, parameters)
    
    # upload metadata
    out_metafile = os.path.join(out_dir, os.path.basename(metafile))
    shutil.copyfile(metafile, out_metafile)
    extractors.upload_file_to_dataset(out_metafile, parameters)
    
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
    return


if __name__ == "__main__":
    global getPSIIExtractorScript

    # Import canopyCover script from configured location
    psiiCore = imp.load_source('PSII_analysis', getPSIIExtractorScript)

    main()