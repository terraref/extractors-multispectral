#!/usr/bin/env python

'''
Created on Oct 6, 2016

@author: Zongyang Li
'''

import os
import logging
import tempfile
import shutil

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets

import Get_FLIR as getFlir


class FlirBin2JpgTiff(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')
        self.parser.add_argument('--output', '-o', dest="output_dir", type=str, nargs='?',
                                 default="/home/extractor/sites/ua-mac/Level_1/flirIrCamera",
                                 help="root directory where timestamp & output directories will be created")
        self.parser.add_argument('--overwrite', dest="force_overwrite", type=bool, nargs='?', default=False,
                                 help="whether to overwrite output file if it already exists in output directory")

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

        # assign other arguments
        self.output_dir = self.args.output_dir
        self.force_overwrite = self.args.force_overwrite

    def check_message(self, connector, host, secret_key, resource, parameters):
        # Check for an ir.BIN file and metadata before beginning processing
        found_ir = None
        found_md = None

        for f in resource['files']:
            if 'filename' in f and f['filename'].endswith('_ir.bin'):
                found_ir = f['filepath']
            elif 'filename' in f and f['filename'].endswith('_metadata.json'):
                found_md = f['filepath']

        if found_ir:
            # Check if outputs already exist
            out_dir = self.determineOutputPath(resource['dataset_info']['name'])
            binbase = os.path.basename(found_ir)[:-7]
            png_path = os.path.join(out_dir, binbase+'.png')
            tiff_path = os.path.join(out_dir, binbase+'.tif')
            if os.path.exists(png_path) and os.path.exists(tiff_path) and not self.force_overwrite:
                logging.info("skipping dataset %s, outputs already exist" % resource['id'])
                return CheckMessage.ignore

            # If we don't find _metadata.json file, check if we have metadata attached to dataset instead
            if not found_md:
                md = pyclowder.datasets.download_metadata(connector, host, secret_key,
                                                          resource['id'], self.extractor_info['name'])
                if len(md) > 0:
                    for m in md:
                        # Check if this extractor has already been processed
                        if 'agent' in m and 'name' in m['agent']:
                            if m['agent']['name'].find(self.extractor_info['name']) > -1:
                                logging.info("skipping dataset %s, already processed" % resource['id'])
                                return CheckMessage.ignore
                        if 'content' in m and 'lemnatec_measurement_metadata' in m['content']:
                            found_md = True
            if found_md:
                return CheckMessage.download

        return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        metafile, bin_file, metadata = None, None, None

        # Get BIN file and metadata
        for f in resource['local_paths']:
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
            logging.error('could not find all 2 of ir.bin/metadata')
            return

        # Determine output directory
        out_dir = self.determineOutputPath(resource['dataset_info']['name'])
        logging.info("...writing outputs to: %s" % out_dir)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        #Determine output paths
        binbase = os.path.basename(bin_file)[:-7]
        png_path = os.path.join(out_dir, binbase+'.png')
        tiff_path = os.path.join(out_dir, binbase+'.tif')

        logging.info("...creating PNG image")
        if not os.path.exists(png_path) or self.force_overwrite:
            raw_data = getFlir.load_flir_data(bin_file) # get raw data from bin file
            im_color = getFlir.create_png(raw_data, png_path) # create png
            logging.info("...uploading output PNG to dataset")
            pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], png_path)

        if not os.path.exists(tiff_path) or self.force_overwrite:
            logging.info("...getting information from json file for geoTIFF")
            center_position, scan_time, fov = getFlir.parse_metadata(metadata)
            if center_position is None or scan_time is None or fov is None:
                logging.error("error getting metadata; skipping geoTIFF")
            else:
                gps_bounds = getFlir.get_bounding_box(center_position, fov) # get bounding box using gantry position and fov of camera

                logging.info("...creating TIFF image")
                # Rename temporary tif after creation to avoid long path errors
                out_tmp_tiff = tempfile.mkstemp()
                tc = getFlir.rawData_to_temperature(raw_data, scan_time, metadata) # get temperature
                getFlir.create_geotiff_with_temperature(im_color, tc, gps_bounds, out_tmp_tiff[1]) # create geotiff
                shutil.copyfile(out_tmp_tiff[1], tiff_path)
                os.remove(out_tmp_tiff[1])
                logging.info("...uploading output TIFF to dataset")
                pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], tiff_path)

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        metadata = {
            # TODO: Generate JSON-LD context for additional fields
            "@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
            "dataset_id": resource['id'],
            "content": {"status": "COMPLETED"},
            "agent": {
                "@type": "cat:extractor",
                "extractor_id": host + "/api/extractors/" + self.extractor_info['name']
            }
        }
        pyclowder.datasets.upload_metadata(connector, host, secret_key, resource['id'], metadata)

    def determineOutputPath(self, dsname):
        if dsname.find(" - ") > -1:
            timestamp = dsname.split(" - ")[1]
        else:
            timestamp = "dsname"
        if timestamp.find("__") > -1:
            datestamp = timestamp.split("__")[0]
        else:
            datestamp = ""

        return os.path.join(self.output_dir, datestamp, timestamp)

if __name__ == "__main__":
    extractor = FlirBin2JpgTiff()
    extractor.start()
