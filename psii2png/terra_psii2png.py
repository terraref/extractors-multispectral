#!/usr/bin/env python

import os
import logging
import numpy

from pyclowder.utils import CheckMessage
from pyclowder.files import upload_to_dataset
from pyclowder.datasets import download_metadata, upload_metadata
from terrautils.extractors import TerrarefExtractor, is_latest_file, load_json_file, create_image, \
    build_metadata, build_dataset_hierarchy
from terraref.metadata import get_extractor_metadata, get_terraref_metadata

import PSII_analysis as psiiCore


class PSIIBin2Png(TerrarefExtractor):
    def __init__(self):
        super(PSIIBin2Png, self).__init__()

        # parse command line and load default logging configuration
        self.setup(sensor='ps2_png')

    def check_message(self, connector, host, secret_key, resource, parameters):
        # Check for 0000-0101 bin files before beginning processing
        if len(resource['files']) < 102:
            return CheckMessage.ignore
        if not is_latest_file(resource):
            return CheckMessage.ignore

        timestamp = resource['dataset_info']['name'].split(" - ")[1]
        hist_path = self.sensors.get_sensor_path(timestamp, opts=['combined_hist'])
        coloredImg_path = self.sensors.get_sensor_path(timestamp, opts=['combined_pseudocolored'])

        # Count number of bin files in dataset, as well as number of existing outputs
        ind_add = 0
        ind_output = 0
        for ind in range(0, 102):
            format_ind = "{0:0>4}".format(ind) # e.g. 1 becomes 0001
            for f in resource['files']:
                if f['filename'].endswith(format_ind+'.bin'):
                    ind_add += 1
                    out_png = self.sensors.get_sensor_path(timestamp, opts=[format_ind])
                    if os.path.exists(out_png) and not self.overwrite:
                        ind_output += 1
                    break

        # Do the outputs already exist?
        if ind_output == 102 and os.path.exists(hist_path) and os.path.exists(coloredImg_path):
            logging.info("skipping dataset %s, outputs already exist" % resource['id'])
            return CheckMessage.ignore
        # Do we have too few input BIN files?
        if ind_add < 102:
            return CheckMessage.ignore

        md = download_metadata(connector, host, secret_key, resource['id'])
        if get_extractor_metadata(md, self.extractor_info['name']) and not self.overwrite:
            logging.info("skipping dataset %s, found existing metadata" % resource['id'])
            return CheckMessage.ignore

        if get_terraref_metadata(md):
            return CheckMessage.download

        return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message()

        # Get bin files and metadata
        metadata = None
        for f in resource['local_paths']:
            # First check metadata attached to dataset in Clowder for item of interest
            if f.endswith('_dataset_metadata.json'):
                all_dsmd = load_json_file(f)
                metadata = get_extractor_metadata(all_dsmd)
            # Otherwise, check if metadata was uploaded as a .json file
            elif f.endswith('_metadata.json') and f.find('/_metadata.json') == -1 and metadata is None:
                metadata = load_json_file(f)
        frames = {}
        for ind in range(0, 101):
            format_ind = "{0:0>4}".format(ind) # e.g. 1 becomes 0001
            for f in resource['files']:
                if f['filename'].endswith(format_ind+'.bin'):
                    frames[ind] = f['filename']
        if None in [metadata] or len(frames) < 101:
            logging.error('could not find all of frames/metadata')
            return

        # Determine output directory
        timestamp = resource['dataset_info']['name'].split(" - ")[1]
        hist_path = self.sensors.create_sensor_path(timestamp, opts=['combined_hist'])
        coloredImg_path = self.sensors.create_sensor_path(timestamp, opts=['combined_pseudocolored'])
        uploaded_file_ids = []

        target_dsid = build_dataset_hierarchy(connector, host, secret_key, self.clowderspace,
                                          self.sensors.get_display_name(), timestamp[:4], timestamp[:7],
                                          timestamp[:10], leaf_ds_name=resource['dataset_info']['name'])

        img_width = 1936
        img_height = 1216
        png_frames = {}
        # skip 0101.bin since 101 is an XML file that lists the frame times
        for ind in range(0, 101):
            format_ind = "{0:0>4}".format(ind) # e.g. 1 becomes 0001
            png_path = self.sensors.create_sensor_path(timestamp, opts=[format_ind])
            png_frames[ind] = png_path
            if not os.path.exists(png_path) or self.overwrite:
                logging.info("...generating and uploading %s" % png_path)
                pixels = numpy.fromfile(frames[ind], numpy.dtype('uint8')).reshape([img_height, img_width])
                create_image(pixels, png_path)
                if png_path not in resource['local_paths']:
                    fileid = upload_to_dataset(connector, host, secret_key, target_dsid, png_path)
                    uploaded_file_ids.append(fileid)
                self.created += 1
                self.bytes += os.path.getsize(png_path)

        # Generate aggregate outputs
        logging.info("...generating aggregates")
        if not (os.path.exists(hist_path) and os.path.exists(coloredImg_path)) or self.overwrite:
            psiiCore.psii_analysis(png_frames, hist_path, coloredImg_path)
            self.created += 2
            self.bytes += os.path.getsize(hist_path)
            self.bytes += os.path.getsize(coloredImg_path)
        if hist_path not in resource['local_paths']:
            fileid = upload_to_dataset(connector, host, secret_key, target_dsid, hist_path)
            uploaded_file_ids.append(fileid)
        if coloredImg_path not in resource['local_paths']:
            fileid = upload_to_dataset(connector, host, secret_key, target_dsid, coloredImg_path)
            uploaded_file_ids.append(fileid)

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        metadata = build_metadata(host, self.extractor_info, target_dsid, {
                                  "files_created": uploaded_file_ids}, 'dataset')
        upload_metadata(connector, host, secret_key, resource['id'], metadata)

        self.end_message()

if __name__ == "__main__":
    extractor = PSIIBin2Png()
    extractor.start()
