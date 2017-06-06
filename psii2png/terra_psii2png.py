#!/usr/bin/env python

import os
import logging
import datetime

import numpy

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.datasets
import terrautils.extractors

import PSII_analysis as psiiCore


class PSIIBin2Png(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        influx_host = os.getenv("INFLUXDB_HOST", "terra-logging.ncsa.illinois.edu")
        influx_port = os.getenv("INFLUXDB_PORT", 8086)
        influx_db = os.getenv("INFLUXDB_DB", "extractor_db")
        influx_user = os.getenv("INFLUXDB_USER", "terra")
        influx_pass = os.getenv("INFLUXDB_PASSWORD", "")

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')
        self.parser.add_argument('--output', '-o', dest="output_dir", type=str, nargs='?',
                                 default="/home/extractor/sites/ua-mac/Level_1/ps2_png",
                                 help="root directory where timestamp & output directories will be created")
        self.parser.add_argument('--overwrite', dest="force_overwrite", type=bool, nargs='?', default=False,
                                 help="whether to overwrite output file if it already exists in output directory")
        self.parser.add_argument('--influxHost', dest="influx_host", type=str, nargs='?',
                                 default=influx_host, help="InfluxDB URL for logging")
        self.parser.add_argument('--influxPort', dest="influx_port", type=int, nargs='?',
                                 default=influx_port, help="InfluxDB port")
        self.parser.add_argument('--influxUser', dest="influx_user", type=str, nargs='?',
                                 default=influx_user, help="InfluxDB username")
        self.parser.add_argument('--influxPass', dest="influx_pass", type=str, nargs='?',
                                 default=influx_pass, help="InfluxDB password")
        self.parser.add_argument('--influxDB', dest="influx_db", type=str, nargs='?',
                                 default=influx_db, help="InfluxDB database")

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

        # assign other arguments
        self.output_dir = self.args.output_dir
        self.force_overwrite = self.args.force_overwrite
        self.influx_params = {
            "host": self.args.influx_host,
            "port": self.args.influx_port,
            "db": self.args.influx_db,
            "user": self.args.influx_user,
            "pass": self.args.influx_pass
        }

    def check_message(self, connector, host, secret_key, resource, parameters):
        # Check for 0000-0101 bin files before beginning processing
        if len(resource['files']) < 103:
            return CheckMessage.ignore
        if not terrautils.extractors.is_latest_file(resource):
            return CheckMessage.ignore

        out_dir = terrautils.extractors.get_output_directory(self.output_dir, resource['dataset_info']['name'])
        hist_path = os.path.join(out_dir, terrautils.extractors.get_output_filename(
                resource['dataset_info']['name'], 'png', opts=['combined_hist']))
        coloredImg_path = os.path.join(out_dir, terrautils.extractors.get_output_filename(
                resource['dataset_info']['name'], 'png', opts=['combined_pseudocolored']))

        # Count number of bin files in dataset, as well as number of existing outputs
        ind_add = 0
        ind_output = 0
        for ind in range(0, 102):
            format_ind = "{0:0>4}".format(ind) # e.g. 1 becomes 0001
            for f in resource['files']:
                if f['filename'].endswith(format_ind+'.bin'):
                    ind_add += 1
                    out_png = os.path.join(out_dir, terrautils.extractors.get_output_filename(
                            resource['dataset_info']['name'], 'png', opts=[format_ind]))
                    if os.path.exists(out_png) and not self.force_overwrite:
                        ind_output += 1
                    break

        # Do the outputs already exist?
        if ind_output == 102 and os.path.exists(hist_path) and os.path.exists(coloredImg_path):
            logging.info("skipping dataset %s, outputs already exist" % resource['id'])
            return CheckMessage.ignore
        # Do we have too few input BIN files?
        if ind_add < 102:
            return CheckMessage.ignore

        md = pyclowder.datasets.download_metadata(connector, host, secret_key, resource['id'])
        found_md = False
        if len(md) > 0:
            for m in md:
                # Check if this extractor has already been processed
                if 'agent' in m and 'name' in m['agent']:
                    if m['agent']['name'].find(self.extractor_info['name']) > -1 and not self.force_overwrite:
                        logging.info("skipping dataset %s, found existing metadata" % resource['id'])
                        return CheckMessage.ignore
                if 'content' in m and 'lemnatec_measurement_metadata' in m['content']:
                    found_md = True
        if found_md:
            return CheckMessage.download

        return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        created = 0
        bytes = 0

        # Get bin files and metadata
        metadata = None
        for f in resource['local_paths']:
            # First check metadata attached to dataset in Clowder for item of interest
            if f.endswith('_dataset_metadata.json'):
                all_dsmd = terrautils.extractors.load_json_file(f)
                for curr_dsmd in all_dsmd:
                    if 'content' in curr_dsmd and 'lemnatec_measurement_metadata' in curr_dsmd['content']:
                        metadata = curr_dsmd['content']
            # Otherwise, check if metadata was uploaded as a .json file
            elif f.endswith('_metadata.json') and f.find('/_metadata.json') == -1 and metadata is None:
                metadata = terrautils.extractors.load_json_file(f)
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
        out_dir = terrautils.extractors.get_output_directory(self.output_dir, resource['dataset_info']['name'])
        logging.info("...output directory: %s" % out_dir)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        hist_path = os.path.join(out_dir, terrautils.extractors.get_output_filename(
                resource['dataset_info']['name'], 'png', opts=['combined_hist']))
        coloredImg_path = os.path.join(out_dir, terrautils.extractors.get_output_filename(
                resource['dataset_info']['name'], 'png', opts=['combined_pseudocolored']))
        uploaded_file_ids = []

        img_width = 1936
        img_height = 1216
        png_frames = {}
        # skip 0101.bin since 101 is an XML file that lists the frame times
        for ind in range(0, 101):
            format_ind = "{0:0>4}".format(ind) # e.g. 1 becomes 0001
            png_path = os.path.join(out_dir, terrautils.extractors.get_output_filename(
                    resource['dataset_info']['name'], 'png', opts=[format_ind]))
            png_frames[ind] = png_path
            if not os.path.exists(png_path) or self.force_overwrite:
                logging.info("...generating and uploading %s" % png_path)
                pixels = numpy.fromfile(frames[ind], numpy.dtype('uint8')).reshape([img_height, img_width])
                terrautils.extractors.create_image(pixels, png_path)
                if png_path not in resource['local_paths']:
                    fileid = pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], png_path)
                    uploaded_file_ids.append(fileid)
                created += 1
                bytes += os.path.getsize(png_path)

        # Generate aggregate outputs
        logging.info("...generating aggregates")
        if not (os.path.exists(hist_path) and os.path.exists(coloredImg_path)) or self.force_overwrite:
            psiiCore.psii_analysis(png_frames, hist_path, coloredImg_path)
            created += 2
            bytes += os.path.getsize(hist_path)
            bytes += os.path.getsize(coloredImg_path)
        if hist_path not in resource['local_paths']:
            fileid = pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], hist_path)
            uploaded_file_ids.append(fileid)
        if coloredImg_path not in resource['local_paths']:
            fileid = pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], coloredImg_path)
            uploaded_file_ids.append(fileid)

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        metadata = terrautils.extractors.build_metadata(host, self.extractor_info['name'], resource['id'], {
                                                            "files_created": uploaded_file_ids}, 'dataset')
        pyclowder.datasets.upload_metadata(connector, host, secret_key, resource['id'], metadata)

        endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        terrautils.extractors.log_to_influxdb(self.extractor_info['name'], self.influx_params,
                                              starttime, endtime, created, bytes)

if __name__ == "__main__":
    extractor = PSIIBin2Png()
    extractor.start()
