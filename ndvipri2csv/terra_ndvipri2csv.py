#!/usr/bin/env python

"""
Extract NDVI or PRI from .bin file and Save to .csv file.
"""

import os
import csv
import logging

from pyclowder.utils import CheckMessage
from pyclowder.datasets import download_metadata, get_info, upload_metadata
from pyclowder.files import upload_to_dataset
from terrautils.metadata import get_extractor_metadata
from terrautils.extractors import TerrarefExtractor, is_latest_file, build_dataset_hierarchy, \
    build_metadata


class BinValues2Csv(TerrarefExtractor):
    def __init__(self):
        super(BinValues2Csv, self).__init__()

        # parse command line and load default logging configuration
        self.setup(sensor="ndvipri2csv")

    def check_message(self, connector, host, secret_key, resource, parameters):
        # First, check if we have the correct sensor type
        md = download_metadata(connector, host, secret_key, resource['parent']['id'])
        ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
        sensortype = self.determineSensorType(ds_info['name'])
        if sensortype in ["ndvi", "pri"]:
            if get_extractor_metadata(md, self.extractor_info['name']) and not self.overwrite:
                logging.info("skipping dataset %s, already processed" % resource['id'])
                return CheckMessage.ignore

            # Check if output already exists
            timestamp = ds_info['name'].split(" - ")[1]
            out_file = self.get_sensor_path(timestamp, opts=['extracted_values'])
            if os.path.isfile(out_file) and not self.overwrite:
                logging.info("skipping %s, outputs already exist" % resource['id'])
                return CheckMessage.ignore

            return CheckMessage.download
        else:
            return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message()

        inPath = resource['local_paths'][0]

        # Determine output file path
        ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
        timestamp = ds_info['name'].split(" - ")[1]
        out_file = self.create_sensor_path(timestamp, opts=['extracted_values'])
        uploaded_file_ids = []

        target_dsid = build_dataset_hierarchy(connector, host, secret_key, self.clowderspace,
                                              self.sensors.get_display_name(), timestamp[:4], timestamp[:7],
                                              timestamp[:10], leaf_ds_name=resource['dataset_info']['name'])

        # Extract NDVI values
        if not os.path.isfile(out_file) or self.overwrite:
            logging.info("...writing values to: %s" % out_file)
            data = open(inPath, "rb").read()
            values = float(data[49:66])
            data.close()
            with open(out_file,'wb') as csvfile:
                fields = ['file_name', 'NDVI'] # fields name for csv file
                wr = csv.DictWriter(csvfile, fieldnames=fields, lineterminator = '\n')
                wr.writeheader()
                wr.writerow({'file_name':resource['name'], 'NDVI': values})

            fileid = upload_to_dataset(connector, host, secret_key, target_dsid, out_file)
            uploaded_file_ids.append(fileid)

            self.created += 1
            self.bytes += os.path.getsize(out_file)
        else:
            logging.info("%s already exists; skipping %s" % (out_file, resource['id']))

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        metadata = build_metadata(host, self.extractor_info['name'], target_dsid, {
            "files_created": uploaded_file_ids}, 'dataset')
        upload_metadata(connector, host, secret_key, target_dsid, metadata)

        self.end_message()

    # Return sensor type based on metadata parameters
    def determineSensorType(self, ds_name):
        if ds_name.lower().find('ndviSensor') > -1:
            return 'ndvi'
        elif ds_name.lower().find('priSensor') > -1:
            return 'pri'
        else:
            return 'unknown'

if __name__ == "__main__":
    extractor = BinValues2Csv()
    extractor.start()
