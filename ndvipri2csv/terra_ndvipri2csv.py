#!/usr/bin/env python

"""
Extract NDVI or PRI from .bin file and Save to .csv file.
"""

import os
import csv
import logging

import datetime
from dateutil.parser import parse
from influxdb import InfluxDBClient, SeriesHelper

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets


class BinValues2Csv(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')
        self.parser.add_argument('--output', '-o', dest="output_dir", type=str, nargs='?',
                                 default="/home/extractor/sites/ua-mac/Level_1/bin2csv",
                                 help="root directory where timestamp & output directories will be created")
        self.parser.add_argument('--overwrite', dest="force_overwrite", type=bool, nargs='?', default=False,
                                 help="whether to overwrite output file if it already exists in output directory")
        self.parser.add_argument('--influxHost', dest="influx_host", type=str, nargs='?',
                                 default="terra-logging.ncsa.illinois.edu", help="InfluxDB URL for logging")
        self.parser.add_argument('--influxPort', dest="influx_port", type=int, nargs='?',
                                 default=8086, help="InfluxDB port")
        self.parser.add_argument('--influxUser', dest="influx_user", type=str, nargs='?',
                                 default="terra", help="InfluxDB username")
        self.parser.add_argument('--influxPass', dest="influx_pass", type=str, nargs='?',
                                 default="", help="InfluxDB password")
        self.parser.add_argument('--influxDB', dest="influx_db", type=str, nargs='?',
                                 default="extractor_db", help="InfluxDB databast")

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

        # assign other arguments
        self.output_dir = self.args.output_dir
        self.force_overwrite = self.args.force_overwrite
        self.influx_host = self.args.influx_host
        self.influx_port = self.args.influx_port
        self.influx_user = self.args.influx_user
        self.influx_pass = self.args.influx_pass
        self.influx_db = self.args.influx_db

    def check_message(self, connector, host, secret_key, resource, parameters):
        # First, check if we have the correct sensor type
        md = pyclowder.datasets.download_metadata(connector, host, secret_key, resource['parent']['id'])
        sensortype = self.determineSensorType(md)
        if sensortype in ["ndvi", "pri"]:
            for m in md:
                # Check if this extractor has already been processed
                if 'agent' in m and 'name' in m['agent']:
                    if m['agent']['name'].find(self.extractor_info['name']) > -1:
                        logging.info("skipping dataset %s, already processed" % resource['id'])
                        return CheckMessage.ignore

            # Check if output already exists
            ds_info = pyclowder.datasets.get_info(connector, host, secret_key, resource['parent']['id'])
            outPath = self.determineOutputDir(ds_info['name'])
            if os.path.isfile(outPath) and not self.force_overwrite:
                logging.info("skipping %s, outputs already exist" % resource['id'])
                return CheckMessage.ignore

            return CheckMessage.download
        else:
            return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        created = 0
        bytes = 0

        ds_info = pyclowder.datasets.get_info(connector, host, secret_key, resource['parent']['id'])
        # Determine output file path
        outPath = self.determineOutputDir(ds_info['name'])
        inPath = resource['local_paths'][0]

        uploaded_file_ids = []

        # Extract NDVI values
        if not os.path.isfile(outPath) or self.force_overwrite:
            logging.info("...writing values to: %s" % outPath)
            data = open(inPath, "rb").read()
            values = float(data[49:66])

            with open(outPath,'wb') as csvfile:
                fields = ['file_name', 'NDVI'] # fields name for csv file
                wr = csv.DictWriter(csvfile, fieldnames=fields, lineterminator = '\n')
                wr.writeheader()
                wr.writerow({'file_name':resource['name'], 'NDVI': values})

            created += 1
            bytes += os.path.getsize(outPath)

            fileid = pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['parent']['id'], outPath)
            uploaded_file_ids.append(fileid)
        else:
            logging.info("%s already exists; skipping %s" % (outPath, resource['id']))

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        metadata = {
            # TODO: Generate JSON-LD context for additional fields
            "@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
            "dataset_id": resource['id'],
            "content": {
                "files_created": uploaded_file_ids
            },
            "agent": {
                "@type": "cat:extractor",
                "extractor_id": host + "/api/extractors/" + self.extractor_info['name']
            }
        }
        pyclowder.datasets.upload_metadata(connector, host, secret_key, resource['id'], metadata)

        endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.logToInfluxDB(starttime, endtime, created, bytes)

    # Return sensor type based on metadata parameters
    def determineSensorType(self, md):
        for meta in md:
            if 'lemnatec_measurement_metadata' in meta:
                lmm = meta['lemnatec_measurement_metadata']
                if 'sensor_fixed_metadata' in lmm and 'sensor product name' in lmm['sensor_fixed_metadata']:
                    sensor = lmm['sensor_fixed_metadata']
                    # NDVI or PRI respectively
                    if ('sensor product name' in sensor and sensor['sensor product name'] == "SKR 1860 DA"):
                        return "ndvi"
                    elif ('sensor product name' in sensor and sensor['sensor product name'] == "SKR 1860DA" or
                          'sensor id' in sensor and sensor['sensor id'] == 'pri camera box'):
                        return "pri"
                    else:
                        return "unknown"

    def determineOutputDir(self, ds_name):
        if ds_name.find(" - ") > -1:
            # sensor - timestamp
            ds_name_parts = ds_name.split(" - ")
            sensor_name = ds_name_parts[0]
            if ds_name_parts[1].find("__") > -1:
                # sensor - date__time
                ds_time_parts = ds_name_parts[1].split("__")
                timestamp = os.path.join(ds_time_parts[0], ds_name_parts[1])
            else:
                timestamp = ds_name_parts[1]
            # /sensor/date/time
            subPath = os.path.join(sensor_name, timestamp)
        else:
            subPath = ds_name

        return os.path.join(self.output_dir, subPath, "extracted_values.csv")


if __name__ == "__main__":
    extractor = BinValues2Csv()
    extractor.start()
