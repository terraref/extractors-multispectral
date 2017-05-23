#!/usr/bin/env python

"""
Extract NDVI or PRI from .bin file and Save to .csv file.
"""

import os
import csv
import logging
import datetime

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets
import terrautils.extractors


class BinValues2Csv(Extractor):
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
                                 default="/home/extractor/sites/ua-mac/Level_1/bin2csv",
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
        # First, check if we have the correct sensor type
        md = pyclowder.datasets.download_metadata(connector, host, secret_key, resource['parent']['id'])
        # TODO: Replace this with call to terrautils once available
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
            out_dir = terrautils.extractors.get_output_directory(self.output_dir, ds_info['name'], True)
            out_file = os.path.join(out_dir, terrautils.extractors.get_output_filename(ds_info['name'], 'csv', opts=['extracted_values']))
            if os.path.isfile(out_file) and not self.force_overwrite:
                logging.info("skipping %s, outputs already exist" % resource['id'])
                return CheckMessage.ignore

            return CheckMessage.download
        else:
            return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        created = 0
        bytes = 0

        inPath = resource['local_paths'][0]

        # Determine output file path
        ds_info = pyclowder.datasets.get_info(connector, host, secret_key, resource['parent']['id'])
        out_dir = terrautils.extractors.get_output_directory(self.output_dir, ds_info['name'], True)
        out_file = os.path.join(out_dir, terrautils.extractors.get_output_filename(ds_info['name'], 'csv', opts=['extracted_values']))
        uploaded_file_ids = []

        # Extract NDVI values
        if not os.path.isfile(out_file) or self.force_overwrite:
            logging.info("...writing values to: %s" % out_file)
            data = open(inPath, "rb").read()
            values = float(data[49:66])
            data.close()
            with open(out_file,'wb') as csvfile:
                fields = ['file_name', 'NDVI'] # fields name for csv file
                wr = csv.DictWriter(csvfile, fieldnames=fields, lineterminator = '\n')
                wr.writeheader()
                wr.writerow({'file_name':resource['name'], 'NDVI': values})

            fileid = pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['parent']['id'], outPath)
            uploaded_file_ids.append(fileid)

            created += 1
            bytes += os.path.getsize(out_file)
        else:
            logging.info("%s already exists; skipping %s" % (out_file, resource['id']))

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        metadata = terrautils.extractors.build_metadata(host, self.extractor_info['name'], resource['id'], {
            "files_created": uploaded_file_ids}, 'dataset')
        pyclowder.datasets.upload_metadata(connector, host, secret_key, resource['id'], metadata)

        endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        terrautils.extractors.log_to_influxdb(self.extractor_info['name'], starttime, endtime, created, bytes)

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

if __name__ == "__main__":
    extractor = BinValues2Csv()
    extractor.start()
