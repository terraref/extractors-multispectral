#!/usr/bin/env python

import os
import logging
import subprocess
import datetime

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets
import terrautils.extractors


class PSIIFluorescenceFeatures(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        influx_host = os.getenv("INFLUXDB_HOST", "terra-logging.ncsa.illinois.edu")
        influx_port = os.getenv("INFLUXDB_PORT", 8086)
        influx_db = os.getenv("INFLUXDB_DB", "extractor_db")
        influx_user = os.getenv("INFLUXDB_USER", "terra")
        influx_pass = os.getenv("INFLUXDB_PASSWORD", "")

        # add any additional arguments to parser
        self.parser.add_argument('--output', '-o', dest="output_dir", type=str, nargs='?',
                                 default="/home/extractor/sites/ua-mac/Level_1/ps2_fluorescence",
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
        if len(resource['files']) < 102:
            return CheckMessage.ignore
        if not terrautils.extractors.is_latest_file(resource):
            return CheckMessage.ignore

        # Count number of bin files in dataset, as well as number of existing outputs
        ind_add = 0
        for ind in range(0, 102):
            format_ind = "{0:0>4}".format(ind) # e.g. 1 becomes 0001
            for f in resource['files']:
                if f['filename'].endswith(format_ind+'.bin'):
                    ind_add += 1
                    break
		    
        # Do we have too few input BIN files?
        if ind_add < 102:
            return CheckMessage.ignore
        else:
            return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        created = 0
        bytes = 0

        for p in resource['local_paths']:
            if p.endswith(".bin"):
                input_dir = p.replace(os.path.basename(p), '')
                # TODO: Eventually light may be in separate location
                input_dir_light = input_dir

        # Determine output directory
        out_name_base = terrautils.sensors.get_sensor_path_by_dataset("ua-mac", "Level_1", resource['dataset_info']['name'],
                                                                "ps2_fluorescence")
        out_dir = os.path.dirname(out_name_base)
        logging.info("...writing outputs to: %s" % out_dir)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        uploaded_file_ids = []

        #subprocess.call(['git clone https://github.com/solmazhajmohammadi/PSII.git '], shell=True)
        #os.chdir('PSII')
	    #subprocess.call(['cp -rT /home/extractor/PSII .'], shell= True)
        #subprocess.call(['chmod 777 PSII.m'], shell=True)
        #subprocess.call(['octave pkg install -forge image'],shell=True)
        subprocess.call(["octave --eval \"PSII(\'%s\',\'%s\' ,\'%s\')\"" %
                         (input_dir, input_dir_light, out_name_base)],shell=True);

        for out_file in ["_Fm_dark", "_Fv_dark", "_FvFm_dark", "_Fm_light", "_Fv_light", "_FvFm_light",
                         "_Phi_PSII", "_NPQ", "_qN", "_qP", "_Rfd"]:
            full_out_name = out_name_base + out_file + ".png"
            if os.path.isfile(full_out_name) and full_out_name not in resource["local_paths"]:
                fileid = pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], full_out_name)
                uploaded_file_ids.append(fileid)
            created += 1
            bytes += os.path.getsize(full_out_name)

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        metadata = terrautils.extractors.build_metadata(host, self.extractor_info['name'], resource['id'], {
            "files_created": uploaded_file_ids}, 'dataset')
        pyclowder.datasets.upload_metadata(connector, host, secret_key, resource['id'], metadata)

        endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        terrautils.extractors.log_to_influxdb(self.extractor_info['name'], self.influx_params,
                                              starttime, endtime, created, bytes)

if __name__ == "__main__":
    extractor = PSIIFluorescenceFeatures()
    extractor.start()
