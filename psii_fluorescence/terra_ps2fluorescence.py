#!/usr/bin/env python

import os
import logging
import subprocess

from pyclowder.utils import CheckMessage
from pyclowder.datasets import upload_metadata
from pyclowder.files import upload_to_dataset
from terrautils.extractors import TerrarefExtractor, is_latest_file, build_metadata, build_dataset_hierarchy


class PSIIFluorescenceFeatures(TerrarefExtractor):
    def __init__(self):
        super(PSIIFluorescenceFeatures, self).__init__()

        # parse command line and load default logging configuration
        self.setup(sensor="ps2_fluorescence")

    def check_message(self, connector, host, secret_key, resource, parameters):
        # Check for 0000-0101 bin files before beginning processing
        if len(resource['files']) < 102:
            return CheckMessage.ignore
        if not is_latest_file(resource):
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
        self.start_message()

        for p in resource['local_paths']:
            if p.endswith(".bin"):
                input_dir = p.replace(os.path.basename(p), '')
                # TODO: Eventually light may be in separate location
                input_dir_light = input_dir

        # Determine output directory
        timestamp = resource['dataset_info']['name'].split(" - ")[1]
        out_name_base = self.sensors.create_sensor_path(timestamp, ext='')
        uploaded_file_ids = []

        subprocess.call(["octave --eval \"PSII(\'%s\',\'%s\' ,\'%s\')\"" %
                         (input_dir, input_dir_light, out_name_base)],shell=True);

        target_dsid = build_dataset_hierarchy(connector, host, secret_key, self.clowderspace,
                                              self.sensors.get_display_name(), timestamp[:4], timestamp[:7],
                                              timestamp[:10], leaf_ds_name=resource['dataset_info']['name'])

        for out_file in ["_Fm_dark", "_Fv_dark", "_FvFm_dark", "_Fm_light", "_Fv_light", "_FvFm_light",
                         "_Phi_PSII", "_NPQ", "_qN", "_qP", "_Rfd"]:
            full_out_name = out_name_base + out_file + ".png"
            if os.path.isfile(full_out_name) and full_out_name not in resource["local_paths"]:
                fileid = upload_to_dataset(connector, host, secret_key, target_dsid, full_out_name)
                uploaded_file_ids.append(fileid)
            self.created += 1
            self.bytes += os.path.getsize(full_out_name)

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        metadata = build_metadata(host, self.extractor_info['name'], target_dsid, {
            "files_created": uploaded_file_ids}, 'dataset')
        upload_metadata(connector, host, secret_key, resource['id'], metadata)

        self.end_message()

if __name__ == "__main__":
    extractor = PSIIFluorescenceFeatures()
    extractor.start()
