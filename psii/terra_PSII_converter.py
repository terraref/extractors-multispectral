'''
Created on Jan 12, 2017

@author: Zongyang
'''
import os
import logging
import shutil

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.datasets

import terra_PSII_analysis as psiiCore


class PSIIBin2Png(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')
        self.parser.add_argument('--output', '-o', dest="output_dir", type=str, nargs='?',
                                 default="/home/extractor/sites/ua-mac/Level_1/ps2_png",
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
        # Check for 0000-0101 bin files before beginning processing
        if len(resource['files']) < 103:
            return CheckMessage.ignore
        ind_add = 0
        ind_output = 0
        out_dir = self.determineOutputDirectory(resource['dataset_info']['name'])
        for ind in range(0, 102):
            file_ends = "{0:0>4}".format(ind)+'.bin'
            for f in resource['files']:
                if 'filename' in f and f['filename'].endswith(file_ends):
                    ind_add += ind_add
                    if os.path.exists(os.path.join(out_dir, f['filename'][:-4]+'.png')):
                        ind_output += 1
                    break

        if ind_output == 102:
            logging.info("skipping dataset %s, outputs already exist" % resource['id'])
            return CheckMessage.ignore
        if ind_add < 102:
            return CheckMessage.ignore

        md = pyclowder.datasets.download_metadata(connector, host, secret_key,
                                                  resource['id'], self.extractor_info['name'])
        found_md = False
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
        global outputDir

        metafile, metadata = None, None

        # Get bin files and metadata
        for f in resource['local_paths']:
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

        out_dir = self.determineOutputDirectory(resource['dataset_info']['name'])
        logging.info("...output directory: %s" % out_dir)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        img_width = 1936
        img_height = 1216
        # skip 0101.bin since 101 is an XML file that lists the frame times
        for ind in range(0, 101):
            binbase = os.path.basename(frames[ind])[:-4]
            png_path = os.path.join(out_dir, binbase+'.png')
            if not os.path.exists(png_path):
                psiiCore.load_PSII_data(frames[ind], img_height, img_width, png_path)
                logging.info("......uploading %s" % png_path)
                pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], png_path)

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        metadata = {
            "@context": {
                "@vocab": "https://clowder.ncsa.illinois.edu/clowder/assets/docs/api/index.html#!/files/uploadToDataset"
            },
            "dataset_id": resource['id'],
            "content": {"status": "COMPLETED"},
            "agent": {
                "@type": "cat:extractor",
                "extractor_id": host + "/api/extractors/" + self.extractor_info['name']
            }
        }
        pyclowder.datasets.upload_metadata(connector, host, secret_key, resource['id'], metadata)

    def determineOutputDirectory(self, dsname):
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
    extractor = PSIIBin2Png()
    extractor.start()
