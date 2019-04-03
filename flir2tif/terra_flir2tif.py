#!/usr/bin/env python

import os
import shutil
import numpy
import tempfile

from pyclowder.utils import CheckMessage
from pyclowder.files import upload_to_dataset
from pyclowder.datasets import download_metadata, upload_metadata, remove_metadata, submit_extraction
from terrautils.metadata import get_extractor_metadata, get_terraref_metadata, calculate_scan_time, \
    get_season_and_experiment
from terrautils.extractors import TerrarefExtractor, is_latest_file, check_file_in_dataset, \
    build_dataset_hierarchy_crawl, build_metadata, load_json_file, file_exists, contains_required_files
from terrautils.formats import create_geotiff, create_image
from terrautils.spatial import geojson_to_tuples, geojson_to_tuples_betydb
from terrautils.betydb import add_arguments, get_site_boundaries

import Get_FLIR as getFlir


def add_local_arguments(parser):
    # add any additional arguments to parser
    parser.add_argument('--scale', dest="scale_values", type=bool, nargs='?', default=True,
                        help="scale individual flir images based on px range as opposed to full field stitch")

    add_arguments(parser)

class FlirBin2JpgTiff(TerrarefExtractor):
    def __init__(self):
        super(FlirBin2JpgTiff, self).__init__()

        add_local_arguments(self.parser)

        # parse command line and load default logging configuration
        self.setup(sensor='ir_geotiff')

        # assign other arguments
        self.scale_values = self.args.scale_values

    def check_message(self, connector, host, secret_key, resource, parameters):
        if "rulechecked" in parameters and parameters["rulechecked"]:
            return CheckMessage.download

        if not is_latest_file(resource):
            self.log_skip(resource, "not latest file")
            return CheckMessage.ignore

        # Check for an _ir.bin file before beginning processing
        if not contains_required_files(resource, ['_ir.bin']):
            self.log_skip(resource, "missing required files")
            return CheckMessage.ignore

        # Check metadata to verify we have what we need
        md = download_metadata(connector, host, secret_key, resource['id'])
        if get_terraref_metadata(md):
            if get_extractor_metadata(md, self.extractor_info['name'], self.extractor_info['version']):
                # Make sure outputs properly exist
                timestamp = resource['dataset_info']['name'].split(" - ")[1]
                tif = self.sensors.get_sensor_path(timestamp)
                png = tif.replace(".tif", ".png")
                if file_exists(png) and file_exists(tif):
                    self.log_skip(resource, "metadata v%s and outputs already exist" % self.extractor_info['version'])
                    return CheckMessage.ignore
            # Have TERRA-REF metadata, but not any from this extractor
            return CheckMessage.download
        else:
            self.log_skip(resource, "no terraref metadata found")
            return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message(resource)

        # Get BIN file and metadata
        bin_file, terra_md_full = None, None
        for f in resource['local_paths']:
            if f.endswith('_dataset_metadata.json'):
                all_dsmd = load_json_file(f)
                terra_md_full = get_terraref_metadata(all_dsmd, 'flirIrCamera')
            elif f.endswith('_ir.bin'):
                bin_file = f
        if None in [bin_file, terra_md_full]:
            raise ValueError("could not locate all files & metadata in processing")

        timestamp = resource['dataset_info']['name'].split(" - ")[1]

        # Fetch experiment name from terra metadata
        season_name, experiment_name, updated_experiment = get_season_and_experiment(timestamp, 'flirIrCamera', terra_md_full)
        if None in [season_name, experiment_name]:
            raise ValueError("season and experiment could not be determined")

        # Determine output directory
        self.log_info(resource, "Hierarchy: %s / %s / %s / %s / %s / %s / %s" % (season_name, experiment_name, self.sensors.get_display_name(),
                                                                                 timestamp[:4], timestamp[5:7], timestamp[8:10], timestamp))
        target_dsid = build_dataset_hierarchy_crawl(host, secret_key, self.clowder_user, self.clowder_pass, self.clowderspace,
                                              season_name, experiment_name, self.sensors.get_display_name(),
                                              timestamp[:4], timestamp[5:7], timestamp[8:10],
                                              leaf_ds_name=self.sensors.get_display_name()+' - '+timestamp)
        tiff_path = self.sensors.create_sensor_path(timestamp)
        png_path = tiff_path.replace(".tif", ".png")
        uploaded_file_ids = []

        # Attach LemnaTec source metadata to Level_1 product
        self.log_info(resource, "uploading LemnaTec metadata to ds [%s]" % target_dsid)
        remove_metadata(connector, host, secret_key, target_dsid, self.extractor_info['name'])
        terra_md_trim = get_terraref_metadata(all_dsmd)
        if updated_experiment is not None:
            terra_md_trim['experiment_metadata'] = updated_experiment
        terra_md_trim['raw_data_source'] = host + ("" if host.endswith("/") else "/") + "datasets/" + resource['id']
        level1_md = build_metadata(host, self.extractor_info, target_dsid, terra_md_trim, 'dataset')
        upload_metadata(connector, host, secret_key, target_dsid, level1_md)

        skipped_png = False
        if not file_exists(png_path) or self.overwrite:
            # Perform actual processing
            self.log_info(resource, "creating & uploading %s" % png_path)
            raw_data = numpy.fromfile(bin_file, numpy.dtype('<u2')).reshape([480, 640]).astype('float')
            raw_data = numpy.rot90(raw_data, 3)
            create_image(raw_data, png_path, self.scale_values)
            self.created += 1
            self.bytes += os.path.getsize(png_path)
        else:
            skipped_png = True
        # Only upload the newly generated file to Clowder if it isn't already in dataset
        found_in_dest = check_file_in_dataset(connector, host, secret_key, target_dsid, png_path, remove=self.overwrite)
        if not found_in_dest or self.overwrite:
            fileid = upload_to_dataset(connector, host, secret_key, target_dsid, png_path)
            uploaded_file_ids.append(host + ("" if host.endswith("/") else "/") + "files/" + fileid)

        if not file_exists(tiff_path) or self.overwrite:
            # Generate temperature matrix and perform actual processing
            self.log_info(resource, "creating & uploading %s" % tiff_path)
            gps_bounds = geojson_to_tuples(terra_md_full['spatial_metadata']['flirIrCamera']['bounding_box'])
            if skipped_png:
                raw_data = numpy.fromfile(bin_file, numpy.dtype('<u2')).reshape([480, 640]).astype('float')
                raw_data = numpy.rot90(raw_data, 3)
            tc = getFlir.rawData_to_temperature(raw_data, terra_md_full) # get temperature
            create_geotiff(tc, gps_bounds, tiff_path, None, True, self.extractor_info, terra_md_full)
            self.created += 1
            self.bytes += os.path.getsize(tiff_path)
        # Only upload the newly generated file to Clowder if it isn't already in dataset
        found_in_dest = check_file_in_dataset(connector, host, secret_key, target_dsid, tiff_path, remove=self.overwrite)
        if not found_in_dest or self.overwrite:
            fileid = upload_to_dataset(connector, host, secret_key, target_dsid, tiff_path)
            uploaded_file_ids.append(host + ("" if host.endswith("/") else "/") + "files/" + fileid)

        # Trigger additional extractors
        self.log_info(resource, "triggering downstream extractors")
        submit_extraction(connector, host, secret_key, target_dsid, "terra.plotclipper_tif")

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        if len(uploaded_file_ids) > 0:
            extractor_md = build_metadata(host, self.extractor_info, target_dsid, {
                "files_created": uploaded_file_ids
            }, 'dataset')
            self.log_info(resource, "uploading extractor metadata to raw dataset")
            remove_metadata(connector, host, secret_key, resource['id'], self.extractor_info['name'])
            upload_metadata(connector, host, secret_key, resource['id'], extractor_md)

        self.end_message(resource)

if __name__ == "__main__":
    extractor = FlirBin2JpgTiff()
    extractor.start()
