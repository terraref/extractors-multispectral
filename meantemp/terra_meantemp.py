#!/usr/bin/env python

import logging
import numpy
import json
import os
import re
import yaml

from pyclowder.utils import CheckMessage
from pyclowder.files import submit_extraction
from pyclowder.datasets import download_metadata, upload_metadata, get_info
from terrautils.extractors import TerrarefExtractor, is_latest_file, \
    build_dataset_hierarchy, build_metadata, load_json_file, upload_to_dataset, file_exists
from terrautils.gdal import centroid_from_geojson, clip_raster
from terrautils.betydb import add_arguments, submit_traits, get_site_boundaries
from terrautils.metadata import get_extractor_metadata
from terrautils.spatial import geojson_to_tuples_betydb


logging.basicConfig(format='%(asctime)s %(message)s')

def add_local_arguments(parser):
    # add any additional arguments to parser
    add_arguments(parser)

def get_traits_table():
    # Compiled traits table
    fields = ('local_datetime', 'surface_temperature', 'access_level', 'site', 'method')
    traits = {'local_datetime' : '',
              'surface_temperature' : [],
              'access_level': '2',
              'site': [],
              'method': 'Mean temperature from infrared images'}

    return (fields, traits)

def generate_traits_list(traits):
    # compose the summary traits
    trait_list = [  traits['local_datetime'],
                    traits['surface_temperature'],
                    traits['access_level'],
                    traits['site'],
                    traits['method']
                    ]

    return trait_list

def generate_csv(fname, fields, trait_list):
    """ Generate CSV called fname with fields and trait_list """
    csv = open(fname, 'w')
    csv.write(','.join(map(str, fields)) + '\n')
    csv.write(','.join(map(str, trait_list)) + '\n')
    csv.close()

    return fname

class FlirMeanTemp(TerrarefExtractor):
    def __init__(self):
        super(FlirMeanTemp, self).__init__()

        add_local_arguments(self.parser)

        # parse command line and load default logging configuration
        self.setup(sensor='ir_meanTemp')

        # assign other argumentse
        self.bety_url = self.args.bety_url
        self.bety_key = self.args.bety_key

    def check_message(self, connector, host, secret_key, resource, parameters):
        if resource['name'].find('fullfield') > -1 and re.match("^.*\d+_ir_.*.tif", resource['name']):
            # Check metadata to verify we have what we need
            md = download_metadata(connector, host, secret_key, resource['id'])
            if get_extractor_metadata(md, self.extractor_info['name']) and not self.overwrite:
                self.log_skip(resource,"metadata indicates it was already processed")
                return CheckMessage.ignore
            return CheckMessage.download
        else:
            self.log_skip(resource,"regex not matched for %s" % resource['name'])
            return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message(resource)

        # Get full list of experiment plots using date as filter
        ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
        timestamp = ds_info['name'].split(" - ")[1]
        time_fmt = timestamp+"T12:00:00-07:00"
        rootdir = self.sensors.create_sensor_path(timestamp, sensor="fullfield", ext=".csv")
        out_csv = os.path.join(os.path.dirname(rootdir),
                               resource['name'].replace(".tif", "_meantemp_bety.csv"))
        out_geo = os.path.join(os.path.dirname(rootdir),
                               resource['name'].replace(".tif", "_meantemp_geo.csv"))

        # TODO: What should happen if CSV already exists? If we're here, there's no completed metadata...

        self.log_info(resource, "Writing BETY CSV to %s" % out_csv)
        csv_file = open(out_csv, 'w')
        (fields, traits) = get_traits_table()
        csv_file.write(','.join(map(str, fields)) + '\n')

        self.log_info(resource, "Writing Geostreams CSV to %s" % out_geo)
        geo_file = open(out_geo, 'w')
        geo_file.write(','.join(['site', 'trait', 'lat', 'lon', 'dp_time', 'source', 'value', 'timestamp']) + '\n')

        successful_plots = 0
        nan_plots = 0
        all_plots = get_site_boundaries(timestamp, city='Maricopa')
        for plotname in all_plots:
            if plotname.find("KSU") > -1:
                self.log_info(resource, "skipping %s" % plotname)
                continue

            bounds = all_plots[plotname]
            tuples = geojson_to_tuples_betydb(yaml.safe_load(bounds))
            centroid_lonlat = json.loads(centroid_from_geojson(bounds))["coordinates"]

            # Use GeoJSON string to clip full field to this plot
            pxarray = clip_raster(resource['local_paths'][0], tuples)

            # Filter out any
            pxarray[pxarray < 0] = numpy.nan
            mean_tc = numpy.nanmean(pxarray) - 273.15

            # Create BETY-ready CSV
            if not numpy.isnan(mean_tc):
                geo_file.write(','.join([plotname,
                                         'IR Surface Temperature',
                                         str(centroid_lonlat[1]),
                                         str(centroid_lonlat[0]),
                                         time_fmt,
                                         host + ("" if host.endswith("/") else "/") + "files/" + resource['id'],
                                         str(mean_tc),
                                         timestamp]) + '\n')

                traits['surface_temperature'] = str(mean_tc)
                traits['site'] = plotname
                traits['local_datetime'] = timestamp+"T12:00:00"
                trait_list = generate_traits_list(traits)
                csv_file.write(','.join(map(str, trait_list)) + '\n')
            else:
                nan_plots += 1

            successful_plots += 1

        self.log_info(resource, "skipped %s of %s plots due to NaN" % (nan_plots, len(all_plots)))

        # submit CSV to BETY
        csv_file.close()
        geo_file.close()

        # Upload CSVs to Clowder
        fileid = upload_to_dataset(connector, host, self.clowder_user, self.clowder_pass, resource['parent']['id'], out_csv)
        geoid  = upload_to_dataset(connector, host, self.clowder_user, self.clowder_pass, resource['parent']['id'], out_geo)

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        self.log_info(resource, "updating file metadata")
        metadata = build_metadata(host, self.extractor_info, resource['parent']['id'], {
            "total_plots": len(all_plots),
            "plots_processed": successful_plots,
            "blank_plots": nan_plots,
            "files_created": [fileid, geoid],
            "betydb_link": "https://terraref.ncsa.illinois.edu/bety/api/beta/variables?name=surface_temperature"
        }, 'dataset')
        upload_metadata(connector, host, secret_key, resource['parent']['id'], metadata)

        # Trigger downstream extractors
        self.log_info(resource, "triggering BETY extractor on %s" % fileid)
        submit_extraction(connector, host, secret_key, fileid, "terra.betydb")
        self.log_info(resource, "triggering geostreams extractor on %s" % geoid)
        submit_extraction(connector, host, secret_key, geoid, "terra.geostreams")

        self.end_message(resource)

if __name__ == "__main__":
    extractor = FlirMeanTemp()
    extractor.start()
