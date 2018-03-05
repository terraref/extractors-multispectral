#!/usr/bin/env python

import logging
import numpy
import json
import os
import re

from pyclowder.utils import CheckMessage
from pyclowder.datasets import download_metadata, upload_metadata, get_info
from terrautils.extractors import TerrarefExtractor, is_latest_file, \
    build_dataset_hierarchy, build_metadata, load_json_file
from terrautils.gdal import centroid_from_geojson, clip_raster
from terrautils.betydb import add_arguments, submit_traits, get_site_boundaries
from terrautils.geostreams import create_datapoint_with_dependencies


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
        if resource['name'].find('fullfield') > -1 and re.match("^.*\d+_ir_.*thumb.tif", resource['name']):
            return CheckMessage.download

        return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message(resource)

        # Get full list of experiment plots using date as filter
        ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
        timestamp = ds_info['name'].split(" - ")[1]
        all_plots = get_site_boundaries(timestamp, city='Maricopa')

        csv_path = self.sensors.create_sensor_path(timestamp)
        csv_file = open(csv_path, 'w')
        (fields, traits) = get_traits_table()
        csv_file.write(','.join(map(str, fields)) + '\n')

        successful_plots = 0
        nan_plots = 0
        for plotname in all_plots:
            if plotname.find("KSU") > -1:
                self.log_info(resource, "skipping %s" % plotname)
                continue
            bounds = all_plots[plotname]
            self.log_info(resource, "clipping and processing %s" % plotname)

            # Use GeoJSON string to clip full field to this plot
            (pxarray, geotrans) = clip_raster(resource['local_paths'][0], bounds)

            # Filter out any
            pxarray[pxarray < 0] = numpy.nan
            mean_tc = numpy.nanmean(pxarray) - 273.15

            # Create BETY-ready CSV
            if not numpy.isnan(mean_tc):
                traits['surface_temperature'] = str(mean_tc)
                traits['site'] = plotname
                traits['local_datetime'] = timestamp+"T12:00:00"
                trait_list = generate_traits_list(traits)
                #generate_csv(tmp_csv, fields, trait_list)
                csv_file.write(','.join(map(str, trait_list)) + '\n')

                # Prepare and submit datapoint
                centroid_lonlat = json.loads(centroid_from_geojson(bounds))["coordinates"]
                time_fmt = timestamp+"T12:00:00-07:00"
                dpmetadata = {
                    "source": host + ("" if host.endswith("/") else "/") + "files/" + resource['id'],
                    "surface_temperature": str(mean_tc)
                }
                create_datapoint_with_dependencies(connector, host, secret_key, "IR Surface Temperature",
                                                   (centroid_lonlat[1], centroid_lonlat[0]), time_fmt, time_fmt,
                                                   dpmetadata, timestamp)
            else:
                nan_plots += 1

            successful_plots += 1

        self.log_info(resource, "skipped %s of %s plots due to NaN" % (nan_plots, len(all_plots)))

        # submit CSV to BETY
        csv_file.close()
        self.log_info(resource, "submitting %s to BETYdb" % csv_path)
        submit_traits(csv_path, betykey=self.bety_key)

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        self.log_info(resource, "uploading metadata to dataset")
        metadata = build_metadata(host, self.extractor_info, resource['parent']['id'], {
            "total_plots": len(all_plots),
            "plots_processed": successful_plots,
            "blank_plots": nan_plots,
            "betydb_link": "https://terraref.ncsa.illinois.edu/bety/api/beta/variables?name=surface_temperature"
        }, 'dataset')
        upload_metadata(connector, host, secret_key, resource['parent']['id'], metadata)

        self.end_message(resource)

if __name__ == "__main__":
    extractor = FlirMeanTemp()
    extractor.start()
