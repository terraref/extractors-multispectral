#!/usr/bin/env python

import logging
import numpy
import json
import os

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
    fields = ('local_datetime', 'canopy_cover', 'access_level', 'species', 'site',
              'citation_author', 'citation_year', 'citation_title', 'method')
    traits = {'local_datetime' : '',
              'surface_temperature' : [],
              'access_level': '2',
              'species': 'Sorghum bicolor',
              'site': [],
              'citation_author': '"Zongyang, Li"',
              'citation_year': '2016',
              'citation_title': 'Maricopa Field Station Data and Metadata',
              'method': 'Mean temperature from infrared images'}

    return (fields, traits)

def generate_traits_list(traits):
    # compose the summary traits
    trait_list = [  traits['local_datetime'],
                    traits['surface_temperature'],
                    traits['access_level'],
                    traits['species'],
                    traits['site'],
                    traits['citation_author'],
                    traits['citation_year'],
                    traits['citation_title'],
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
        if resource['name'].find('fullfield') > -1 and resource['name'].find('_ir.tif') > -1:
            return CheckMessage.download

        return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message()

        tmp_csv = "meantemptraits.csv"

        # Get full list of experiment plots using date as filter
        ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
        dsmd = download_metadata(connector, host, secret_key, resource['parent']['id'])
        timestamp = ds_info['name'].split(" - ")[1]

        all_plots = get_site_boundaries(timestamp, city='Maricopa')
        successful_plots = 0
        for plotname in all_plots:
            bounds = all_plots[plotname]

            # Use GeoJSON string to clip full field to this plot
            (pxarray, geotrans) = clip_raster(resource['local_paths'][0], bounds)
            #tc = getFlir.rawData_to_temperature(pxarray, terramd) # get temperature
            mean_tc = numpy.mean(pxarray)

            # Create BETY-ready CSV
            (fields, traits) = get_traits_table()
            traits['surface_temperature'] = str(mean_tc)
            traits['site'] = plotname
            traits['local_datetime'] = timestamp+"T12-00-00-000"
            trait_list = generate_traits_list(traits)
            generate_csv(tmp_csv, fields, trait_list)

            # submit CSV to BETY
            submit_traits(tmp_csv, self.bety_key)

            # Prepare and submit datapoint
            centroid_lonlat = json.loads(centroid_from_geojson(bounds))["coordinates"]
            time_fmt = timestamp+"T12:00:00-07:00"
            dpmetadata = {s
                "source": host + ("" if host.endswith("/") else "/") + "files/" + resource['id'],
                "surface_temperature": str(mean_tc)
            }
            print("submitting datapoint for %s at %s" % (plotname, str(centroid_lonlat)))
            create_datapoint_with_dependencies(connector, host, secret_key, "IR Surface Temperature",
                                               (centroid_lonlat[1], centroid_lonlat[0]), time_fmt, time_fmt,
                                               dpmetadata, timestamp)

            successful_plots += 1

        os.remove(tmp_csv)

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        metadata = build_metadata(host, self.extractor_info, resource['parent']['id'], {
            "plots_processed": successful_plots,
            "plots_skipped": len(all_plots)-successful_plots,
            "betydb_link": "https://terraref.ncsa.illinois.edu/bety/api/beta/variables?name=surface_temperature"
        }, 'dataset')
        upload_metadata(connector, host, secret_key, resource['parent']['id'], metadata)

        self.end_message()

if __name__ == "__main__":
    extractor = FlirMeanTemp()
    extractor.start()
