#!/usr/bin/env python

import numpy
import json

from pyclowder.utils import CheckMessage
from pyclowder.datasets import download_metadata, upload_metadata
from terrautils.metadata import get_extractor_metadata, get_terraref_metadata
from terrautils.extractors import TerrarefExtractor, is_latest_file, calculate_scan_time, \
    calculate_gps_bounds, build_dataset_hierarchy, create_geotiff, build_metadata, \
    create_image, load_json_file
from terrautils.gdal import centroid_from_geojson, clip_raster
from terrautils.betydb import add_arguments, submit_traits, get_site_boundaries
from terrautils.geostreams import create_datapoint_with_dependencies

import Get_FLIR as getFlir


def add_local_arguments(parser):
    # add any additional arguments to parser
    add_arguments(parser)

def get_traits_table():
    # Compiled traits table
    fields = ('local_datetime', 'canopy_cover', 'access_level', 'species', 'site',
              'citation_author', 'citation_year', 'citation_title', 'method')
    traits = {'local_datetime' : '',
              'avg_temp' : [],
              'access_level': '2',
              'species': 'Sorghum bicolor',
              'site': [],
              'citation_author': '"Zongyang, Li"',
              'citation_year': '2016',
              'citation_title': 'Maricopa Field Station Data and Metadata',
              'method': 'Mean temperature from infrared images'}

def generate_traits_list(traits):
    # compose the summary traits
    trait_list = [  traits['local_datetime'],
                    traits['avg_temp'],
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
        if resource['name'].find('fullfield') > -1:
            return CheckMessage.download

        return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message()

        tmp_csv = "meantemptraits.csv"

        # Get full list of experiment plots using date as filter
        ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
        dsmd = download_metadata(connector, host, secret_key, resource['parent']['id'])
        terramd = get_terraref_metadata(dsmd, 'flirIrCamera')
        timestamp = ds_info['name'].split(" - ")[1]
        all_plots = get_site_boundaries(timestamp, city='Maricopa')
        # TODO: Get dataset metadata


        for plotname in all_plots:
            bounds = all_plots[plotname]

            # Use GeoJSON string to clip full field to this plot
            (pxarray, geotrans) = clip_raster(resource['local_paths'][0], json.dumps(bounds))
            tc = getFlir.rawData_to_temperature(pxarray, scan_time, terramd) # get temperature
            # TODO: Get mean of tc after clipping by plot
            mean_tc = numpy.mean(tc)

            # Create BETY-ready CSV
            (fields, traits) = get_traits_table()
            traits['avg_temp'] = str(mean_tc)
            traits['site'] = plotname
            traits['local_datetime'] = timestamp+"T12-00-00-000"
            trait_list = generate_traits_list(traits)
            generate_csv(tmp_csv, fields, trait_list)

            # submit CSV to BETY
            submit_traits(tmp_csv, self.bety_key)

            # Prepare and submit datapoint
            centroid = centroid_from_geojson(bounds)
            time_fmt = timestamp+"T12:00:00-07:00"
            dpmetadata = {
                "source": host+"files/"+resource['id'],
                "canopy_cover": mean_tc
            }
            create_datapoint_with_dependencies(connector, host, secret_key, "IR Average Temperature",
                                               centroid, time_fmt, time_fmt, dpmetadata, timestamp)

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        metadata = build_metadata(host, self.extractor_info, resource['parent']['id'], {
            "plots_processed": len(all_plots)
            # TODO: add link to BETY trait IDs
        }, 'dataset')
        upload_metadata(connector, host, secret_key, resource['parent']['id'], metadata)

        self.end_message()

if __name__ == "__main__":
    extractor = FlirMeanTemp()
    extractor.start()
