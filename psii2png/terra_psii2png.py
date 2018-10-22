#!/usr/bin/env python

import os
import numpy as np
from PIL import Image
from matplotlib import pyplot as plt

from pyclowder.utils import CheckMessage
from pyclowder.files import upload_to_dataset
from pyclowder.datasets import download_metadata, upload_metadata
from terrautils.extractors import TerrarefExtractor, is_latest_file, load_json_file, \
    build_metadata, build_dataset_hierarchy
from terrautils.metadata import get_extractor_metadata, get_terraref_metadata
from terrautils.formats import create_geotiff, create_image
from terrautils.spatial import geojson_to_tuples


class PSIIBin2Png(TerrarefExtractor):
    def __init__(self):
        super(PSIIBin2Png, self).__init__()

        # parse command line and load default logging configuration
        self.setup(sensor='ps2_png')

    def get_image_dimensions(self, metadata):
        """Returns (image width, image height)"""

        if 'sensor_fixed_metadata' in metadata:
            dims = metadata['sensor_fixed_metadata']['camera_resolution']
            return dims.split("x")
        else:
            # Default based on original fixed metadata
            return (1936, 1216)

    def load_png(self, file_path, height, width):
        """Load PNG image into a numpy array"""
        im = Image.open(file_path)
        return np.array(im).astype('uint8')

    def analyze(self, img_width, img_height, frames, hist_path, coloredImg_path):

        fdark = self.load_png(frames[0], img_height, img_width)
        fmin = self.load_png(frames[1], img_height, img_width)

        # Calculate the maximum fluorescence for each frame
        fave = []
        fave.append(np.max(fdark))
        # Calculate the maximum value for frames 2 through 100. Bin file 101 is an XML file that lists the frame times
        for i in range(2, 101):
            img = self.load_png(frames[i], img_height, img_width)
            fave.append(np.max(img))

        # Assign the first image with the most fluorescence as F-max
        fmax = self.load_png(frames[np.where(fave == np.max(fave))[0][0]], img_height, img_width)
        # Calculate F-variable (F-max - F-min)
        fv = np.subtract(fmax, fmin)
        # Calculate Fv/Fm (F-variable / F-max)
        if fmax.astype('float') == 0:
            fvfm = 0
        else:
            fvfm = np.divide(fv.astype('float'), fmax.astype('float'))
        # Fv/Fm will generate invalid values, such as division by zero
        # Convert invalid values to zero. Valid values will be between 0 and 1
        fvfm[np.where(np.isnan(fvfm))] = 0
        fvfm[np.where(np.isinf(fvfm))] = 0
        fvfm[np.where(fvfm > 1.0)] = 0

        # Plot Fv/Fm (pseudocolored)
        plt.imshow(fvfm, cmap="viridis")
        plt.savefig(coloredImg_path)
        plt.show()
        plt.close()

        # Calculate histogram of Fv/Fm values from the whole image
        hist, bins = np.histogram(fvfm, bins=20)
        # Plot Fv/Fm histogram
        width = 0.7 * (bins[1] - bins[0])
        center = (bins[:-1] + bins[1:]) / 2
        plt.bar(center, hist, align='center', width=width)
        plt.xlabel("Fv/Fm")
        plt.ylabel("Pixels")
        plt.show()
        plt.savefig(hist_path)
        plt.close()

    def check_message(self, connector, host, secret_key, resource, parameters):
        # Check for 0000-0101 bin files before beginning processing
        if len(resource['files']) < 102:
            self.log_skip(resource, "less than 102 files found")
            return CheckMessage.ignore
        if not is_latest_file(resource):
            self.log_skip(resource, "not latest file")
            return CheckMessage.ignore

        timestamp = resource['dataset_info']['name'].split(" - ")[1]
        hist_path = self.sensors.get_sensor_path(timestamp, opts=['combined_hist'])
        coloredImg_path = self.sensors.get_sensor_path(timestamp, opts=['combined_pseudocolored'])

        # Count number of bin files in dataset, as well as number of existing outputs
        ind_add = 0
        ind_output = 0
        for ind in range(0, 102):
            format_ind = "{0:0>4}".format(ind) # e.g. 1 becomes 0001
            for f in resource['files']:
                if f['filename'].endswith(format_ind+'.bin'):
                    ind_add += 1
                    out_png = self.sensors.get_sensor_path(timestamp, opts=[format_ind])
                    if os.path.exists(out_png) and not self.overwrite:
                        ind_output += 1
                    break

        # Do the outputs already exist?
        if ind_output == 102 and os.path.exists(hist_path) and os.path.exists(coloredImg_path):
            self.log_skip(resource, "outputs already exist")
            return CheckMessage.ignore
        # Do we have too few input BIN files?
        if ind_add < 102:
            self.log_skip(resource, "less than 102 .bin files found")
            return CheckMessage.ignore

        # Check metadata to verify we have what we need
        md = download_metadata(connector, host, secret_key, resource['id'])
        if get_extractor_metadata(md, self.extractor_info['name']) and not self.overwrite:
            self.log_skip(resource, "metadata indicates it was already processed")
            return CheckMessage.ignore
        if get_terraref_metadata(md):
            return CheckMessage.download
        else:
            self.log_skip(resource, "no terraref metadata found")
            return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message(resource)

        # Get bin files and metadata
        metadata = None
        for f in resource['local_paths']:
            # First check metadata attached to dataset in Clowder for item of interest
            if f.endswith('_dataset_metadata.json'):
                all_dsmd = load_json_file(f)
                metadata = get_terraref_metadata(all_dsmd, "ps2Top")
            # Otherwise, check if metadata was uploaded as a .json file
            elif f.endswith('_metadata.json') and f.find('/_metadata.json') == -1 and metadata is None:
                metadata = load_json_file(f)
        frames = {}
        for ind in range(0, 101):
            format_ind = "{0:0>4}".format(ind) # e.g. 1 becomes 0001
            for f in resource['local_paths']:
                if f.endswith(format_ind+'.bin'):
                    frames[ind] = f
        if None in [metadata] or len(frames) < 101:
            self.log_error(resource, 'could not find all of frames/metadata')
            return

        # Determine output directory
        timestamp = resource['dataset_info']['name'].split(" - ")[1]
        hist_path = self.sensors.create_sensor_path(timestamp, opts=['combined_hist'])
        coloredImg_path = self.sensors.create_sensor_path(timestamp, opts=['combined_pseudocolored'])
        uploaded_file_ids = []

        target_dsid = build_dataset_hierarchy(host, secret_key, self.clowder_user, self.clowder_pass, self.clowderspace,
                                              self.sensors.get_display_name(),
                                              timestamp[:4], timestamp[5:7], timestamp[8:10],
                                              leaf_ds_name=self.sensors.get_display_name()+' - '+timestamp)

        (img_width, img_height) = self.get_image_dimensions(metadata)
        gps_bounds = geojson_to_tuples(metadata['spatial_metadata']['ps2Top']['bounding_box'])

        self.log_info(resource, "image dimensions (w, h): (%s, %s)" % (img_width, img_height))

        png_frames = {}
        # skip 0101.bin since 101 is an XML file that lists the frame times
        for ind in range(0, 101):
            format_ind = "{0:0>4}".format(ind) # e.g. 1 becomes 0001
            png_path = self.sensors.create_sensor_path(timestamp, opts=[format_ind])
            tif_path = png_path.replace(".png", ".tif")
            png_frames[ind] = png_path
            if not os.path.exists(png_path) or self.overwrite:
                self.log_info(resource, "generating and uploading %s" % png_path)
                pixels = np.fromfile(frames[ind], np.dtype('uint8')).reshape([int(img_height), int(img_width)])
                create_image(pixels, png_path)
                create_geotiff(pixels, gps_bounds, tif_path, None, False, self.extractor_info, metadata)

                if png_path not in resource['local_paths']:
                    fileid = upload_to_dataset(connector, host, secret_key, target_dsid, png_path)
                    uploaded_file_ids.append(fileid)
                self.created += 1
                self.bytes += os.path.getsize(png_path)

        # Generate aggregate outputs
        self.log_info(resource, "generating aggregates")
        if not (os.path.exists(hist_path) and os.path.exists(coloredImg_path)) or self.overwrite:
            # TODO: Coerce histogram and pseudocolor to geotiff?
            self.analyze(int(img_width), int(img_height), png_frames, hist_path, coloredImg_path)
            self.created += 2
            self.bytes += os.path.getsize(hist_path) + os.path.getsize(coloredImg_path)
        if hist_path not in resource['local_paths']:
            fileid = upload_to_dataset(connector, host, secret_key, target_dsid, hist_path)
            uploaded_file_ids.append(fileid)
        if coloredImg_path not in resource['local_paths']:
            fileid = upload_to_dataset(connector, host, secret_key, target_dsid, coloredImg_path)
            uploaded_file_ids.append(fileid)

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        metadata = build_metadata(host, self.extractor_info, target_dsid, {
                                  "files_created": uploaded_file_ids}, 'dataset')
        self.log_info(resource, "uploading extractor metadata")
        upload_metadata(connector, host, secret_key, resource['id'], metadata)

        self.end_message(resource)

if __name__ == "__main__":
    extractor = PSIIBin2Png()
    extractor.start()
