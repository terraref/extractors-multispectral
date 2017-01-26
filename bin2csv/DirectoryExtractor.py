# -*- coding: utf-8 -*-
"""
Extract NDVI or PRI from .bin file and Save to .csv file.
"""
import csv
import glob
import numpy as np

# obtain files with extention '.bin'
bin_filenames = glob.glob('*.bin')
num_binfile = len(bin_filenames)
NDVI_val = np.zeros(num_binfile)
if num_binfile>0:
    print ".bin file exists"
    for bf in range(num_binfile):
        print "Processing bin file %d" % bf
        # open .bin file
        data = open(bin_filenames[bf], "rb").read()
        # read NDVI from file
        NDVI_val[bf] = float(data[49:66])

with open('NDVI_info.csv','wb') as csvfile:
    fields = ['file_name', 'NDVI'] # fields name for csv file
    wr = csv.DictWriter(csvfile, fieldnames=fields, lineterminator = '\n')
    wr.writeheader()
    for bf in range(num_binfile):
        wr.writerow({'file_name':bin_filenames[bf], 'NDVI': NDVI_val[bf]})
