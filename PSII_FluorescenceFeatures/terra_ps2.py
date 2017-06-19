#!/usr/bin/env python

"""
terra.ps2.py
This extractor will trigger when
"""

import os
import logging
import subprocess
import tempfile

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets



class heightmap(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)
        print "\n 0 \n"


    # Check whether dataset already has metadata
    def check_message(self, connector, host, secret_key, resource, parameters):
       
	input_ = None
	

        # Count number of bin files in dataset, as well as number of existing outputs
        ind_add = 0
	input_ = None


        for ind in range(0, 102 ):
            format_ind = "{0:0>4}".format(ind) # e.g. 1 becomes 0001
            for f in resource['files']:
                if f['filename'].endswith(format_ind+'.bin'):
                    ind_add += 1
		    
        # Do we have too few input BIN files?
        if ind_add < 102 :
            return CheckMessage.ignore
	else:
	    return CheckMessage.download
	#out_dir = input_.replace(os.path.basename(input_), "")
        #out_name = resource['name'] 
        #out_jpg = os.path.join(out_dir, out_name)

        #if os.path.exists(out_jpg): #ind_output == 9
	 #   logging.info("skipping dataset %s, outputs already exist" % resource['id'])
          #  return CheckMessage.ignore

	return CheckMessage.ignore

##################################################################################################################################3

                   
    def process_message(self, connector, host, secret_key, resource, parameters):
        	
        input_ = None
        for p in resource['local_paths']:
            if p.endswith(".bin"):
                input_ = p


	def replace_right(source, target, replacement, replacements=None):
    		return replacement.join(source.rsplit(target, replacements))
 

	# Create output in same directory as input, but check name
        out_dir = replace_right(input_,os.path.basename(input_),"",1)
	out_split = out_dir.split(os.sep)
	out_dir= replace_right(out_dir,out_split[-2],"",1)
	out_dir= replace_right(out_dir,"/","",1)
        out_name = "outputname"
        out_jpg = os.path.join(out_dir, out_name+".jpg")
	out_dir_light= out_dir


        subprocess.call(['git clone https://github.com/solmazhajmohammadi/PSII.git '], shell=True)
        #os.chdir('PSII')
	subprocess.call(['cp -rT /home/extractor/PSII .'], shell= True)
	subprocess.call(['chmod 777 PSII.m'], shell=True)
        #subprocess.call(['octave pkg install -forge image'],shell=True)
        subprocess.call(["octave --eval \"PSII(\'%s\',\'%s\' ,\'%s\')\"" % (out_dir, out_dir_light,out_name)],shell=True);

	
        if os.path.isfile(out_name +"_Fm_dark.jpg"):
	    logging.info("uploading %s to dataset" % out_jpg)
	    pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], out_name+"_Fm_dark.jpg")
            pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], out_name+"_Fv_dark.jpg")
            pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], out_name+"_FvFm_dark.jpg")
            pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], out_name+"_Fm_light.jpg")
            pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], out_name+"_Fv_dark.jpg")
            pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], out_name+"_FvFm_light.jpg")
            pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], out_name+"_Phi_PSII.jpg")
            pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], out_name+"_NPQ.jpg")
            pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], out_name+"_qN.jpg")
            pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], out_name+"_qP.jpg")
            pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], out_name+"_Rfd.jpg")

       

if __name__ == "__main__":
    extractor = heightmap()
extractor.start()
