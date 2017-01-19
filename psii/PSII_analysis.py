'''
core script of psii system
Created on Jan 12, 2017

@author: Zongyang
'''

import os, sys, json
import numpy as np
from PIL import Image
from matplotlib import pyplot as plt



def main():
    
    in_dir = '/Users/nijiang/Desktop/pythonTest/PSII_clowderTest/t1'
    hist_path = os.path.join(in_dir, 'hist.png')
    map_path = os.path.join(in_dir, 'imgMap.png')
    
    frames = {}
    for i in range(0, 101):
        file_ends = "{0:0>4}".format(i)+'.png'
        frames[i] = os.path.join(in_dir, '0e568025-5a74-43e0-9567-45b02310f6f5_rawData'+file_ends)
    
    psii_analysis(frames, hist_path, map_path)
    
    
    #create_ps2_images(in_dir)
    
    return

def create_ps2_images(in_dir):
    
    list_dirs = os.walk(in_dir)
    
    out_dir = os.path.join(in_dir, 'images')
    
    os.mkdir(out_dir)
    
    img_width = 1936
    img_height = 1216
    
    for root, dirs, files in list_dirs:
        for file_path in files:
            if not file_path.endswith('.bin'):
                continue
            
            if file_path.endswith('0101.bin'):
                continue
            
            input_path = os.path.join(in_dir, file_path)
            out_path = os.path.join(out_dir, file_path)
            out_path = out_path[:-3] + 'png'
            load_PSII_data(input_path, img_height, img_width, out_path)
            
    return

def load_PSII_data(file_path, height, width, out_file):
    
    try:
        im = np.fromfile(file_path, np.dtype('uint8')).reshape([height, width])
        Image.fromarray(im).save(out_file)
        return im.astype('u1')
    except Exception as ex:
        fail('Error processing image "%s": %s' % (file_path,str(ex)))
        
def load_PSII_png(file_path, height, width):
    
    try:
        im = Image.open(file_path)
        pix = np.array(im).astype('uint8')
        return pix
    except Exception as ex:
        fail('Error loading image "%s": %s' % (file_path,str(ex)))
        
def psii_analysis(frames, hist_path, coloredImg_path):
    
    img_width = 1936
    img_height = 1216
    fdark = load_PSII_png(frames[0], img_height, img_width)
    fmin = load_PSII_png(frames[1], img_height, img_width)
    
    # Calculate the maximum fluorescence for each frame
    fave = []
    fave.append(np.max(fdark))
    # Calculate the maximum value for frames 2 through 100. Bin file 101 is an XML file that lists the frame times
    for i in range(2, 101):
        img = load_PSII_png(frames[i], img_height, img_width)
        fave.append(np.max(img))
    
    # Assign the first image with the most fluorescence as F-max
    fmax = load_PSII_png(frames[np.where(fave == np.max(fave))[0][0]], img_height, img_width)
    # Calculate F-variable (F-max - F-min)
    fv = np.subtract(fmax, fmin)
    # Calculate Fv/Fm (F-variable / F-max)
    fvfm = np.divide(fv.astype('float'), fmax.astype('float'))
    # Fv/Fm will generate invalid values, such as division by zero
    # Convert invalid values to zero. Valid values will be between 0 and 1
    fvfm[np.where(np.isnan(fvfm))] = 0
    fvfm[np.where(np.isinf(fvfm))] = 0
    fvfm[np.where(fvfm > 1.0)] = 0
    
    fig, ax = plt.subplots()
    
    # Plot Fv/Fm (pseudocolored)
    plt.imshow(fvfm, cmap="viridis")
    plt.show()
    plt.savefig(coloredImg_path)
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
    
    return

def fail(reason):
    print >> sys.stderr, reason
    
def load_json(meta_path):
    try:
        with open(meta_path, 'r') as fin:
            return json.load(fin)
    except Exception as ex:
        fail('Corrupt metadata file, ' + str(ex))
        
def lower_keys(in_dict):
    if type(in_dict) is dict:
        out_dict = {}
        for key, item in in_dict.items():
            out_dict[key.lower()] = lower_keys(item)
        return out_dict
    elif type(in_dict) is list:
        return [lower_keys(obj) for obj in in_dict]
    else:
        return in_dict

if __name__ == "__main__":

    main()