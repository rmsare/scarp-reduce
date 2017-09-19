import os, sys
import numpy as np 

sys.path.append('/home/rmsare/src/scarplet-python/scarplet')
import dem, scarplet


from s3utils import download_data_from_s3, save_file_to_s3 

from timeit import default_timer as timer
from Worker import Matcher, Reducer


if __name__ == "__main__":
    #local_data_directory = '/media/rmsare/GALLIUMOS/data/ot_data/tif/'
    #local_mask_directory = '/media/rmsare/GALLIUMOS/data/ot_data/mask/'
    remote_data_directory = 'ot-ncal/'
    local_data_directory ='tif/'

    bucket_name = 'scarp-data'

    for tile in os.listdir(local_data_directory):
        tile_name = tile.split('/')[-1][:-4]
        print("masking " + tile_name + "...")

        data = dem.DEMGrid(local_data_directory + tile)
        data._fill_nodata()
        data.save(local_data_directory + tile)
        save_file_to_s3(local_data_directory + tile, remote_data_directory + 'tif/' + tile, bucket_name=bucket_name)

        print ("uploading...")
        mask_filename = tile.split('.')[0] + '_mask.npy'
        np.save('mask/' + mask_filename, data.nodata_mask)
        save_file_to_s3('mask/' + mask_filename, remote_data_directory + 'mask/' + mask_filename, bucket_name=bucket_name)
        os.remove('mask/' + mask_filename)
