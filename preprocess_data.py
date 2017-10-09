import os, sys
import numpy as np 
import matplotlib.pyplot as plt

sys.path.append('/home/rmsare/src/scarplet-python/scarplet')
import dem, scarplet

from merge_grids import pad_with_neighboring_values
from s3utils import download_data_from_s3, save_file_to_s3 

from timeit import default_timer as timer

PAD = 200

if __name__ == "__main__":
    #local_data_directory = '/media/rmsare/GALLIUMOS/data/ot_data/tif/'
    #local_mask_directory = '/media/rmsare/GALLIUMOS/data/ot_data/mask/'
    remote_data_directory = 'testing/hayward/'
    local_data_directory ='/home/rmsare/r/scarplet/tif/'

    bucket_name = 'scarp-data'

    for tile in os.listdir(local_data_directory)[0:50]:
        tile_name = tile.split('/')[-1][:-4]
        data = dem.DEMGrid(local_data_directory + tile)

        print("masking and padding " + tile_name + "...")
        data._griddata = pad_with_neighboring_values(tile, PAD)
        
        ny, nx = data._griddata.shape
        data._georef_info.nx = nx
        data._georef_info.ny = ny

        data._georef_info.xllcenter -= PAD
        data._georef_info.yllcenter -= PAD
        x_min = data._georef_info.geo_transform[0] - PAD * data._georef_info.dx
        y_max = data._georef_info.geo_transform[3] + PAD * np.abs(data._georef_info.dy)
        new_transform = (x_min, data._georef_info.dx, 0, y_max, 0, -data._georef_info.dy)
        data._georef_info.geo_transform = new_transform

        data._fill_nodata()

        print ("uploading...")
        data.save(local_data_directory + tile)
        save_file_to_s3(local_data_directory + tile, remote_data_directory + tile, bucket_name=bucket_name)

