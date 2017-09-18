import os, sys
sys.path.append('/home/ubuntu/scarplet-python/scarplet')
import dem, scarplet

from s3utils import download_data_from_s3, list_dir_s3

from timeit import default_timer as timer
from Worker import Matcher, Reducer


if __name__ == "__main__":
    remote_data_directory = 'ot-ncal-test'
    local_data_directory = '/efs/data/'
    local_mask_directory = '/efs/masks/'
    bucket_name = 'scarp-data'
    tiles = list_dir_s3(remote_data_directory, bucket_name)

    for tile in tiles:
        download_data_from_s3(remote_data_directory + '/' + tile, local_data_directory + tile, bucket_name)
        tile_name = tile.split('/')[-1][:-4]
        os.mkdir('/efs/results/' + tile_name)

        data = dem.DEMGrid(local_data_directory + tile)
        data._fill_nodata()
        data.save(local_data_directory + tile)
        np.save(local_mask_directory + tile.split('.')[0] + '_mask.npy', data.nodata_mask)
