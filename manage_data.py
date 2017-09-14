import os, sys
sys.path.append('/home/ubuntu/scarplet-python/scarplet')
import dem, scarplet

from s3utils import download_data_from_s3, list_dir_s3

from timeit import default_timer as timer
from Worker import Matcher, Reducer

PAD_DX = 250
PAD_DY = 250

if __name__ == "__main__":
    remote_data_directory = 'ot-ncal'
    local_data_directory = '/efs/data/'
    bucket_name = 'scarp-data'
    tiles = list_dir_s3(data_directory, bucket_name)

    for tile in tiles:
        download_data_from_s3(remote_data_directory + '/' + tile, local_data_directory + tile, bucket_name)
        tile_name = tile.split('/')[-1][:-4]
        os.mkdir('/efs/results/' + tile_name)

        data = dem.DEMGrid(local_data_directory + tile)
        data._pad_data_boundary(PAD_DX, PAD_DY)
        data.save(local_data_directory + tile)
