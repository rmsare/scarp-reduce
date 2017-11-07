import os, sys
import logging

from launch_instances import launch_jobs
from s3utils import download_data, save_file_to_s3
from shutil import rmtree
from time import sleep

from datetime import datetime
from Worker import Reducer

def delete_local_files():
    """
    Delete files in EFS file hierarchy
    """

    rmtree('/efs/data')
    rmtree('/efs/results')
    os.mkdir('/efs/data')
    os.mkdir('/efs/results')

if __name__ == "__main__":
    num_workers = int(sys.argv[1])
    remote_dir = sys.argv[2]
    batch_size = 20
    data_dir = '/efs/data/'

    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('scarp_reduce')

    last_key = 'fg484_4334.tif' 
    finished_processing = False
    while not finished_processing:
        files = os.listdir(data_dir)
        while len(files) < batch_size:
            logger.info("Downloading batch beginning with " + last_key)
            num_files = batch_size - len(files)
            last_key = download_data(remote_dir, last_key=last_key, batch_size=num_files)
            for f in os.listdir(data_dir):
                directory = f.strip('.tif')
                if not os.path.exists('/efs/results/' + directory):
                    os.mkdir('/efs/results/' + directory)
            files = os.listdir(data_dir)
        finished_processing = last_key is None
