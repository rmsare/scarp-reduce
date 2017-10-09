import os, sys
import logging

from launch_instances import launch_jobs
from s3utils import download_data, save_file_to_s3
from shutil import rmtree
from time import sleep

from datetime import datetime
from Worker import Reducer

def delete_local_files():
    rmtree('/efs/data')
    rmtree('/efs/results')
    os.mkdir('/efs/data')
    os.mkdir('/efs/results')

def reduce_current_data(num_files=35):
    # Launch Reducer instance
    logger.info("Starting Reducer")
    reducer = Reducer('/efs/results/')
    reducer.set_num_files(num_files)

    # Reduce results as they arrive
    reducer.reduce_all_results()

def upload_log():
    outfilename = 'scarplet-' + datetime.now().isoformat() + '.log' 
    save_file_to_s3('/efs/logs/scarp_reduce.log', outfilename, bucket='scarp-log')
    os.remove('/efs/logs/scarp_reduce.log')

if __name__ == "__main__":
    d = int(sys.argv[1])
    num_workers = int(sys.argv[2])
    remote_dir = sys.argv[3]
    batch_size = int(sys.argv[4])

    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('scarp_reduce')

    last_key = ''
    finished_processing = last_key is None
    while not finished_processing:
        logger.info("Downloading batch beginning with " + last_key)
        last_key = download_data(remote_dir, last_key=last_key, batch_size=batch_size)
        logger.info("Launching matching jobs")
        launch_jobs(d, num_workers)
        while len(os.listdir('/efs/results')) == 0:
            sleep(5)
        logger.info("Reducing current results")
        reduce_current_data(num_workers)
        logger.info("Deleting data and working files")
        delete_local_files()
        logger.info("Uploading log")
        upload_log()
        finished_processing = last_key is None

        


