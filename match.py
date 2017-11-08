import os, sys
sys.path.append('/home/ubuntu/scarplet-python/scarplet')

import dem, scarplet
import numpy as np
import logging

from timeit import default_timer as timer
from Worker import Matcher, Reducer

if __name__ == "__main__":

    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('scarp_reduce')

    d = np.float(sys.argv[1])
    age = np.float(sys.argv[2])
    pad_dx = int(sys.argv[3])
    pad_dy = int(sys.argv[4])
    ages = [age]

    local_data_directory = '/efs/data/'
    tiles = os.listdir(local_data_directory)
    local_results_directory = '/efs/results/'

    logger.info("Starting Matcher for {} {:.2f}".format(d, ages[0]))

    files = os.listdir(local_data_directory)
    np.random.shuffle(files)
    finished_processing = False
    while not finished_processing:
        for tile in files:
            logger.debug("Starting Matcher for {}, {} {:.2f}".format(tile, d, ages[0]))
            worker = Matcher(local_data_directory + tile, pad_dx, pad_dy) 
            worker.process(d, ages)
            logger.debug("Finished processing {}, {} {:.2f}".format(tile, d, ages[0]))
        files = os.listdir(local_data_directory)
        np.random.shuffle(files)
        finished_processing = len(files) == 0
