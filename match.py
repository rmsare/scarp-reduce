import os, sys
sys.path.append('/home/ubuntu/scarplet-python/scarplet')

import dem, scarplet
import numpy as np
import logging

from timeit import default_timer as timer
from Worker import Matcher, Reducer

if __name__ == "__main__":

    logger = logging.getLogger(__name__)

    d = np.float(sys.argv[1])
    age = np.float(sys.argv[2])
    ages = [age]
   # min_age = np.float(sys.argv[2])
   # max_age = np.float(sys.argv[3])
   # d_age = 0.1

   # if min_age == max_age:
   #     ages = [min_age]
   # else:
   #     num_ages = int((max_age - min_age) / d_age)
   #     ages = np.linspace(min_age, max_age, num_ages)

    local_data_directory = '/efs/data/'
    tiles = os.listdir(local_data_directory)
    local_results_directory = '/efs/results/'
    for tile in tiles:
        result_dir = local_results_directory + tile
        if not os.path.exists(result_dir):
            os.mkdir(result_dir)

    for tile in tiles:
        logger.info("Starting Matcher for {}".format(tile))
        worker = Matcher(local_data_directory + tile)
        worker.process(d, ages)
        logger.info("Finished processing {}".format(tile))
        
        
