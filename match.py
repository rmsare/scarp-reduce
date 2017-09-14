import os, sys
sys.path.append('/home/ubuntu/scarplet-python/scarplet')

import dem, scarplet
import numpy as np

from timeit import default_timer as timer
from Worker import Matcher, Reducer

if __name__ == "__main__":
    d = np.float(sys.argv[1])
    min_age = np.float(sys.argv[2])
    max_age = np.float(sys.argv[3])
    d_age = 0.1

    if min_age == max_age:
        ages = [min_age]
    else:
        num_ages = (max_age - min_age) / d_age
        ages = np.linspace(min_age, max_age, num_ages)

    local_data_directory = '/efs/data/'
    tiles = os.listdir(local_data_directory)

    for tile in tiles:
        worker = Matcher(local_data_directory + tile)
        worker.process(d, ages)
        
        
