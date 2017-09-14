import os, sys
sys.path.append('/home/ubuntu/scarplet-python/scarplet')
import dem, scarplet

from timeit import default_timer as timer
from Worker import Matcher, Reducer


if __name__ == "__main__":
    tile_name = sys.argv[1]
    # Launch Reducer instance
    reducer = Reducer('/efs/results/' + tile_name)
    reducer.set_num_workers(num_workers)

    # Reduce results as they arrive
    start = timer()
    reducer.reduce()
    stop = timer()
    print('Reduce:\t\t {} s'.format(stop-start))

    # Convert best results to TIFF

    # Save best results to S3
