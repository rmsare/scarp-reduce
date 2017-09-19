import os, sys
sys.path.append('/home/ubuntu/scarplet-python/scarplet')
import dem, scarplet

from timeit import default_timer as timer
from Worker import Matcher, Reducer


if __name__ == "__main__":

    num_files = int(sys.argv[1])
    # Launch Reducer instance
    reducer = Reducer('/efs/results/')
    reducer.set_num_files(num_files)

    # Reduce results as they arrive
    start = timer()
    reducer.reduce_all_results()
    stop = timer()
    print('Reduce:\t\t {:.2f} s'.format(stop-start))

    # Convert best results to TIFF

    # Save best results to S3
    #reducer.save_best_results()
