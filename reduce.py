import os, sys
sys.path.append('/home/ubuntu/scarplet-python/scarplet')
import dem, scarplet

import logging

from timeit import default_timer as timer
from Worker import Matcher, Reducer


if __name__ == "__main__":

    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('scarp_reduce')

    num_files = int(sys.argv[1])

    # Launch Reducer instance
    logger.info("Starting Reducer")
    reducer = Reducer('/ef/results/')
    reducer.set_num_files(num_files)

    # Reduce results as they arrive
    reducer.reduce_all_results()

    # Convert best results to TIFF

    # Save best results to S3
    logger.info("Saving best reduced results")
    reducer.save_results()
