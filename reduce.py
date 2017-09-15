import os, sys
sys.path.append('/home/ubuntu/scarplet-python/scarplet')
import dem, scarplet

from s3utils import save_file_to_s3

from timeit import default_timer as timer
from Worker import Matcher, Reducer


if __name__ == "__main__":
    tile_name = sys.argv[1]
    if tile_name[-1] is not '/':
        tile_name = tile_name +'/'

    num_files = int(sys.argv[2])

    # Launch Reducer instance
    start = timer()
    reducer = Reducer('/efs/results/' + tile_name)
    reducer.set_num_files(num_files)

    # Reduce results as they arrive
    reducer.reduce()
    stop = timer()
    print('Reduce:\t\t {:.2f} s'.format(stop-start))

    # Convert best results to TIFF

    # Save best results to S3
    filename = os.listdir('/efs/results/' + tile_name)[0]
    save_file_to_s3('/efs/results/' + tile_name + '/' + filename, tile_name + '/best_results.npy', bucket_name='scarp-testing')
