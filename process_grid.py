import os, sys
sys.path.append('/home/ubuntu/scarplet-python/scarplet')
import dem, scarplet
from timeit import default_timer as timer
from Worker import Matcher, Reducer

PAD_DX = 250
PAD_DY = 250

# Divide grid into standard size chunks... 

# For each chunk:

# Save chunk on EFS volume in /efs/data/
# upload_data()

# Pad data
#current_tile = tiles.pop()
current_tile = '/efs/data/carrizo.tif'
tile_name = current.tile.split('/')[-1][:-4]
os.mkdir('/efs/results/' + tile_name)

data = dem.DEMGrid(current_tile)
data._pad_data_boundary(PAD_DX, PAD_DY)
data.save(current_tile)

# Divide up parameter space into k subsets
# Launch k Matcher instances
# Autoscaling group?
num_workers = 1
worker = Matcher(current_tile)    

# Match templates
d = 100
ages = [0, 10]
start = timer()
worker.process(d, ages)
stop = timer()
print('Fits:\t\t {} s per template'.format((stop-start)/len(ages)))

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
