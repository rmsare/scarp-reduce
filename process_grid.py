import os, sys
sys.path.append('/home/ubuntu/scarplet-python/scarplet')
import dem, scarplet
from timeit import default_timer as timer
from Worker import Matcher, Reducer

# Divide grid into standard size chunks... 100 

# For each chunk:

# Save chunk on EFS volume in /efs/data/

# Pad data
source = '/efs/data/carrizo.tif'

# Divide up parameter space into k subsets
# Launch k Matcher instances
# Autoscaling group?
worker = Matcher(source)    

# Match templates
ages = [0]
start = timer()
worker.process(ages)
stop = timer()
print('Fits:\t\t {} s per template'.format((stop-start)/len(ages)))

# Launch Reducer instance
reducer = Reducer('/efs/results')

# Reduce results as they arrive
start = timer()
reducer.reduce()
stop = timer()
print('Reduce:\t\t {} s'.format(stop-start))

# Convert best results to TIFF

# Save best results to S3
