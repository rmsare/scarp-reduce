from Worker import CacheProcessor, Matcher, TreeReducer

# Divide grid into standard size chunks... 100 

# For each chunk:

# Save chunk on EFS volume in /efs/data/

# Pad data
source = '/efs/data/carrizo.tif'
data = dem.DEMGrid(source)
data._pad_bpoundary(PAD_DX, PAD_DY)
print('Data size: {}'.format(data._griddata.shape))

# Divide up parameter space into k subsets
# Launch k Matcher instances
# Autoscaling group?
worker = Matcher(source)    

# Match templates
ages = [0, 1]
worker.process_data(ages)

# Launch Reducer instance

# Reduce results as they arrive

# Convert best results to TIFF

# Save best results to S3
