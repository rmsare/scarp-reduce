# scarp-reduce
Sandbox for map-reduce style distributed template matching. Very much a work in progress, not for general consumption yet.

### Requirements
- Python 2.7
- The usual suspects (`pip install -r requirements.txt`)
- [scarplet-python](https://github.com/rmsare/scarplet-python)
- AWS EC2, S3, EFS, CloudWatch

### Core functionality:
- `Worker.py`: classes for matcher and reducer instances
- `match.py`: Start and maintain template matching worker
- `reduce.py`: Initialize reducer that reduces working results directory, then exits
- `reduce_loop.py`: Start reducer that reduces all intermediate results, until working data directory is empty

### Utilities:
- `launch_instaces.py`: Various conveience functions for AWS EC2 instance management
- `monitor.py`: Monitors and restarts idle instances
- `manage_data.py`: Fetches tiles from S3 bucket in batches. Fetches entire bucket, or subset with specified starting file.

### Misc.
Also contains various utilities for copying files in bounding box, tiling a large GeoTIFF dataset, and padding tiles. 

All processing currently relies on a filename convention based on UTM coordinates of tile bounding box, e.g. `fg396_4508.tif`. This is based on EarthScope survey naming conventions:

`ccXXX_YYYY.fmt`

where:  
- `cc` is a data code (`u` for unfiltered, `fg` for filtered ground returns only, etc)
- `XXX` and `YYYY` are the most siginificant digits of the dataset's lower left corner (XXX000, YYYY000) in UTM coordinate system. In this case, I work in UTM zone 10N.
