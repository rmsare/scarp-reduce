import os, sys
import subprocess
import numpy as np
from osgeo import gdal
from itertools import product

def neighbors(filename, length=2):
    x = int(filename[2:5])
    y = int(filename[6:10])
    offsets = product(np.arange(length), np.arange(length))
    filenames = [form_valid_filename(x + dx, y + dy) for dx, dy in offsets]
    return filenames

def form_valid_filename(x, y):
    return 'fg' + str(x) + '_' + str(y) + '.tif'

def sort_by_utm_northing(filenames):
    """
    Sorts list of grid files by lower left UTM coordinates 
    in descending order.

    Geographically northeasternmost grids come first.
    """

    coords = [(int(f[2:5]), int(f[6:10])) for f in filenames]
    NE = np.array([(lly, -llx) for llx, lly in coords], dtype=[('y', '>i4'), ('-x', '>i4')])
    idx = np.argsort(NE, order=('y', '-x'))[::-1]
    filenames = np.asarray(filenames)

    return list(filenames[idx])

if __name__ == "__main__":
    source_dir = '/media/rmsare/GALLIUMOS/data/ot_data/tif/1m/'
    dest_dir = '/media/rmsare/GALLIUMOS/data/ot_data/merged_2km/'

    files = os.listdir(source_dir)
    files = sort_by_utm_northing(files)

    for f in files:
        grids = neighbors(f)
        grids = set.intersection(set(grids), set(files))
        if len(grids) > 1:
            file_args = [source_dir + nb for nb in grids]
            command = ['gdal_merge.py', '-o', dest_dir + f, '-of', 'GTiff', '-init', 'nan']
            command.extend(file_args)

            print("Merging {}".format(f))
            subprocess.call(command)

            #for x in grids:
            #   files.remove(x)

    print("Deleting swath edge tiles...")
    for f in os.listdir(dest_dir):
        raster = gdal.Open(dest_dir + f)
        not_square = raster.RasterXSize != raster.RasterYSize
        too_small = raster.RasterXSize < 2000 or raster.RasterYSize < 2000
        if not_square or too_small:
            os.remove(dest_dir + f)
