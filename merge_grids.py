import os, sys
import subprocess
import numpy as np
from osgeo import gdal
from itertools import product

sys.path.append('/home/rmsare/src/scarplet-python/scarplet')
import dem

def neighbors(filename, offset=2):
    x = int(filename[2:5])
    y = int(filename[6:10])
    offsets = product(np.arange(-offset + 1, offset), np.arange(-offset + 1, offset))
    filenames = [form_valid_filename(x + dx, y + dy) for dx, dy in offsets]
    return filenames

def form_valid_filename(x, y):
    return 'fg' + str(x) + '_' + str(y) + '.tif'

def pad_with_neighboring_values(filename, pad, data_dir='/media/rmsare/GALLIUMOS/data/ot_data/tif/2m/'):
    src_data = dem.DEMGrid(data_dir + filename)._griddata
    ny, nx = src_data.shape
    padded_shape = (ny + 2*pad, nx + 2*pad)
    dest_data = np.zeros(padded_shape)

    grids = neighbors(filename)
    for i, f in enumerate(grids):
        if i == 4:
            src_data = dem.DEMGrid(data_dir + filename)._griddata
            dest_data[pad:ny + pad, pad:nx + pad] = src_data
        else:
            if os.path.exists(data_dir + f):
                src_data = dem.DEMGrid(data_dir + f)._griddata
            else:
                src_data = np.nan * np.ones((ny, nx))

            if i == 0: # SW corner
                pad_values = src_data[0:pad, -pad:]
                dest_data[pad + ny:2*pad + ny, 0:pad] = pad_values 
            elif i == 1: # W edge 
                pad_values = src_data[:, -pad:]
                dest_data[pad:nx + pad, 0:pad] = pad_values 
            elif i == 2: # NW corner
                pad_values = src_data[-pad:, -pad:]
                dest_data[0:pad, 0:pad] = pad_values 
            elif i == 3: # S edge 
                pad_values = src_data[0:pad, :]
                dest_data[pad + ny:2*pad + ny, pad:pad + nx] = pad_values 
            elif i == 5: # N edge 
                pad_values = src_data[-pad:, :]
                dest_data[0:pad, pad:pad + nx] = pad_values 
            elif i == 6: # SE corner
                pad_values = src_data[0:pad, 0:pad]
                dest_data[pad + ny:2*pad + ny, pad + nx:2*pad + nx] = pad_values 
            elif i == 7: # E edge 
                pad_values = src_data[:, 0:pad]
                dest_data[pad:pad + ny, pad + nx:2*pad + nx] = pad_values 
            elif i == 8: # NE corner
                pad_values = src_data[-pad:, 0:pad]
                dest_data[0:pad, pad + ny: 2*pad + ny] = pad_values 

    return dest_data

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
