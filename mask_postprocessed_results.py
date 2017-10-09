import os, sys
import numpy as np

from osgeo import gdal, osr

from shutil import copyfile

from scarputils import calculate_alpha_band, write_tiff

FLOAT32_MIN = np.finfo(np.float32).min

if __name__ == "__main__":

    results_dir = '/home/rmsare/r/scarplet/raw/'
    working_dir = '/home/rmsare/r/scarplet/masked/'
    remote_dir = '/media/rmsare/GALLIUMOS/data/ot_data/tif/2m/'
    files = os.listdir(results_dir)
    #files.remove('masked')
    for filename in files:
        tile_name = filename[0:10]
        print("processing {}".format(tile_name))
        
        raster = gdal.Open(results_dir + filename)
        data = raster.ReadAsArray()

        mask = data[1] < 10
        data[:, mask] = np.nan
        
        #mask = data[3] < 100
        #data[:, mask] = np.nan

        ang_average = 38 * (np.pi / 180)
        ang_tol = 20 * (np.pi / 180)
        mask = np.abs(data[2] - ang_average) > ang_tol
        data[:, mask] = np.nan

        mask = np.isnan(data[0])
        data[:, mask] = -9999 

        alpha = calculate_alpha_band(data, 100, 500)

        write_tiff(working_dir + 'amp_' + tile_name + '.tif', np.abs(data[0]), alpha, remote_dir + tile_name + '.tif')
        write_tiff(working_dir + 'kt_' + tile_name + '.tif', data[1], alpha, remote_dir + tile_name + '.tif')
        write_tiff(working_dir + 'ang_' + tile_name + '.tif', data[2]*(180/np.pi), alpha, remote_dir + tile_name + '.tif')
        write_tiff(working_dir + 'snr_' + tile_name + '.tif', data[3], alpha, remote_dir + tile_name + '.tif')
        


