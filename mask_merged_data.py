import os, sys
import numpy as np

from osgeo import gdal, osr

from shutil import copyfile

from scarputils import calculate_alpha_band, write_tiff

FLOAT32_MIN = np.finfo(np.float32).min

if __name__ == "__main__":

    #maxs = []
    #mins = []

    results_dir = 'results/'
    #for filename in os.listdir(results_dir):
    #    tile_name = filename[0:10]
    #    
    #    dem = gdal.Open('tif/' + tile_name + '.tif')
    #    elev = dem.ReadAsArray()
    #    nodata = np.logical_or(elev == FLOAT32_MIN, np.isnan(elev))

    #    raster = gdal.Open(results_dir + filename)
    #    data = raster.ReadAsArray()
    #    data[:, nodata] = np.nan
    #    mask = data[1] < 10
    #    data[:, mask] = np.nan

    #    maxs.append(np.nanmax(data[3]))
    #    mins.append(np.nanmin(data[3]))
    #
    #snr_max = np.array(maxs).max()
    #snr_min = np.array(mins).min()
    #print("SNR MAX: {:.2f} MIN: {:.2f}".format(snr_max, snr_min))

    remote_dir = '/media/rmsare/GALLIUMOS/data/ot_data/tif/'
    for filename in os.listdir(results_dir):
        tile_name = filename[0:10]
        print("processing {}".format(tile_name))
        
        if not os.path.exists('tif/' + tile_name + '.tif'):
            print("copying {}...".format(tile_name))
            copyfile(remote_dir + tile_name + '.tif', 'tif/' + tile_name + '.tif')

        dem = gdal.Open('tif/' + tile_name + '.tif')
        elev = dem.ReadAsArray()
        nodata = np.logical_or(elev == FLOAT32_MIN, np.isnan(elev))

        raster = gdal.Open(results_dir + filename)
        data = raster.ReadAsArray()
        data[:, nodata] = np.nan

        mask = data[1] < 10
        data[:, mask] = np.nan

        ang_average = 38 * (np.pi / 180)
        ang_tol = 20 * (np.pi / 180)
        mask = np.abs(data[2] - ang_average) > ang_tol
        data[:, mask] = np.nan

        alpha = calculate_alpha_band(data, 0, 500)

        #write_tiff('masked/amp_' + tile_name + '.tif', np.abs(data[0]), alpha, 'tif/' + tile_name + '.tif')
        #write_tiff('masked/kt_' + tile_name + '.tif', data[1], alpha, 'tif/' + tile_name + '.tif')
        #write_tiff('masked/ang_' + tile_name + '.tif', data[2]*(180/np.pi), alpha, 'tif/' + tile_name + '.tif')
        write_tiff('masked/snr_' + tile_name + '.tif', data[3], alpha, 'tif/' + tile_name + '.tif')
        


