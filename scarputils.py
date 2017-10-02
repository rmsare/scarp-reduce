"""
Utilities for post-processing and spatial analysis of scarp template matching 
results
"""

import os
import numpy as np

from osgeo import gdal, osr

from copy import copy


def mask_results(results, ang_average, ang_tol=20*(np.pi/180), amp_thresh=0.1, age_thresh=10):
    ang_mask = np.abs(results[2,:,:] - ang_average) > ang_tol
    amp_mask = np.abs(results[0,:,:]) <= amp_thresh
    age_mask = results[1,:,:] < age_thresh
    results[:, ang_mask] = np.nan
    results[:, amp_mask] = np.nan
    results[:, aage_mask] = np.nan

    snr_thresh = np.median(results[3,:,:])
    snr_mask = results[3,:,:] <= snr_thresh

def calculate_alpha_band(results, snr_min, snr_max=1000):
    snr = copy(results[3])
    if snr_max < np.nanmax(snr): 
        snr[snr > snr_max] = snr_max
    alpha = (snr - snr_min) / (snr_max - snr_min)
    return alpha

def write_tiff(filename, array, alpha, data_file):
    nbands = 2 
    nrows, ncols = array.shape

    inraster = gdal.Open(data_file)
    transform = inraster.GetGeoTransform()
    
    driver = gdal.GetDriverByName('GTiff')
    outraster = driver.Create(filename, ncols, nrows, nbands, gdal.GDT_Float32)
    outraster.SetGeoTransform(transform)

    out_band = outraster.GetRasterBand(1)
    out_band.WriteArray(array)
    out_band.SetNoDataValue(np.nan)
    out_band.FlushCache()
    
    out_band = outraster.GetRasterBand(2)
    out_band.WriteArray(alpha)
    out_band.SetNoDataValue(np.nan)
    out_band.FlushCache()
    
    srs = osr.SpatialReference()
    srs.ImportFromWkt(inraster.GetProjectionRef())
    outraster.SetProjection(srs.ExportToWkt())

