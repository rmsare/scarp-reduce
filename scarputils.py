"""
Utilities for post-processing and spatial analysis of scarp template matching 
results
"""

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from scipy.ndimage import zoom
from osgeo import gdal, osr

from copy import copy

def calculate_local_swath_orientation(data):
    first = data[0]
    idx = np.where(~np.isnan(first))
    x0 = (idx.max() + idx.min()) / 2.
    y0 = 0
    v0 = np.array([x0, y0])

    last =  data[-1]
    idx = np.where(~np.isnan(last))
    x1 = (idx.max() + idx.min()) / 2.
    y1 = -(data.shape[0] - 1)
    v1 = np.array([x1, y1])
    trend = v1 - v0 / np.norm(v1 - v0)
    ex = np.array([1, 0])
    theta = np.acos(trend.dot(ex))

    return -(np.pi / 2 - theta)

def extract_swath_profile(data, nrow, ncol, length=1000, de=1, alpha=None):
    if not alpha:
        alpha = calculate_local_swath_orientation(data) + np.pi / 2

    lx = (length * de / 2) * np.cos(alpha)
    ly = (length * de / 2) * np.sin(alpha)
    x = np.linspace(ncol - lx, ncol + lx)
    y = np.linspace(nrow - ly, nrow + ly)

    profile = data[y, x]

def load_masked_results(data_filename, snr_filename):
    snr = gdal.Open(snr_filename)
    snr = snr.GetRasterBand(1).ReadAsArray()
    mask = snr > 100
    del snr

    data = gdal.Open(data_filename)
    data = data.GetRasterBand(1).ReadAsArray()
    data[data == 0] = np.nan
    data[~mask] = np.nan

    return data

def plot_polar_scatterplot(theta, data):
    colors = data
    ax = plt.subplot(111, projection='polar')
    c = ax.scatter(theta, data, c=colors, cmap='viridis', alpha=0.75)

def plot_polar_histogram(theta, data, nbins=20):
    #radii = 
    colors = radii
    ax = plt.subplot(111, projection='polar')
    c = ax.bars(theta, radii, c=colors, cmap='viridis', alpha=0.75)

def plot_violinplot(data):
    ax = plt.subplot(111)
    c = ax.violinplot(data, showmeans=true, showmedians=true)

def plot_distribution_ns(data, smoothing_length=None, de=1):
    nrows = data.shape[0]
    mean = np.zeros((nrows,))
    sd = np.zeros((nrows,))
    med = np.zeros((nrows,))

    for i, row in enumerate(data):
        mean[i] = np.nanmean(row)
        sd[i] = np.nanstd(row)
        med[i] = np.nanmedian(row)

    for param in mean, sd, med:
        param[np.isnan(param)] = 0

    if smoothing_length:
        n = float(smoothing_length / de)
        kern = (1 / n) * np.ones((int(n),))
        mean = np.convolve(mean, kern, mode='same')
        sd = np.convolve(sd, kern, mode='same')
        med = np.convolve(sd, kern, mode='same')

    fig = plt.figure()
    #ax = fig.add_subplot(211)
    #imdata = zoom(data, 0.25, order=0)
    #imdata[imdata == 9999] = np.nan
    #im = ax.imshow(np.flipud(np.rot90(np.rot90(imdata)).T), cmap='viridis', aspect='auto')
    #ax.tick_params(labelbottom='off', labelleft='off')
    #cbar = plt.colorbar(im, shrink=0.5, orientation='horizontal')
    #cbar.ax.set_xlabel('Amplitude [m]')

    ax = fig.add_subplot(212)
    x = de * np.arange(len(mean))
    ax.fill_between(x, med - sd, med + sd, color=[0.5, 0.5, 0.5], alpha=0.5)
    ax.plot(x, med, color=[1, 0, 0], alpha=0.75)

    ax.set_xlabel('Along-swath distance [m]')
    ax.set_ylabel('log$_{10}$($\kappa t$) [m$^2$]')
    ymax = (med + sd).max()
    #ax.set_ylim(0, ymax + 10)
    ax.set_xlim(0, x.max())

def mask_results(results, ang_average, ang_tol=20*(np.pi/180), amp_thresh=0.1, age_thresh=10):
    ang_mask = np.abs(results[2,:,:] - ang_average) > ang_tol
    amp_mask = np.abs(results[0,:,:]) <= amp_thresh
    age_mask = results[1,:,:] < age_thresh
    results[:, ang_mask] = np.nan
    results[:, amp_mask] = np.nan
    results[:, age_mask] = np.nan

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

