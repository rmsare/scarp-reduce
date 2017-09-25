import os, sys
import numpy as np
from osgeo import osr, gdal

def save_tiff(array, filename, data_dir='/efs/data/', results_dir='efs/results/'):
    filename = filename[:-4] + '.tif'
    nbands, nrows, ncols = array.shape

    tile = data_dir + filename.split('results')[0][:-1] + '.tif'
    inraster = gdal.Open(tile)
    transform = inraster.GetGeoTransform()
    
    driver = gdal.GetDriverByName('GTiff')
    outraster = driver.Create(results_dir + filename, ncols, nrows, nbands, gdal.GDT_Float32)
    outraster.SetGeoTransform(transform)

    for i in range(nbands):
        out_band = outraster.GetRasterBand(i+1)
        out_band.WriteArray(array[i])
        out_band.FlushCache()
    
    srs = osr.SpatialReference()
    srs.ImportFromWkt(inraster.GetProjectionRef())
    outraster.SetProjection(srs.ExportToWkt())
