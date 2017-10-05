import os, sys
sys.path.append('/home/rmsare/src/scarplet-python/scarplet/')
import dem

import numpy as np
from osgeo import gdal, osr

from s3utils import save_tiff

if __name__ == "__main__":
    files = os.listdir('results/')
    files = [f for f in files if 'tif' in f]
    for f in files:
        print("processing {}...".format(f))
        tile_name = f[0:10]
        data = dem.DEMGrid('/media/rmsare/GALLIUMOS/data/ot_data/merged_2km/2m/' + tile_name + '.tif')
        mask = np.isnan(data._griddata)
        
        inraster = gdal.Open('results/' + f)
        transform = inraster.GetGeoTransform()
        nbands = inraster.RasterCount
        ncols = inraster.RasterXSize
        nrows = inraster.RasterYSize
        
        driver = gdal.GetDriverByName('GTiff')
        outraster = driver.Create('masked/' + f, ncols, nrows, nbands, gdal.GDT_Float32)
        outraster.SetGeoTransform(transform)

        array = inraster.ReadAsArray()
        array[:, mask] = np.nan
        
        for i in range(nbands):
            out_band = outraster.GetRasterBand(i+1)
            out_band.WriteArray(array[i])
            out_band.FlushCache()
        
        srs = osr.SpatialReference()
        srs.ImportFromWkt(inraster.GetProjectionRef())
        outraster.SetProjection(srs.ExportToWkt())


