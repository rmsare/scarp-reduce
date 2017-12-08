"""
Utilities for S3 data transfer
"""

import os
import boto
import numpy as np
from osgeo import gdal, osr
from datetime import datetime, timedelta
from merge_grids import neighbors

def delete_file_from_s3(filename, bucket_name):
    connection = boto.connect_s3()
    bucket = connection.get_bucket(bucket_name, validate=False)
    key = bucket.get_key(filename)
    bucket.delete_key(key.name)

def delete_old_keys_from_s3(max_age, bucket_name, subdirectory='', bad_substring=''):
    if subdirectory[-1] is not '/':
        subdirectory += '/'
    connection = boto.connect_s3()
    bucket = connection.get_bucket(bucket_name, validate=False)
    today = datetime.now()
    date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    for key in bucket.get_all_keys():
        in_subdirectory = subdirectory in key.name and subdirectory is not key.name
        key_modified = datetime.strptime(key.last_modified, date_format)
        older_than_max_age = today - key_modified >= max_age
        if in_subdirectory and older_than_max_age and bad_substring in key.name:
            key.delete()

def download_data(remote_dir, last_key='', batch_size=100):
    dest_dir = '/efs/data/'
    curdir = os.getcwd()
    os.chdir(dest_dir)

    connection = boto.connect_s3()
    bucket = connection.get_bucket('scarp-data')

    keys = bucket.get_all_keys(marker=remote_dir + last_key,
                               prefix=remote_dir + 'fg',
                               max_keys=batch_size)
    for k in keys:
        fn = k.name.split('/')[-1]
        k.get_contents_to_filename(fn)
        last_key = fn

    os.chdir(curdir)
    
    if not keys.is_truncated:
        last_key = None 

    return last_key

def download_unprocessed_data(remote_dir):
    dest_dir = '/efs/data/'
    curdir = os.getcwd()
    os.chdir(dest_dir)

    connection = boto.connect_s3()
    data_bucket = connection.get_bucket('scarp-data')
    results_bucket = connection.get_bucket('scarp-results')
    data = [k.name for k in data_bucket if remote_dir in k.name]
    data.remove('ot-ncal/')
    data = [f[8:18] for f in data] 
    processed = [k.name[0:10] for k in results_bucket]
    unprocessed = list(set(data) - set(processed))
     
    for tile in unprocessed:
        fn = tile + '.tif'
        key = data_bucket.get_key(remote_dir + fn)
        if key:
            key.get_contents_to_filename(fn)

    os.chdir(curdir)

def list_dir_s3(directory, bucket_name):
    connection = boto.connect_s3()
    bucket = connection.get_bucket(bucket_name, validate=False)
    keys = bucket.get_all_keys(prefix=directory)
    filenames = [k.name for k in keys]
    filenames = [fn.replace(directory, '') for fn in filenames]
    #filenames.remove('')
    return filenames

def download_data_from_s3(infilename, outfilename, bucket_name):
    connection = boto.connect_s3()
    bucket = connection.get_bucket(bucket_name, validate=False)
    key = bucket.new_key(infilename)
    key.get_contents_to_filename(outfilename)

def save_data_to_s3(data, filename=None, bucket_name=None):
    connection = boto.connect_s3()
    bucket = connection.get_bucket(bucket_name, validate=False)
    if not filename:
        d = datetime.now()
        filename = 'tmp_' + d.isoformat() + '.pk'
    key = bucket.new_key(filename)
    data.to_pickle(filename)
    key.set_contents_from_filename(filename)
    key.set_canned_acl('public-read')

def save_file_to_s3(infilename, outfilename, bucket_name):
    connection = boto.connect_s3()
    bucket = connection.get_bucket(bucket_name, validate=False)
    key = bucket.new_key(outfilename)
    key.set_contents_from_filename(infilename)
    key.set_canned_acl('public-read')

def save_tiff(array, tile, pad=0, data_dir='/efs/working/', results_dir='/efs/reducing/'):
    filename = tile  + '_results.tif'
    nbands, nrows, ncols = array.shape

    data_file = data_dir + tile + '.tif'
    inraster = gdal.Open(data_file)
    transform = inraster.GetGeoTransform()
    
    if pad > 0:
        new_ulx = transform[0] + pad
        new_uly = transform[3] - pad
        transform = (new_ulx, transform[1], transform[2], new_uly, transform[4], transform[5])
    
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
