"""
Utilities for S3 data transfer
"""

import boto
from datetime import datetime, timedelta
import numpy as np
from osgeo import gdal, osr

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

def list_dir_s3(directory, bucket_name):
    connection = boto.connect_s3()
    bucket = connection.get_bucket(bucket_name, validate=False)
    keys = bucket.get_all_keys(prefix=directory)
    filenames = [k.name for k in keys]
    filenames = [fn.replace(directory, '') for fn in filenames]
    #filenames.remove('')
    return filenames

def make_dir_for_user_request(datetime_min, datetime_max, bucket_name, base_dir='requested/', station_name='HSL'):
    connection = boto.connect_s3()
    bucket = connection.get_bucket(bucket_name, validate=False)
    dir_name = base_dir +  station_name + "_EC_" + '_'.join(datetime_min.isoformat().split(' ') + datetime_max.isoformat().split(' ')) + "/"
    key = bucket.new_key(dir_name)
    key.set_contents_from_string('')
    subfolders = ['fig/', 'filtered/']
    for folder in subfolders:
        path = dir_name + folder
        key = bucket.new_key(path)
        key.set_contents_from_string('')
        key.set_canned_acl('public-read')
    return dir_name

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

def save_tiff(array, tile, data_dir='/efs/data/', results_dir='/efs/results/'):
    filename = tile  + '_results.tif'
    nbands, nrows, ncols = array.shape

    data_file = data_dir + tile + '.tif'
    inraster = gdal.Open(data_file)
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
