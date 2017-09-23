"""
Worker classes for template matching
"""

import os
import dem, scarplet
import numpy as np

import logging 
import uuid

from s3utils import save_file_to_s3

from time import sleep
from timeit import default_timer as timer

from WindowedTemplate import Scarp


NUM_ANGLES = 181
NUM_AGES = 35
MAX_AGE = 3.5


logging.config.fileConfig('logging.conf')
logger = logging.getLogger('scarp_reduce.Worker')

class Worker(object):

    def __init__(self):
        pass


class Matcher(object):
    
    def __init__(self, source, pad_dx, pad_dy, base_path='/efs/results/'):
        self.age = None
        self. d = None
        self.pad_dx = pad_dx
        self.pad_dy = pad_dy
        self.logger = logger or logging.getLogger(__name__)

        name = source.split('/')[-1][:-4]
        self.path = base_path + name + '/'
        if not os.path.exists(self.path):
            os.mkdir(self.path)

        self.source = source    

    def load_data(self):
        # Load data from source file
        PAD_DX = self.pad_dx
        PAD_DY = self.pad_dy
        # XXX: Assume data has been interpolated to remove nans
        self.data = dem.DEMGrid(self.source)
        self.dx = self.data._georef_info.dx
        self.dy = self.data._georef_info.dy
        self.data._pad_boundary(PAD_DX, PAD_DY) # TODO: Pad data once and save
        self.logger.info("Loaded data from {}".format(self.source))
    
    def match_template(self):
        # Match template to data

        return scarplet.calculate_best_fit_parameters(self.data, Scarp, self.d, self.age)

    def process(self, d, ages):
        # Match templates for a list of parameters

        start = timer()
        self.load_data()
        self.d = d
        
        for age in ages:
            self.set_params(age, d)
            self.save_template_match()
        stop = timer()
        self.logger.info("Processed:\t {}".format(self.source))
        self.logger.info("Paramaters:\t d = {:d}, logkt = {:.2f}".format(int(self.d), self.age))
        self.logger.info("Elapsed time:\t {:.2f} s".format(stop - start))

    def save_template_match(self):
        # Match template, and save clipped (valid) results

        self.results = self.match_template()
        # XXX: Assume data is padded!
        np.save(self.path + self.filename, self.results[:, PAD_DY:-PAD_DY, PAD_DX:-PAD_DX])
        del self.results

    def set_params(self, age, d):
        # Set worker template parameters

        self.age = age
        self.d = d
        self.filename = 'results_{:.2f}.npy'.format(self.age)

    def set_source(self, source):
        # Set data source location

        self.source = source

      
class Reducer(object):

    def __init__(self, path):
        if path[-1] == '/':
            path = path[:-1]
        self.path = path
        self.tile_name = path.split('/')[-1]
        self.best_results = None
        self.logger = logger or logging.getLogger(__name__)
        
    def compare(self, file1, file2):
        # Compare two search step results ndarrays

        data1 = np.load(file1)
        data2 = np.load(file2)
        mask = data1[-1,:,:] > data2[-1,:,:]
        data2[:,mask] = data1[:,mask]
        
        os.remove(file1)
        os.remove(file2)
        #self.logger.info("Compared {} and {} in {}".format(file1, file2, os.getcwd().split('/')[-1])) 
        return data2
    
    def mask_results(self, tile_name, results):
        # Mask out nodata in results ndarray using saved mask

        if os.path.exists('/efs/masks/' + tile_name + '_mask.npy'):
            mask = np.load('/efs/masks/' + tile_name + '_mask.npy')
            results[:, mask] = np.nan

        return results

    def reduce(self, directory):
        # Reduce a single directory of results

        curdir = os.getcwd()
        self.path = self.path + '/' + directory
        results = os.listdir(self.path)
        os.chdir(self.path)

        files_processed = 0
        while files_processed < self.num_files - 1: 
            if len(results) > 1:
                sleep(2) # XXX: this is to avoid reading in a npy array as it is being written to disk
                results1 = results.pop()
                results2 = results.pop()
                best = self.compare(results1, results2)
                filename = uuid.uuid4().hex + '.npy'
                np.save(filename, best)
                files_processed += 1
            results = os.listdir('.')

        os.chdir(curdir)
        self.best_results = self.path + os.listdir(self.path)[0]

    def reduce_all_results(self):
        # Reduce results in all directories until all search steps have been compared

        curdir = os.getcwd()
        subgrids = os.listdir(self.path)
        os.chdir(self.path)

        start = timer()

        num_subgrids = len(subgrids)
        total_files = num_subgrids*(self.num_files - 1)
        self.logger.info("Expecting:\t {} files".format(total_files))

        files_processed = 0
        while files_processed < total_files:
            for directory in subgrids:
                results = os.listdir(directory)
                os.chdir(directory)
                if len(results) > 1:
                    sleep(2) # XXX: this is to avoid reading in a npy array as it is being written to disk
                    results1 = results.pop()
                    results2 = results.pop()
                    best = self.compare(results1, results2)
                    filename = uuid.uuid4().hex + '.npy'
                    np.save(filename, best)
                    files_processed += 1
                os.chdir('..')

        stop = timer()
        average_time = (stop - start) / num_subgrids
        self.logger.info("Processed:\t {} files".format(files_processed + num_subgrids))
        self.logger.info("Average processing time: {:.2f} s per grid".format(average_time)) 

        os.chdir(curdir)
                
    def save_results(self):
        # Save all reduced results to S3 bucket

        subgrids = os.listdir(self.path)
        # XXX: Assumes each directory contains a single file containing fully reduced data!
        for tile in subgrids:
            best_file = self.path + '/' + tile + '/' + os.listdir(self.path + '/' + tile)[0]
            results = np.load(best_file)    
            #results = self.mask_results(tile, results)
            np.save(best_file, results)    
            save_file_to_s3(best_file, tile + '_results.npy', bucket_name='scarp-testing')
            self.logger.info("Saved best results for {}".format(tile))

    def set_num_files(self, num_files):
        # Number of files per directory (= number of workers/search steps)

        self.num_files = num_files
    
    def set_path(self, path):

        self.path = path
                

