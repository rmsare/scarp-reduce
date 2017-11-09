"""
Worker classes for template matching
"""

import os
import dem, scarplet
import numpy as np

import logging 
import uuid

from osgeo import gdal, osr

from s3utils import save_file_to_s3, save_tiff

from shutil import rmtree
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

        self.source = source    

    def load_data(self):
        """
        Load data from source file
        """

        # XXX: Assumes data has been interpolated to remove nans
        # XXX: Assumes ata is padded by pad_dx, pad_dy 
        self.data = dem.DEMGrid(self.source)
        self.dx = self.data._georef_info.dx
        self.dy = self.data._georef_info.dy
        #self.pad_dx /= self.dx
        #self.pad_dy /= np.abs(self.dy)
        self.pad_dx = int(self.pad_dx)
        self.pad_dy = int(self.pad_dy)
        #self.data._pad_boundary_with_neighboring_values(self.pad_dx)
        self.logger.debug("Loaded data from {}".format(self.source))
    
    def match_template(self):
        """
        Match template to current data
        """

        return scarplet.calculate_best_fit_parameters(self.data, Scarp, self.d, self.age)

    def process(self, d, ages):
        """
        Match templates for a list of parameters

        Currently supports a simple length scale (d) and multiple ages (ages)
        """

        start = timer()
        self.load_data()
        this_age = ages[0]
        self.set_params(this_age, d)
        files = os.listdir(self.path)
        not_full = len(files) < 35
        if not_full:
            if os.path.exists(self.path + self.filename):
            # XXX: This is awful, use a job queue
                ages = np.linspace(0, 3.5, 35)
                ages = [float('{:.2f}'.format(x)) for x in ages]
                processed_ages = [float(f[8:11]) for f in files]
                unprocessed_ages = list(set(ages) - set(processed_ages))
                this_age = np.random.choice(unprocessed_ages)
                self.set_params(this_age, d)
                self.save_template_match()
            else:
                self.save_template_match()
        stop = timer()

        self.logger.debug("Processed:\t {}".format(self.source))
        self.logger.debug("Paramaters:\t d = {:d}, logkt = {:.2f}".format(int(self.d), self.age))
        self.logger.debug("Elapsed time:\t {:.2f} s".format(stop - start))

    def save_template_match(self):
        """
        Save clipped results for template match
        """

        # XXX: Assumes data is padded!
        self.results = self.match_template()
        np.save(self.path + self.filename, self.results[:, self.pad_dy:-self.pad_dy, self.pad_dx:-self.pad_dx])
        del self.results

    def set_params(self, age, d):
        """
        Set template parameters
        """ 

        self.age = age
        self.d = d
        self.filename = 'results_{:.2f}.npy'.format(self.age)

    def set_source(self, source):
        """
        Set data source filename
        """

        self.source = source

      
class Reducer(object):

    def __init__(self, path):
        if path[-1] == '/':
            path = path[:-1]
        self.path = path
        self.tile_name = path.split('/')[-1]
        self.best_results = None
        self.data_dir = '/efs/data/'
        self.logger = logger or logging.getLogger(__name__)
        
    def compare(self, file1, file2):
        """
        Load and compare two search step results, then save the best results 
        """

        data1 = np.load(file1)
        data2 = np.load(file2)
        mask = data1[-1,:,:] > data2[-1,:,:]
        data2[:,mask] = data1[:,mask]
        
        os.remove(file1)
        os.remove(file2)
        self.logger.debug("Compared {} and {} in {}".format(file1, file2, os.getcwd().split('/')[-1])) 
        return data2
    
    def reduce_all_results(self):
        """
        Reduce results in all directories until all search steps have been compared
        """

        curdir = os.getcwd()
        subgrids = os.listdir(self.path)
        os.chdir(self.path)

        start = timer()

        num_subgrids = len(subgrids)
        total_files = num_subgrids * (self.num_files - 1)
        self.logger.debug("Reducing {} grids".format(num_subgrids))
        self.logger.debug("Expecting {} files total".format(total_files))

        self.files_processed = 0
        while self.files_processed < total_files:
            for directory in os.listdir(self.path):
                os.chdir(directory)
                if len(os.listdir('.')) == self.num_files:
                    self.reduce_current_directory()
                    now = timer()
                    self.logger.info("Done with {}".format(directory))
                    self.logger.info("Elapsed time: {:.2f} s".format(now - start))
                    self.save_best_result(directory)
                    os.remove(self.data_dir + directory + '.tif')
                    os.chdir('..')
                    rmtree(directory)
                else:
                    os.chdir('..')

        stop = timer()
        average_time = (stop - start) / num_subgrids
        self.logger.info("Processed:\t {} files".format(self.files_processed + num_subgrids))
        self.logger.info("Average processing time: {:.2f} s per grid".format(average_time)) 

        os.chdir(curdir)
                
    def reduce_current_directory(self):
        """
        Reduce all intermediate results files in current directory
        """

        results = os.listdir('.')
        while len(results) > 1:
            results1 = results.pop()
            results2 = results.pop()
            try:
                best = self.compare(results1, results2)
                filename = uuid.uuid4().hex + '.npy'
                np.save(filename, best)
                self.files_processed += 1
            except (IOError, ValueError) as e:
                self.logger.debug("Error: " + str(e))
                self.logger.debug("Tried to read incomplete npy file")
            results = os.listdir('.')

    def save_best_result(self, directory):
        """
        Save best results file to S3
        """

        # XXX: Assumes a single file remains after reduction
        tile = directory.strip('/')
        best_file = os.listdir('.')[0]
        results = np.load(best_file)    
        save_tiff(results, tile, results_dir='')
        save_file_to_s3(tile + '_results.tif', tile + '_results.tif', bucket_name='scarp-results')
        os.remove(tile + '_results.tif')
        os.remove(best_file)
        self.logger.debug("Saved best results for {}".format(tile))

    def set_num_files(self, num_files):
        """
        Set the nNumber of files per directory to reduce
        (= number of workers/search steps)
        """

        self.num_files = num_files
    
    def set_path(self, path):
        """
        Set path for intermediate results mastwer directory
        """

        self.path = path

