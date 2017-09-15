"""
Worker classes for template matching
"""

import os
import dem, scarplet
import numpy as np

import uuid

from time import sleep
from timeit import default_timer as timer

from WindowedTemplate import Scarp


NUM_ANGLES = 181
NUM_AGES = 35
MAX_AGE = 3.5

PAD_DX = 250
PAD_DY = 250


class Worker(object):

    def __init__(self):
        pass


class Matcher(object):
    
    def __init__(self, source):
        self.age = None
        self. d = None
        name = source.split('/')[-1][:-4]
        self.path = '/efs/results/' + name + '/'
        self.source = source    

    def clip_results(self, pad_dx, pad_dy):
        i = int(pad_dx / self.dx)
        j = int(pad_dy / self.dy)
        self.results = self.results[:, j:-j, i:-i]

    def load_data(self):
        self.data = dem.DEMGrid(self.source)
        self.dx = self.data._georef_info.dx
        self.dy = self.data._georef_info.dy
        self.data._pad_boundary(PAD_DX, PAD_DY) # TODO: Pad data once and save
    
    def match_template(self):
        #return scarplet.calculate_fit(self.data, Scarp, self.d, self.age, self.angle)
        return scarplet.calculate_best_fit_parameters(self.data, Scarp, self.d, self.age)

    def process(self, d, ages):
        self.load_data()
        self.d = d

        for age in ages:
            start = timer()
            self.set_params(age, d)
            self.save_template_match()
            stop = timer()
            print("Fit template for {}, params d={:.0f}, kt={:.2f}".format(self.source, self.d, age))
            print("Elapsed time:\t {:.2f} s".format(stop-start))

    def save_template_match(self):
        self.results = self.match_template()
        self.clip_results(PAD_DX, PAD_DY) # Assume data is padded!
        np.save(self.path + self.filename, self.results)

    def set_params(self, age, d):
        self.age = age
        self.d = d
        self.filename = 'results_{:.2f}.npy'.format(self.age)
        #self.filename = 'results_{:.2f}_{:.2f}.npy'.format(self.angle, self.age)

    def set_source(self, source):
        self.source = source

      
class Reducer(object):

    def __init__(self, path):
        self.path = path
        self.best_results = None
        
    def compare(self, file1, file2):
        data1 = np.load(self.path + file1)
        data2 = np.load(self.path + file2)
        mask = data1[-1,:,:] > data2[-1,:,:]
        data2[:,mask] = data1[:,mask]
        
        os.remove(self.path + file1)
        os.remove(self.path + file2)
        
        filename = uuid.uuid4().hex + '.npy'
        np.save(self.path + filename, data2)
        
    def reduce(self):
        results = os.listdir(self.path)
        
        files_processed = 0
        while files_processed < self.num_files - 1: 
            if len(results) > 1:
                sleep(1) # XXX: this is to avoid reading in a npy array as it is being written to disk
                results1 = results.pop()
                results2 = results.pop()
                self.compare(results1, results2)
                files_processed += 1
            results = os.listdir(self.path)

        self.best_results = os.listdir(self.path)[0]

    def set_num_files(self, num_files):
        self.num_files = num_files
    
    def set_path(self, path):
        self.path = path
                

