"""
Worker classes for template matching
"""

import os
import dem, scarplet
import numpy as np

import uuid

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
        self.path = '/efs/results/'
        self.source = source    

    def clip_results(pad_dx, pad_dy):
        i = pad_dx / self.dx
        j = pad_dy / self.dy
        self.results = self.results[:, j:-j, i:-i]

    def load_data(self):
        self.data = dem.DEMGrid(self.source)
        self.dx = self.data._georef_info.dx
        self.dy = self.data._georef_info.dy
        #self.data._pad_boundary(PAD_DX, PAD_DY) # Pad data once
    
    def match_template(self, data, d, age):
        return scarplet.calculate_best_fit_parameters(data, Scarp, d, age)

    def process(self, d, ages):
        self.load_data()
        self.d = d
        for age in ages:
            self.set_params(age, d)
            self.save_template_match()

    def save_template_match(self):
        self.results = self.match_template(self.data, self.d, self.age)
        self.clip_results(PAD_DX, PAD_DY) # Assume data is padded!
        np.save(self.path + self.filename, self.results)

    def set_params(self, age, d):
        self.age = age
        self.d = d
        self.filename = 'results_' + str(self.age) + '.npy'

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
        while files_processed < self.num_workers - 1: 
            if len(results) > 1:
                results1 = results.pop()
                results2 = results.pop()
                self.compare(results1, results2)
                files_processed += 1
            results = os.listdir(self.path)

        self.best_results = os.listdir(self.path)[0]

    def set_num_workers(self, num_workers):
        self.num_workers = num_workers
    
    def set_path(self, path):
        self.path = path
                

