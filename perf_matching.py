import dem, scarplet

import os
import numpy as np
import pyfftw

#import matplotlib.pyplot as plt

from itertools import izip
from shutil import rmtree

from progressbar import ProgressBar, Percentage, Bar, ETA
from timeit import default_timer as timer

from cachetest import TemplateGenerator, GridProcessor
from WindowedTemplate import Scarp

np.seterr(divide='ignore', invalid='ignore')
pyfftw.interfaces.cache.enable()

def fit_template(age, angle):
    d = 100,
    de =1
    g = GridProcessor(100, n, n, 'temp.tif')
    template = Scarp(d, age, angle, n, n, de).template()
    curv = data_obj._calculate_directional_laplacian(angle)
    
    amp, snr = scarplet.match_template_numexpr(curv, template)
    results = np.stack([amp, age*np.ones_like(amp), angle*np.ones_like(amp), snr])
    fn = '/efs/results/fit_' + str(age) + '_' + str(angle) + '.npy'
    np.save(fn, results)

def fit_template_cached(age, angle):
    g = GridProcessor(100, n, n, 'temp.tif')
    amp, snr = g.calculate_fit(angle, age)
    results = np.stack([amp, age*np.ones_like(amp), angle*np.ones_like(amp), snr])
    fn = '/efs/results/fit_' + str(age) + '_' + str(angle) + '.npy'
    np.save(fn, results)

def fit_all(num_ages, num_angles):
    for age in np.linspace(0, 3.5, num_ages):
        for angle in np.linspace(-np.pi/2, np.pi/2, num_angles):
            fit_template(age, angle)

def fit_all_cached(num_ages, num_angles):
    for age in np.linspace(0, 3.5, num_ages):
        for angle in np.linspace(-np.pi/2, np.pi/2, num_angles):
            fit_template_cached(age, angle)

def compare_fitreduce(num_ages, num_angles):
    fit_all(num_ages, num_angles)
    tree_reduce('/efs/results/')

def compare_fitreduce_cached(num_ages, num_angles):
    fit_all_cached(num_ages, num_angles)
    tree_reduce('/efs/results/')

def compare(fn1, fn2):
    data1 = np.load(fn1)
    data2 = np.load(fn2)
    mask = data1[-1,:,:] > data2[-1,:,:]
    data2[:,mask] = data1[:,mask]
    os.remove(fn1)
    os.remove(fn2)
    fn = '/efs/results/' + str(os.getpid()) + '.npy'
    np.save(fn, data2)

def tree_reduce(directory):
    start = timer()
    results = os.listdir(directory)
    while len(results) > 1:
        for res1, res2 in pairwise(results):
            compare(directory + res1, directory + res2)
        results = os.listdir(directory)
    stop = timer()
    print('Tree reduce:\t\t\t {:.2f} s'.format(stop-start))

def generate_data(n):    
    print("generating synthetic data...")
    obj = dem.DEMGrid('data/randomgrid.tif')
    data = 1000 + 10*np.random.randn(n, n)
    data[:, n/2:] += 10
    obj._georef_info.ny, obj._georef_info.nx = data.shape
    obj._griddata = data
    obj.save('/efs/data/temp.tif')
    return obj
    
def fill_cache(n, num_ages):
    start = timer()
    for age in np.linspace(0, 3.5, num_ages):
        t = TemplateGenerator(100, age, 1, n, n)
        t.cache_templates()
    g = GridProcessor(100, n, n, 'temp.tif')
    g.cache_del2zs()
    stop = timer()
    print('Fill cache:\t\t\t\t {:.2f} s'.format(stop-start))

def compare_serial(data_obj, num_ages):
    start = timer()
    _ = scarplet.calculate_best_fit_parameters(data_obj, Scarp, num_ages)
    stop = timer()
    print('Serial fit:\t\t\t {:.2f} s'.format(stop-start))


def pairwise(iterable):
    a = iter(iterable)
    return izip(a, a)

if __name__ == "__main__":
    sizes = [1000]
    nsteps = 181*35
    t_cached = []
    t_full = []
    t_serial = []
    
    for n in sizes:
        data_obj = generate_data(n)
        fill_cache(n, 2)       
        
        start = timer()
        compare_fitreduce_cached(2, 181)        
        stop = timer()
        print('Full fit and reduce:\t\t {:.2f} s'.format(stop-start))
       
        compare_serial(data_obj, 2)

        rmtree('/efs/results/')
        os.mkdir('/efs/results/')
        rmtree('/efs/templates/')
        os.mkdir('/efs/templates/')
        rmtree('/efs/curv/')
        os.mkdir('/efs/curv/')

    #fig = plt.figure()
    #ax = fig.add_subplot(111)
    #ax.plot(sizes, t_serial, '-^', sizes, t_cached, '-s')
    #x = np.vstack([t_serial, t_cached]).T
    #ax.boxplot(x, labels=['serial', 'cached']) 
    #ax.set_ylabel('time (s)')
    
