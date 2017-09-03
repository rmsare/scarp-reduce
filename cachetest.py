"""
Utilities for precomputing and caching derived quantities and templates for 
large-scale data processing
"""

import os
import dem

import numexpr
import numpy as np
import pyfftw
from pyfftw.interfaces.numpy_fft import fft2, ifft2, fftshift

import matplotlib.pyplot as plt

from shutil import rmtree

from progressbar import ProgressBar, Percentage, Bar, ETA
from timeit import default_timer as timer

import scarplet
from Lock import FileLock as lock
from WindowedTemplate import Scarp as Template

pyfftw.interfaces.cache.enable()
np.seterr(divide='ignore', invalid='ignore')

eps = np.spacing(1)

CHUNK_NROWS = 500
CHUNK_NCOLS = 500
NUM_AGES = 35
NUM_ANGLES = 181

class DataSplitter(object):
    
    def __init__(self, llx, lly, source, path='chunks/'):
        self.nrows = CHUNK_NROWS
        self.ncols = CHUNK_NCOLS
        
        self.source = source
        self.block_row = None
        self.block_col = None
        
        self.llx = None
        self.lly = None 
        self.ix = None
        self.jy = None
        
        self.path = path
        self.dirname = None

    def load(self, source, block_row, block_col):
        self.block_row = block_row
        self.block_col = block_col
        self.dirname = self.path + self.source.split('.')[0] + str(self.block_row) + '_' + str(self.block_col) + '/'

        source_dem = dem.load(self.source)

        total_cols = source_dem._georef_info.nx
        total_rows = source_dem._georef_info.ny
        
        # TODO: Check against GDAL TIFF read-in array (should this be flipped??
        self.llx = source_dem._georef_info.llx + (total_cols - self.block_col * self.ncols) 
        self.lly = source_dem._georef_info.lly + (total_rows - self.block_row * self.nrows) 
        self.ix = self.ncols * self.block_col
        self.jy = self.nrows * self.block_row
        
        self.obj = source_dem
        self.obj._georef_info.nx = self.ncols
        self.obj._georef_info.ny = self.nrows
        self.obj._georef_info.llx = self.llx
        self.obj._georef_info.lly = self.lly

        self.obj._griddata = self.source_data[jy:jy+nrows, ix:ix+ncols]


class TemplateGenerator(object):
    
    def __init__(self, d, logage, de, nrows, ncols, path='templates/'):
        self.d = d
        self.logage = logage
        self.name = 't_' + str(self.d) + 'm_kt' + str(self.logage)
        #self.nrows = CHUNK_NROWS + PAD_DY
        #self.ncols = CHUNK_NCOLS + PAD_DX
        self.nrows = nrows 
        self.ncols = ncols
        self.de = de
        self.path = path 
    
    def cache_templates(self):
        if not os.path.exists(self.path):
            os.mkdir(self.path)

        for angle in np.linspace(-np.pi/2, np.pi/2, NUM_ANGLES):
            age = 10.**self.logage
            template = Template(self.d, age, angle, self.ncols, self.nrows, self.de).template()

            fname = self.path + self.name + '_ang' + str(angle) + '.npy'
            np.save(fname, template)

        #print('Caching templates:\t\t {:.2f} s'.format(stop - start))


class GridProcessor(object):

    def __init__(self, d, nrows, ncols, source):
        self.d = d
        self.nrows = nrows 
        self.ncols = ncols
        self.source = source
        self.dirname = source.split('.')[0]
        self.path = 'curv/'
        self.template_path = 'templates/'

    def process_grid(self, logage):
        for angle in np.linspace(-np.pi/2, np.pi/2, NUM_ANGLES):
            this_amp, this_snr = self.calculate_fit(angle, logage)
            self.update_best_fits(this_amp, this_snr, logage, angle)
 
    def cache_del2zs(self):
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        
        self.obj = dem.DEMGrid('data/' + self.source)
        #self.obj.pad_boundary(PAD_DX, PAD_DY)
        
        for i, angle in enumerate(np.linspace(-np.pi/2, np.pi/2, NUM_ANGLES)):
            c = self.obj._calculate_directional_laplacian_numexpr(angle)
            np.save(self.path + 'curv_' + str(angle) + '.npy', c)

    def calculate_fit(self, angle, logage):

        cname = 'curv/curv_' + str(angle) + '.npy'
        tname = 'templates/t_' + str(self.d) + 'm_kt' + str(logage) + '_ang' + str(angle) + '.npy'

        data = np.load(cname)
        template = np.load(tname)

        M = template != 0
        fc = fft2(data)
        ft = fft2(template)
        fc2 = fft2(data**2)
        fm2 = fft2(M)

        #xcorr = signal.fftconvolve(data, template, mode='same')
        xcorr = np.real(fftshift(ifft2(ft*fc)))
        template_sum = np.sum(template**2)
        
        amp = xcorr/template_sum
       
        # TODO  remove intermediate terms to make more memory efficent
        n = np.sum(M) + eps
        T1 = numexpr.evaluate("template_sum*(amp**2)")
        T3 = fftshift(ifft2(numexpr.evaluate("fc2*fm2")))
        error = (1/n)*numexpr.evaluate("real(T1 - 2*amp*xcorr + T3)") + eps # avoid small-magnitude dvision
        #error = (1/n)*(amp**2*template_sum - 2*amp*fftshift(ifft2(fc*ft)) + fftshift(ifft2(fc2*fm2))) + eps
        snr = numexpr.evaluate("real(T1/error)")

        #amp = amp[PAD_DY:self.nrows-PAD_DY, PAD_DX:self.ncols-PAD_DX]
        #snr = snr[PAD_DY:self.nrows-PAD_DY, PAD_DX:self.ncols-PAD_DX]

        return amp, snr


def compare_serial(n):
    print("Generating synthetic data...")
    obj = dem.DEMGrid('data/randomgrid.tif')
    data = 1000 + 10*np.random.randn(n, n)
    data[:, n/2:] += 10
    obj._georef_info.ny, obj._georef_info.nx = data.shape
    obj._griddata = data
    obj.save('data/temp.tif')
        
    rmtree('results/temp/')
    os.mkdir('results/temp/')
    snr = -9999*np.ones((n, n))
    np.save('results/temp/best_snr.npy', snr)
    np.save('results/temp/best_amp.npy', snr)
    np.save('results/temp/best_age.npy', snr)
    np.save('results/temp/best_angle.npy', snr)

    print("Generating cache...")
    for age in [1, 2, 3]:
        t = TemplateGenerator(100, age, 1, n, n)
        t.cache_templates()
    g = GridProcessor(100, n, n, 'temp.tif')
    g.cache_del2zs()
    
    print('Single search block (cached)')
    start = timer()    
    g.calculate_fit(np.pi/2, 2)
    stop = timer()
    t1 = stop - start
    
    print('Single search block (serial)')
    start = timer()
    curv = obj._calculate_directional_laplacian(np.pi/2)
    template = Template(100, 2, np.pi/2, n, n, 1).template()
    scarplet.match_template(curv, template)
    stop = timer()
    t2 = stop - start
    
    rmtree('templates/')
    os.mkdir('templates/')
    rmtree('curv/')
    os.mkdir('curv/')
    
    #print('Full processing stream (cached)')
    #start = timer()
    #for age in [1, 2, 3]:
    #    g.process_grid(age)
    #stop = timer()
    #print('Total:\t\t\t\t {:.2f} s'.format(stop - start))
    #t1 = stop - start

    #print('Full processing stream (serial)')
    #start = timer()
    #data = dem.DEMGrid('data/temp.tif')
    #start = timer()
    #_ = scarplet.calculate_best_fit_parameters(data, Template)
    #stop = timer()
    #print('Total:\t\t\t\t {:.2f} s'.format(stop - start))
    #t2 = stop - start

    return t1, t2

if __name__ == "__main__":
    sizes = [10, 25, 50, 100]
    t_cached = []
    t_serial = []

    for n in sizes:
        t1, t2 = compare_serial(n)
        t_cached.append(t1)
        t_serial.append(t2)

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(sizes, t_serial, '-^', sizes, t_cached, '-s')
    ax.set_xlabel('dimension')
    ax.set_ylabel('time (s)')
    plt.legend(['serial', 'cached'])
    plt.show()
