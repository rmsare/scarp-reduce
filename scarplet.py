""" Functions for determinig best-fit template parameters by convolution with a
grid """

from dem import BaseSpatialGrid, Hillshade
import WindowedTemplate as wt

import numpy as np
import numexpr

import matplotlib.pyplot as plt
import pyfftw
from pyfftw.interfaces.numpy_fft import fft2, ifft2, fftshift

from progressbar import ProgressBar, Bar, Percentage, ETA
from timeit import default_timer as timer

np.seterr(divide='ignore', invalid='ignore')

eps = np.spacing(1)
pyfftw.interfaces.cache.enable()

def calculate_amplitude(dem, Template, d, age, alpha):

    ny, nx = dem._griddata.shape
    de = dem._georef_info.dx
    t = Template(d, age, alpha, nx, ny, de)
    template = t.template()

    curv = dem._calculate_directional_laplacian(alpha)
    
    amp, snr = match_template(curv, template)
    mask = t.get_window_limits()
    amp[mask] = 0 
    snr[mask] = 0 

    return amp, snr

#@profile
def calculate_best_fit_parameters(dem, Template, num_ages, **kwargs):
    
    #args = parse_args(**kwargs)
    d = 100
    de = 1

    age_max = 3.5 
    age_min = 0
    age_stepsize = 0.1
    ang_stepsize = 1

    num_angles = 180/ang_stepsize + 1
    #num_ages = (age_max - age_min)/age_stepsize
    orientations = np.linspace(-np.pi/2, np.pi/2, num_angles)
    ages = np.linspace(age_min, age_max, num_ages)

    ny, nx = dem._griddata.shape
    best_amp = np.zeros((ny, nx))
    best_age = np.zeros((ny, nx))
    best_alpha = np.zeros((ny, nx))
    best_snr = np.zeros((ny, nx))

    #pbar = ProgressBar(widgets=[Percentage(), Bar(left='[', right=']'), ' ', ETA()], maxval=len(ages)*len(orientations)).start()
    for i, this_alpha in enumerate(orientations):
        for j, this_age in enumerate(ages):
            
            this_age = 10**this_age

            #args['kt'] = this_age
            #args['alpha'] = this_alpha
            #t = Template(args)
            t = Template(d, this_age, this_alpha, nx, ny, de)
            template = t.template()

            curv = dem._calculate_directional_laplacian(this_alpha)
            
            this_amp, this_snr = match_template_numexpr(curv, template)
            mask = t.get_window_limits()
            this_amp[mask] = 0 
            this_snr[mask] = 0

            best_amp = numexpr.evaluate("(best_snr > this_snr)*best_amp + (best_snr < this_snr)*this_amp")
            best_age = numexpr.evaluate("(best_snr > this_snr)*best_age + (best_snr < this_snr)*this_age")
            best_alpha = numexpr.evaluate("(best_snr > this_snr)*best_alpha + (best_snr < this_snr)*this_alpha")
            best_snr = numexpr.evaluate("(best_snr > this_snr)*best_snr + (best_snr < this_snr)*this_snr")         
        #pbar.update((i+1)*len(ages)
    
    best_snr = ParameterGrid(dem, best_snr, d, name='SNR')
    best_amp = ParameterGrid(dem, best_amp, d, name='Amplitude', units='m')
    best_age = ParameterGrid(dem, best_age, d, name='Morphologic age', units='m^2')
    best_alpha = ParameterGrid(dem, best_alpha, d, name='Orientation', units='deg.')

    return best_amp, best_age, best_alpha, best_snr 

def compare_fits(best_results, this_results):

    best_amp, best_age, best_alpha, best_snr = best_results
    this_amp, this_age, this_alpha, this_snr = this_results

    best_amp = numexpr.evaluate("(best_snr > this_snr)*best_amp + (best_snr < this_snr)*this_amp")
    best_age = numexpr.evaluate("(best_snr > this_snr)*best_age + (best_snr < this_snr)*this_age")
    best_alpha = numexpr.evaluate("(best_snr > this_snr)*best_alpha + (best_snr < this_snr)*this_alpha")
    best_snr = numexpr.evaluate("(best_snr > this_snr)*best_snr + (best_snr < this_snr)*this_snr")    
    return best_amp, best_age, best_alpha, best_snr

def mask_by_snr(amp, age, alpha, snr, thresh=None):

    if thresh is None:
        thresh = np.nanmean(snr._griddata)

    mask = snr._griddata < thresh

    amp._griddata[mask] = np.nan
    age._griddata[mask] = np.nan
    alpha._griddata[mask] = np.nan
    snr._griddata[mask] = np.nan

    return amp, age, alpha, snr

#@profile
def match_template(data, age, angle):
    
    #template = template_function(template_args)
    ny, nx = data.shape
    de = 1
    template = Template(d, age, angle, nx, ny, de).template()

    if data.ndim < template.ndim:
        raise ValueError("Dimensions of template must be less than or equal to dimensions of data matrix")
    if np.any(np.less(data.shape, template.shape)):
        raise ValueError("Size of template must be less than or equal to size of data matrix")

    pad_width = tuple((wid, wid) for wid in template.shape)

    #data = np.pad(data, pad_width=pad_width, mode='symmetric')

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
    T1 = template_sum*(amp**2)
    T2 = -2*amp*xcorr
    T3 = fftshift(ifft2(fc2*fm2))
    error = (1/n)*(T1 + T2 + T3) + eps # avoid small-magnitude dvision
    #error = (1/n)*(amp**2*template_sum - 2*amp*fftshift(ifft2(fc*ft)) + fftshift(ifft2(fc2*fm2))) + eps
    error = np.real(error)
    snr = T1/error
    snr = np.real(snr)

    return amp, snr

def match_template_numexpr(data, template):
    
    eps = np.spacing(1)
    #template = template_function(template_args)

    if data.ndim < template.ndim:
        raise ValueError("Dimensions of template must be less than or equal to dimensions of data matrix")
    if np.any(np.less(data.shape, template.shape)):
        raise ValueError("Size of template must be less than or equal to size of data matrix")

    #pad_width = tuple((wid, wid) for wid in template.shape)

    #data = np.pad(data, pad_width=pad_width, mode='symmetric')

    M = numexpr.evaluate("template != 0")
    fc = fft2(data)
    ft = fft2(template)
    fc2 = fft2(numexpr.evaluate("data**2"))
    fm2 = fft2(M)

    #xcorr = signal.fftconvolve(data, template, mode='same')
    xcorr = np.real(fftshift(ifft2(numexpr.evaluate("ft*fc"))))
    template_sum = np.sum(template**2)
    
    amp = numexpr.evaluate("xcorr/template_sum")
   
    # TODO  remove intermediate terms to make more memory efficent
    n = np.sum(M) + eps
    T1 = numexpr.evaluate("template_sum*(amp**2)")
    T3 = fftshift(ifft2(numexpr.evaluate("fc2*fm2")))
    error = (1/n)*numexpr.evaluate("real(T1 - 2*amp*xcorr + T3)") + eps # avoid small-magnitude dvision
    #error = (1/n)*(amp**2*template_sum - 2*amp*fftshift(ifft2(fc*ft)) + fftshift(ifft2(fc2*fm2))) + eps
    snr = numexpr.evaluate("real(T1/error)")

    return amp, snr

def plot_results(dem, amp, age, alpha, snr, colormap='viridis'):
   
    results = [amp, age, alpha, snr]

    for data in results:
        plt.figure()
        hs = Hillshade(dem)
        hs.plot()
        data.plot(colormap=colormap)
        plt.savefig(data.name + '_' + dem.label + '.png', dpi=300, bbox_inches='tight')
        plt.show()

def run_on_grid(dem):

    amp, age, alpha, snr = calculate_best_fit_parameters(dem, wt.Scarp)
    amp, age, alpha, snr = mask_by_snr(amp, age, alpha, snr)
    plot_results(dem, amp, age, alpha, snr)

    return amp, age, alpha, snr

def save_results(dem, amp, age, alpha, snr, base_dir=''):
    
    results = [amp, age, alpha, snr] # as ParameterGrid objects

    labels = {'Amplitude' : 'm',
            'Morphologic age' : 'm^2',
            'Orientation' : 'deg.',
            'Signal-to-noise ratio' : ''}
    
    for data, param in zip(results, labels):
        #grid = ParameterGrid(dem, data, d, name=param, units=labels[param])
        filename = '_'.join(param.split(' ')).lower() + '_' + dem.label + '.tif'
        filename = base_dir + filename
        data.save(filename)


class ParameterGrid(BaseSpatialGrid):

    def __init__(self, dem, data, d, name='', units=''):
        
        self._georef_info = dem._georef_info
        self._griddata = data
        self.d = d
        self.name = name
        self.units = units

    def plot(self, alpha=0.5, colormap='viridis'):

        plt.imshow(self._griddata, alpha=alpha, cmap=colormap, origin='lower')
        cb = plt.colorbar(orientation='horizontal', extend='both')
        label = self.name + ' [' + self.units + ']'
        cb.set_label(label)


# XXX: necessary?
class TemplateFit(object):

    def __init__(self, d, age, alpha, amplitude, snr):
    
        self.d = d
        self.age = age
        self.alpha = alpha
        self.amplitude = amplitude
        self.snr = snr

