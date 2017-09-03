import numexpr
import numpy as np
from scipy.fftpack import fft2, ifft2, fftshift

import matplotlib.pyplot as plt

from WindowedTemplate import Scarp
from timeit import default_timer as timer


def test(n):
    data = np.random.randn(n, n)
    
    for alpha in range(10):
        c = calc(data, alpha)
        t = Scarp(100, 10, alpha, n, n, 1).template()
        fc = fft2(c)
        ft = fft2(t)
        xcor = fftshift(ifft2(fc*ft))
        np.save('temp/temp' + str(alpha) + 'c.npy', fc)
        np.save('temp/temp' + str(alpha) + 't.npy', ft)

    start = timer()
    for alpha in range(10):
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
    stop = timer()
    t1 = stop - start
    #print('Full compute time: {:.2f} s'.format(t1))

    del c, t, ft, fc 

    start = timer()
    for alpha in range(10):
        fc = np.load('temp/temp' + str(alpha) + 'c.npy')
        ft = np.load('temp/temp' + str(alpha) + 't.npy')
        xcor = fftshift(ifft2(fc*ft))
    stop = timer()
    t2 = stop - start
    #print('Cached to disk compute time: {:.2f} s'.format(t2))
    print('.')

    return t1, t2
   
def calc(data, alpha):
    dx = 1 
    dy = 1
    z = data
    nan_idx = np.isnan(z)
    z[nan_idx] = 0
    
    dz_dx = np.diff(z, 1, 1)/dx
    d2z_dxdy = np.diff(dz_dx, 1, 0)/dx
    pad_x = np.zeros((d2z_dxdy.shape[0], 1))
    d2z_dxdy = np.hstack([pad_x, d2z_dxdy])
    pad_y = np.zeros((1, d2z_dxdy.shape[1]))
    d2z_dxdy = np.vstack([pad_y, d2z_dxdy])
    
    d2z_dx2 = np.diff(z, 2, 1)/dx**2
    pad_x = np.zeros((d2z_dx2.shape[0], 1))
    d2z_dx2 = np.hstack([pad_x, d2z_dx2, pad_x])

    d2z_dy2 = np.diff(z, 2, 0)/dy**2
    pad_y = np.zeros((1, d2z_dy2.shape[1]))
    d2z_dy2 = np.vstack([pad_y, d2z_dy2, pad_y])

    del2z = numexpr.evaluate("d2z_dx2*cos(alpha)**2 - 2*d2z_dxdy*sin(alpha)*cos(alpha) + d2z_dy2*sin(alpha)**2")
    del2z[nan_idx] = np.nan 

    return del2z

if __name__ == "__main__":
    sizes = [100, 200, 300, 500, 1000, 1500, 2000]
    t_full = []
    t_cache = []

    for size in sizes:
        t1, t2 = test(size)
        t_full.append(t1)
        t_cache.append(t2)

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(sizes, t_full, '-^', sizes, t_cache, '-s')
    ax.set_xlabel('dimension')
    ax.set_ylabel('time (s)')
    plt.legend(['full', 'cached'])
    
    plt.show()

