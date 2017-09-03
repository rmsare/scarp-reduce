"""
Lock classes for safe access to array elements and files
"""

import os

from time import sleep

from datetime import datetime


class Lock(object):
    
    def __init__(self):
        self.lockfile = '{}.lock'.format(os.getpid()) 
        self.timeout = 5 # s
        self.interval = 1 # s

    def acquire(self):
        while self.is_locked():
            sleep(self.interval)
        self.write_lockfile()

    def is_locked(self):
        if os.path.exists(self.lockfile): 
            return True 
        else:
            return False

    def release(self):
        os.remove(self.lockfile)

    def set_interval(self, interval):
        self.interval = interval
    
    def write_lockfile(self):
        with open(self.lockfile, 'w') as f:
            f.write(str(os.getpid()))

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


class IndexLock(Lock):

    def __init__(self, i, j, path='/efs/'):
        self.lockfile = path + '_'.join(['lock', str(i), str(j)])
        self.timeout = 5 # s
        self.interval = 1 # s


class FileLock(Lock):

    def __init__(self, filename, path='/efs/'):
        self.filename = filename
        self.lockfile = path + '_'.join([self.filename, str(os.getpid()), '.lock'])
        self.timeout = 10 # s
        self.interval = 5 # s
