import os, sys
import psutil
import subprocess
import logging
import logging.config

from time import sleep
from timeit import default_timer as timer

if __name__ == "__main__":

    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('scarp_reduce')

    interval = 60
    
    this_pid = os.getpid()
    os.chdir('/home/ubuntu/')
    sleep(2*interval)

    while True:
        if psutil.cpu_percent() < 25.0:
            commands = []
            for p in psutil.process_iter():
                if 'ipython' in p.name() and p.pid != this_pid:
                    commands.append(['sudo', 'kill', '{}'.format(p.pid)])
            commands.append(['sudo', 'sysctl', '-w',  'vm.drop_caches=3'])
            commands.append(['screen', '-d',  '-m', './runme.sh'])

            for c in commands:
                subprocess.call(c)
        else:
            sleep(interval)

        
