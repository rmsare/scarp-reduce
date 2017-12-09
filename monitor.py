import os, sys
import psutil
import subprocess
import logging
import logging.config
import numpy as np
from time import sleep
from timeit import default_timer as timer

if __name__ == "__main__":

    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('scarp_reduce')

    interval = 15 
    max_periods = 4
    
    this_pid = os.getpid()
    os.chdir('/home/ubuntu/')

    cpu_usage = []
    for _ in range(2 * max_periods):
        sleep(interval) # Allow matching job some startup time
        cpu_usage.append(psutil.cpu_percent())

    while True:
        sleep(interval)
        cpu_usage.append(psutil.cpu_percent())
        cpu_usage = cpu_usage[-int(max_periods)::]

        average_cpu_usage = np.sum(cpu_usage) / max_periods

        if average_cpu_usage < 10.0:
            commands = []
            #commands.append(['sudo', 'umount', '-f', '/efs'])
            #commands.append(['sudo', 'mount', '-t', 'nfs4', '-o', 'nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2', 'fs-2fab1c86.efs.us-west-2.amazonaws.com:/', '/efs'])
            #commands.append(['sudo', 'chown', '-R', 'ubuntu', '/efs'])

            for p in psutil.process_iter():
                if 'ipython' in p.name() and p.pid != this_pid:
                    commands.append(['sudo', 'kill', '{}'.format(p.pid)])
                elif p.pid == this_pid:
                    kill_me = ['sudo', 'kill', '{}'.format(p.pid)]

            commands.append(['sudo', 'sysctl', '-w',  'vm.drop_caches=3'])
            commands.append(['screen', '-wipe'])
            commands.append(['./runme.sh'])
            commands.append(kill_me)

            for c in commands:
                subprocess.call(c)
