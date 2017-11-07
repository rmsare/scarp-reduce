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
    sleep(interval) # Allow matching job some startup time

    periods = 1.
    total_cpu_usage = psutil.cpu_percent()
    average_cpu_usage = total_cpu_usage / periods

    while True:
        sleep(interval)
            
        periods += 1
        total_cpu_usage += psutil.cpu_percent()
        average_cpu_usage = total_cpu_usage / periods

        if average_cpu_usage < 25.0:
            commands = []
            #commands.append(['sudo', 'umount', '-f', '/efs'])
            #commands.append(['sudo', 'mount', '-t', 'nfs4', '-o', 'nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2', 'fs-2fab1c86.efs.us-west-2.amazonaws.com:/', '/efs'])
            #commands.append(['sudo', 'chown', '-R', 'ubuntu', '/efs'])

            for p in psutil.process_iter():
                if 'ipython' in p.name() and p.pid != this_pid:
                    commands.append(['sudo', 'kill', '{}'.format(p.pid)])

            commands.append(['sudo', 'sysctl', '-w',  'vm.drop_caches=3'])
            commands.append(['screen', '-wipe'])
            commands.append(['./runme.sh'])

            for c in commands:
                subprocess.call(c)
        

        
