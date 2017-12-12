AWS_BUCKET_NAME = 'scarp-data'
AWS_WORKER_INSTANCE_TYPE = 'c4.2xlarge'
AWS_REDUCER_INSTANCE_TYPE = 't2.medium'
AWS_KEY_NAME = 'aws-scarp'
AWS_SECURITY_GROUP = 'sg-2b925f50'
AWS_WORKER_AMI = 'ami-d8f136a0'

SSH_LOCAL_KEY = '/home/ubuntu/aws-scarp.pem'

REDUCER_SCRIPT = """#!/bin/bash
cd /home/ubuntu/scarp-reduce
#git pull origin master
screen -d -m ipython manage_data.py {} {}
screen -d- m ipython reduce_loop.py {} {} {} {}
#sudo shutdown -h now"""

WORKER_SCRIPT = """#!/bin/bash
cd /home/ubuntu/scarp-reduce
ipython -c "%run match.py {} {} {} {} {} {}"
#sudo shutdown -h now"""

STARTUP_SCRIPT = """#!/bin/bash
sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 fs-2fab1c86.efs.us-west-2.amazonaws.com:/ /efs 
sudo chown -R ubuntu /efs
mkdir /efs/data /efs/results /efs/logs
cd /home/ubuntu/scarp-reduce
git stash
git pull origin master
sudo killall ipython screen"""

TEARDOWN_SCRIPT = """#!/bin/bash
sudo umount -f /efs
sudo killall ipython screen mount"""
