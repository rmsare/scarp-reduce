AWS_BUCKET_NAME = 'scarp-data'
AWS_INSTANCE_TYPE = 'c4.xlarge'
AWS_KEY_NAME = 'aws-scarp'
AWS_SECURITY_GROUP = 'sg-2b925f50'
AWS_WORKER_AMI = 'ami-f7d02f8f'

SSH_LOCAL_KEY = '/home/rmsare/aws_keys/aws-scarp.pem'

REDUCER_SCRIPT = """#!/bin/bash
sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 fs-47cf69ee.efs.us-west-2.amazonaws.com:/ /efs 
sudo chown -R ubuntu /efs
#cd /home/ubuntu/scarplet-python
#git pull origin master
cd /home/ubuntu/scarp-reduce
#git pull origin master
ipython reduce.py {}
#sudo shutdown -h now"""

STARTUP_SCRIPT = """#!/bin/bash
sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 fs-47cf69ee.efs.us-west-2.amazonaws.com:/ /efs 
sudo chown -R ubuntu /efs
#cd /home/ubuntu/scarplet-python
#git pull origin master
cd /home/ubuntu/scarp-reduce
#git pull origin master
ipython match.py {} {} 200 200
#sudo shutdown -h now"""


