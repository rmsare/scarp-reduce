import boto.ec2
import os, time
import subprocess
import numpy as np

from timeit import default_timer as timer

from boto.manage.cmdshell import sshclient_from_instance

from settings import AWS_WORKER_AMI, AWS_BUCKET_NAME, AWS_INSTANCE_TYPE, AWS_KEY_NAME, AWS_SECURITY_GROUP, STARTUP_SCRIPT, SSH_LOCAL_KEY

MAX_RETRIES = 100

def launch_workers(num_workers):
    
    connection = boto.ec2.connect_to_region('us-west-2')
    new_reservation = connection.run_instances(
                                    AWS_WORKER_AMI,
                                    key_name=AWS_KEY_NAME,
                                    instance_type=AWS_INSTANCE_TYPE,
                                    security_group_ids=[AWS_SECURITY_GROUP],
                                    min_count=num_workers,
                                    max_count=num_workers)

    for instance in new_reservation.instances:
        connection.create_tags([instance.id], 
                                {"Name" : "Worker"})

    #print("Launched worker: d = {:.2f}, age = {:.2f}".format(param[0], param[1]))
    #print("Public DNS: {}".format(dns))
    #print("State: {}".format(instance.state))
    #print("Launched worker: {}".format(instance.public_dns_name))

    return new_reservation.instances

def launch_reducer():
    
    connection = boto.ec2.connect_to_region('us-west-2')
    new_reservation = connection.run_instances(
                                    AWS_WORKER_AMI,
                                    key_name=AWS_KEY_NAME,
                                    instance_type=AWS_INSTANCE_TYPE,
                                    security_group_ids=[AWS_SECURITY_GROUP])
    instance = new_reservation.instances[0]
    connection.create_tags([instance.id], 
                            {"Name" : "Reducer"})
    
    while instance.state == u'pending':
        time.sleep(5)
        instance.update()

    return instance

def upload_and_run_script(script, instance):
    
    instance.update()
    dns = instance.public_dns_name

    with open('setup_and_run.sh', 'w') as f:
        f.write(script)

    commands = []
    commands.append(['scp', '-o IdentityFile=/home/rmsare/aws_keys/aws-scarp.pem', '-o StrictHostKeyChecking=no', 'setup_and_run.sh', 'ubuntu@' + dns + ':/home/ubuntu/'])
    commands.append(['ssh', '-o IdentityFile=/home/rmsare/aws_keys/aws-scarp.pem', '-o StrictHostKeyChecking=no','ubuntu@' + dns, 'sudo /etc/init.d/screen-cleanup start'])
    commands.append(['ssh', '-o IdentityFile=/home/rmsare/aws_keys/aws-scarp.pem', '-o StrictHostKeyChecking=no','ubuntu@' + dns, 'screen -d -m chmod +x setup_and_run.sh'])
    commands.append(['ssh', '-o IdentityFile=/home/rmsare/aws_keys/aws-scarp.pem', '-o StrictHostKeyChecking=no','ubuntu@' + dns, 'screen -d -m ./setup_and_run.sh'])
    
    DEVNULL = open(os.devnull, 'w')
    for command in commands:
        start = timer()
        subprocess.call(command, stdout=DEVNULL, stderr=subprocess.STDOUT)
        stop = timer()
        print('Execute command: {:.2f} s'.format(stop - start))
    DEVNULL.close()

def get_worker_instances():

    connection = boto.ec2.connect_to_region('us-west-2')
    reservations = connection.get_all_instances(filters={'tag:Name' : 'Worker', 'instance-state-name' : 'running'})
    return reservations[0].instances

def run_job(instance, param):
    script = STARTUP_SCRIPT.format(param[0], param[1])
    upload_and_run_script(script, instance)
    instance.update()
    print("START: {} Started processing {:d} {:.2f}".format(instance.public_dns_name, param[0], param[1]))

def relaunch_jobs():

    d = 100 
    min_age = 0
    max_age = 3.5 
    d_age = 0.1
    num_ages = int((max_age - min_age) / d_age)
    ages = np.linspace(min_age, max_age, num_ages) 

    workers = get_worker_instances()
    
    start = timer()
    for age, instance in zip(ages, workers):
        run_job(instance, [d, age])
    stop = timer()
    print("Re-launched jobs: {:.2f} s".format(stop - start))

if __name__ == "__main__":

    d = 100 
    min_age = 0
    max_age = 3.5 
    d_age = 0.1
    #num_ages = int((max_age - min_age) / d_age)
    num_ages = int(sys.argv[1])
    ages = np.linspace(min_age, max_age, num_ages) 

    start = timer()
    print("Launching {} nodes...".format(num_ages))
    workers = launch_workers(num_ages)
    stop = timer()
    print("Spin up: {:.2f} s".format((stop - start) / num_ages)) 

    start = timer()
    for age, instance in zip(ages, workers):
        run_job(instance, [d, age])
    stop = timer()

    #reducer_instance = launch_reducer()
    #reducer_script = REDUCER_SCRIPT.format(num_ages)
    #upload_and_run_script(reducer_script, reducer_instance)
    #print("{}: Started reducer".format(reducer_instance.public_dns_name))

