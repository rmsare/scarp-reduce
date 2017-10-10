import boto.ec2
import boto.ec2.cloudwatch
import os, time
import subprocess
import numpy as np

from timeit import default_timer as timer

from boto.manage.cmdshell import sshclient_from_instance

from settings import AWS_WORKER_AMI, AWS_BUCKET_NAME, AWS_REDUCER_INSTANCE_TYPE, AWS_WORKER_INSTANCE_TYPE, AWS_KEY_NAME, AWS_SECURITY_GROUP, REDUCER_SCRIPT, STARTUP_SCRIPT, WORKER_SCRIPT, SSH_LOCAL_KEY

MAX_RETRIES = 100

def launch_workers(num_workers):

    if num_workers == 0:
        return []
    
    connection = boto.ec2.connect_to_region('us-west-2')
    new_reservation = connection.run_instances(
                                    AWS_WORKER_AMI,
                                    key_name=AWS_KEY_NAME,
                                    instance_type=AWS_WORKER_INSTANCE_TYPE,
                                    security_group_ids=[AWS_SECURITY_GROUP],
                                    min_count=num_workers,
                                    max_count=num_workers)

    for instance in new_reservation.instances:
        connection.create_tags([instance.id], 
                                {"Name" : "Worker"})

    for instance in new_reservation.instances:
        while instance.state == u'pending':
            time.sleep(2)
            instance.update()
        upload_and_run_script(STARTUP_SCRIPT, instance)
    
    return new_reservation.instances

def add_alarm_to_instances(instances):

    connection = boto.ec2.cloudwatch.connect_to_region('us-west-2')
    topic = 'arn:aws:sns:us-west-2:197873462522:scarp-cpu'

    for i in instances:
        alarm = boto.ec2.cloudwatch.MetricAlarm(
                name='low-cpu-' + i.id,
                namespace='AWS/EC2',
                metric='CPUUtilization',
                statistic='Average',
                period=60*5,
                comparison='<=',
                threshold=50,
                evaluation_periods=2,
                dimensions={'InstanceId' : [i.id]},
                alarm_actions=[topic])
        connection.put_metric_alarm(alarm)

def launch_reducer():
    
    connection = boto.ec2.connect_to_region('us-west-2')
    new_reservation = connection.run_instances(
                                    AWS_WORKER_AMI,
                                    key_name=AWS_KEY_NAME,
                                    instance_type=AWS_REDUCER_INSTANCE_TYPE,
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

    with open('runme.sh', 'w') as f:
        f.write(script)

    commands = []
    commands.append(['ssh', '-o IdentityFile=/home/ubuntu/aws-scarp.pem', '-o StrictHostKeyChecking=no','ubuntu@' + dns, 'sudo killall ipython'])
    commands.append(['ssh', '-o IdentityFile=/home/ubuntu/aws-scarp.pem', '-o StrictHostKeyChecking=no','ubuntu@' + dns, 'sudo sysctl -w vm.drop_caches=3'])
    commands.append(['scp', '-o IdentityFile=/home/ubuntu/aws-scarp.pem', '-o StrictHostKeyChecking=no', 'runme.sh', 'ubuntu@' + dns + ':/home/ubuntu/'])
    commands.append(['ssh', '-o IdentityFile=/home/ubuntu/aws-scarp.pem', '-o StrictHostKeyChecking=no','ubuntu@' + dns, 'sudo /etc/init.d/screen-cleanup start'])
    commands.append(['ssh', '-o IdentityFile=/home/ubuntu/aws-scarp.pem', '-o StrictHostKeyChecking=no','ubuntu@' + dns, 'screen -d -m ipython /home/ubuntu/scarp-reduce/monitor.py'])
    commands.append(['ssh', '-o IdentityFile=/home/ubuntu/aws-scarp.pem', '-o StrictHostKeyChecking=no','ubuntu@' + dns, 'screen -d -m chmod +x runme.sh'])
    commands.append(['ssh', '-o IdentityFile=/home/ubuntu/aws-scarp.pem', '-o StrictHostKeyChecking=no','ubuntu@' + dns, 'screen -d -m ./runme.sh'])
    
    DEVNULL = open(os.devnull, 'w')
    for command in commands:
        start = timer()
        subprocess.call(command, stdout=DEVNULL, stderr=subprocess.STDOUT)
        stop = timer()
        #print('Execute command: {:.2f} s'.format(stop - start))
    DEVNULL.close()

def get_worker_instances():

    connection = boto.ec2.connect_to_region('us-west-2')
    reservations = connection.get_all_instances(filters={'tag:Name' : 'Worker', 'instance-state-name' : 'running'})

    if len(reservations) == 0:
        return []
    else:
        instance_list = [x.instances for x in reservations]
        workers = set().union(*instance_list)
        return workers 

def run_job(instance, param):

    script = WORKER_SCRIPT.format(param[0], param[1])
    upload_and_run_script(script, instance)
    instance.update()

    connection = boto.ec2.connect_to_region('us-west-2')
    connection.create_tags([instance.id], {"Param" : "{}".format(param)})
    print("START: {} Started processing {:d} {:.2f}".format(instance.public_dns_name, param[0], param[1]))

def launch_jobs(d, num_workers):

    min_age = 0
    max_age = 3.5 
    ages = np.linspace(min_age, max_age, num_workers) 

    old_workers = get_worker_instances()
    if len(old_workers) < num_workers:
        workers = launch_workers(num_workers - len(old_workers))
        workers.extend(old_workers)
    else:
        workers = old_workers
        
    add_alarm_to_instances(workers)
    
    for age, instance in zip(ages, workers):
        run_job(instance, [d, age])

def terminate_instances(name="Worker"):

    connection = boto.ec2.connect_to_region('us-west-2')
    reservations = connection.get_all_instances(filters={'tag:Name' : name, 'instance-state-name' : 'running'})

    for r in reservations:
        ids = [i.id for i in r.instances]
        connection.terminate_instances(ids=ids)

if __name__ == "__main__":

    d =int(sys.argv[1]) 
    min_age = 0
    max_age = 3.5 
    d_age = 0.1
    #num_ages = int((max_age - min_age) / d_age)
    num_workers = int(sys.argv[2])
    ages = np.linspace(min_age, max_age, num_workers) 

    start = timer()
    
    old_workers = get_worker_instances()
    num_workers -= len(old_workers)
    print("Using {} active nodes...".format(len(old_workers)))
    print("Starting {} new nodes...".format(num_workers))

    workers = launch_workers(num_workers)
    print("Adding CPU usage alarms to instances...")
    add_alarm_to_instances(workers)
    workers.extend(old_workers)


    stop = timer()
    #print("Spin up: {:.2f} s".format((stop - start) / num_ages)) 

    start = timer()
    for age, instance in zip(ages, workers):
        run_job(instance, [d, age])
    stop = timer()

    #reducer_instance = launch_reducer()
    #reducer_script = REDUCER_SCRIPT.format(num_ages)
    #upload_and_run_script(reducer_script, reducer_instance)
    #print("{}: Started reducer".format(reducer_instance.public_dns_name))

