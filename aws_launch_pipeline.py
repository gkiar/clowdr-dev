#!/usr/bin/env python

from botocore.exceptions import *
import boto3
import json
import time
import datetime


def start_aws_session(creds=None):
    """
    Expects a credentials CSV just as one would download from AWS
    """
    creds = open(cred_file).read().split('\n')[1].split(',')
    session = boto3.Session(aws_access_key_id=creds[0],
                            aws_secret_access_key=creds[1],
                            region_name="us-east-1")
    return session


def configure_iam_roles(iam):
    #TODO: fix hard code
    with open('./aws_templates/roles.json') as fhandle:
        roles = json.load(fhandle)

    policies = ['arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole',
                'arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role']

    for idx, rolename in enumerate(roles):
        role = roles[rolename]
        try:
            name = role['RoleName']
            response = iam.get_role(RoleName=name)
            role['Arn'] = response['Role']['Arn']

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntity':
                print("Role \"{}\" not found - making role...".format(name))
                role['AssumeRolePolicyDocument'] = role['AssumeRolePolicyDocument'].replace('\'', '"')
                response = iam.create_role(**role)
                role['Arn'] = response['Role']['Arn']
                iam.create_instance_profile(InstanceProfileName=name)
                iam.add_role_to_instance_profile(InstanceProfileName=name, RoleName=name)

                iam.attach_role_policy(RoleName=name,
                                       PolicyArn=policies[idx])
                roles[rolename] = role

        print(roles[rolename]['Arn'])


def configure_s3(s3):
    ts = time.time()
    dt = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    
    buckname = 'clowdr-storage'
    dpath = dt

    buckets = [buck['Name'] for buck in s3.list_buckets()['Buckets']]

    if buckname in buckets:
        print('/{}'.format(buckname))
    else:
        response = s3.create_bucket(
            ACL='public-read-write',
            Bucket=buckname
        )
        print(response['Location'])


def clowdr_aws_driver():
    """
    Main driver of the AWS script
    """
    clowdr = clowdrAWS()

    # Create session for accessing the AWS API
    #TODO: fix hard code
    cred_file = '/Users/gkiar/Dropbox/keys/sugreg.csv'
    session = clowdr.start_aws_session(cred_file)

    # Configure roles in AWS
    iam = session.client('iam')
    configure_iam_roles(iam)

    # Ensure the provided Bucket exists, and that we have write access
    s3 = session.client('s3')
    configure_s3(s3)

    return session


if __name__ == "__main__":
    session = clowdr_aws_driver()

    
    ec2 = session.client('ec2')
    batch = session.client('batch')

# In[7]:
sg = [sg['GroupId'] for sg in ec2.describe_security_groups()['SecurityGroups'] if sg['GroupName'] == 'default']
net = [nets['SubnetId'] for nets in ec2.describe_subnets()['Subnets']]

def waitUntilDone(name, status):
    try:
        curr = batch.describe_compute_environments(computeEnvironments=[name])['computeEnvironments'][0]['status']
        if curr == status:
            waitUntilDone(status)
        else:
            return
    except:
        return

with open('./aws_templates/env.json') as fhandle:
    compute = json.load(fhandle)

try:
    name = compute['computeEnvironmentName']
    response = batch.describe_compute_environments(computeEnvironments=[name])
    if len(response['computeEnvironments']):
        if response['computeEnvironments'][0]['status'] != 'VALID' or            response['computeEnvironments'][0]['state'] != 'ENABLED':
            raise ClientError({'Error': {'Code':'InvalidEnvironment'}}, 'describe_compute_environments')
        else:
            compute['computeEnvironmentArn'] = response['computeEnvironments'][0]['computeEnvironmentArn']
    else:
        raise ClientError({"Error":{"Code":"NoSuchEntity"}}, 'describe_compute_environments')

except ClientError as e:
    if e.response['Error']['Code'] == 'InvalidEnvironment':
        print("Environment \"{}\" invalid - deleting environment...".format(name))
        response = batch.update_compute_environment(computeEnvironment=name, state='DISABLED')
        waitUntilDone(name, 'UPDATING')
        response = batch.delete_compute_environment(computeEnvironment=name)
        waitUntilDone(name, 'DELETING')

    if e.response['Error']['Code'] == 'NoSuchEntity' or e.response['Error']['Code'] == 'InvalidEnvironment':
        print("Environment \"{}\" not found - creating environment...".format(name))
        compute['computeResources']['subnets'] = net
        compute['computeResources']['securityGroupIds'] = sg
        compute['computeResources']['instanceRole'] = roles['ecs']['Arn'].replace('role', 'instance-profile')
        compute['serviceRole'] = roles['batch']['Arn']

        response = batch.create_compute_environment(**compute)
        waitUntilDone(name, 'CREATING')
        compute['computeEnvironmentArn'] = response['computeEnvironmentArn']

print(compute['computeEnvironmentArn'])


# In[13]:


with open('./aws_templates/queue.json') as fhandle:
    queue = json.load(fhandle)

try:
    name = queue['jobQueueName']
    response = batch.describe_job_queues()
    if not len(response['jobQueues']):
        raise ClientError({"Error":{"Code":"NoSuchEntity"}}, 'describe_job_queues')
    else:
        queue_names = [response['jobQueues'][i]['jobQueueName']
                       for i in range(len(response['jobQueues']))]
        if name not in queue_names:
            raise ClientError({"Error":{"Code":"NoSuchEntity"}}, 'describe_job_queues')
        queue['jobQueueArn'] = response['jobQueues'][0]['jobQueueArn']
except ClientError as e:
    print(e)
    if e.response['Error']['Code'] == 'NoSuchEntity':
        print("Queue \"{}\" not found - creating queue...".format(name))
        response = batch.create_job_queue(**queue)
        queue['jobQueueArn'] = response['jobQueueArn']

print(queue['jobQueueArn'])


# In[16]:


# task = {'name': 'clowder-driver'}
with open('./aws_templates/def.json') as fhandle:
    job = json.load(fhandle)

try:
    name = job['jobDefinitionName']
    response = batch.describe_job_definitions()
    if not len(response['jobDefinitions']) or response['jobDefinitions'][0]['status'] == 'INACTIVE':
        raise ClientError({"Error":{"Code":"NoSuchEntity"}}, 'describe_job_definitions')
    else:
        job['jobDefinitionArn'] = response['jobDefinitions'][0]['jobDefinitionArn']
except ClientError as e:
    if e.response['Error']['Code'] == 'NoSuchEntity':
        print("Job \"{}\" not found - creating job...".format(name))
        response = batch.register_job_definition(**job)
        job['jobDefinitionArn'] = response['jobDefinitionArn']

print(job['jobDefinitionArn'])


# In[17]:


orides = {"environment":[{"name":"AWS_ACCESS_KEY_ID","value":creds[0]},
                         {"name":"AWS_SECRET_ACCESS_KEY","value":creds[1]}]}
response = batch.submit_job(jobName="testing-2", jobQueue="clowdr-q",
                            jobDefinition="clowdr",
                            containerOverrides=orides)


# In[18]:


jid = response['jobId']
while True:
    stat = batch.describe_jobs(jobs=[jid])['jobs'][0]['status']
    print(stat)
    if stat == 'SUCCEEDED' or stat == 'FAILED':
        break;
    else:
        time.sleep(30)

