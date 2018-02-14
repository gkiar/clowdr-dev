#!/usr/bin/env python

from botocore.exceptions import *
import boto3, json, re, os.path as op

curdir = op.dirname(__file__)

def start_aws_session(cred_file):
    """
    Expects a credentials CSV just as one would download from AWS
    """
    creds = open(cred_file).read().split('\n')[1].split(',')
    session = boto3.Session(aws_access_key_id=creds[0],
                            aws_secret_access_key=creds[1],
                            region_name="us-east-1")
    return session, creds


def configure_iam_roles(iam, verb=False):
    #TODO: fix hard code
    fp = op.join(curdir, 'aws_templates', 'roles.json')
    with open(fp) as fhandle:
        roles = json.load(fhandle)

    policies = {'batch': 'arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole',
                'spot': 'arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetRole',
                'ecs': 'arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role'}

    for rolename in roles:
        role = roles[rolename]
        try:
            name = role['RoleName']
            response = iam.get_role(RoleName=name)
            role['Arn'] = response['Role']['Arn']

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntity':
                if verb:
                    print("Role \"{}\" not found - making role...".format(name))
                role['AssumeRolePolicyDocument'] = json.dumps(role['AssumeRolePolicyDocument'])
                response = iam.create_role(**role)
                role['Arn'] = response['Role']['Arn']
                iam.create_instance_profile(InstanceProfileName=name)
                iam.add_role_to_instance_profile(InstanceProfileName=name, RoleName=name)

                iam.attach_role_policy(RoleName=name,
                                       PolicyArn=policies[rolename])
                roles[rolename] = role
        if verb:
            print("Role ARN: {}".format(roles[rolename]['Arn']))
    return roles


def configure_s3(s3, verb=False):
    buckname = 'clowdr-storage'

    buckets = [buck['Name'] for buck in s3.list_buckets()['Buckets']]

    if buckname in buckets:
        if verb:
            print("S3 Bucket name: /{}".format(buckname))
    else:
        response = s3.create_bucket(
            ACL='public-read-write',
            Bucket=buckname
        )
        if verb:
            print("S3 Bucket name: /{}".format(response['Location']))


def configure_batch(ec2, batch, roles, verb=False):
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

    fp = op.join(curdir, 'aws_templates', 'env.json')
    with open(fp) as fhandle:
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
            if verb:
                print("Environment \"{}\" invalid - deleting environment...".format(name))
            response = batch.update_compute_environment(computeEnvironment=name, state='DISABLED')
            waitUntilDone(name, 'UPDATING')
            response = batch.delete_compute_environment(computeEnvironment=name)
            waitUntilDone(name, 'DELETING')

        if e.response['Error']['Code'] == 'NoSuchEntity' or e.response['Error']['Code'] == 'InvalidEnvironment':
            if verb:
                print("Environment \"{}\" not found - creating environment...".format(name))
            compute['computeResources']['subnets'] = net
            compute['computeResources']['securityGroupIds'] = sg
            if compute['computeResources']['type'] == 'SPOT':
                compute['computeResources']['spotIamFleetRole'] = roles['spot']['Arn']
            compute['computeResources']['instanceRole'] = roles['ecs']['Arn'].replace('role', 'instance-profile')
            compute['serviceRole'] = roles['batch']['Arn']

            response = batch.create_compute_environment(**compute)
            waitUntilDone(name, 'CREATING')
            compute['computeEnvironmentArn'] = response['computeEnvironmentArn']

    if verb:
        print("Compute Environment ARN: {}".format(compute['computeEnvironmentArn']))


    fp = op.join(curdir, 'aws_templates', 'queue.json')
    with open(fp) as fhandle:
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
        if verb:
            print(e)
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print("Queue \"{}\" not found - creating queue...".format(name))
            response = batch.create_job_queue(**queue)
            queue['jobQueueArn'] = response['jobQueueArn']

    if verb:
        print("Job Queue ARN: {}".format(queue['jobQueueArn']))

    fp = op.join(curdir, 'aws_templates', 'def.json')
    with open(fp) as fhandle:
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
            if verb:
                print("Job \"{}\" not found - creating job...".format(name))
            response = batch.register_job_definition(**job)
            job['jobDefinitionArn'] = response['jobDefinitionArn']

    if verb:
        print("Job Definition ARN: {}".format(job['jobDefinitionArn']))


def launch_job(batch, creds, dpath):
    orides = {"environment":[{"name":"AWS_ACCESS_KEY_ID","value":creds[0]},
                             {"name":"AWS_SECRET_ACCESS_KEY","value":creds[1]}],
              "command":[dpath]}
    p1, p2 = re.match('.+\/.+-(\w+)\/metadata-([A-Za-z0-9]+).json', dpath).group(1, 2)
    response = batch.submit_job(jobName="clowdr_{}-{}".format(p1, p2),
                                jobQueue="clowdr-q",
                                jobDefinition="clowdr",
                                containerOverrides=orides)
    jid = response['jobId']
    return jid


def wait_for_job(batch, jid, status):
    try:
        state = False
        while not state:
            stat = batch.describe_jobs(jobs=[jid])['jobs'][0]['status']
            state = stat == status
            if stat == "FAILED" or stat == "SUCCEEDED": raise Exception("Task Complete")
        return True
    except Exception as e:
        print("Waiting failed with: {}".format(e))
        return -1


def monitor_job(log, lsn, stream):
    try:
        tmp_stream = log.get_log_events(logGroupName="/aws/batch/job",
                                        logStreamName=lsn)['events']
        if len(tmp_stream) > len(stream):
            new = tmp_stream[len(stream):]
            for line in new:
                print("{}".format(line['message']))
            stream += new
        return stream
    except Exception as e:
        print("Failed with: {}".format(e))
        return []
