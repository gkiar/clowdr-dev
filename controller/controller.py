#!/usr/bin/env python

from argparse import ArgumentParser
import random, string, boto3, json, time, datetime
import os, os.path as op, sys

from utils import *


def aws_driver(descriptor, invocation, credentials,
               datapath, outpath, bids=True,
               verb=False, detach=False):
    """
    Driver of the AWS controller
    """

    # Create session for accessing the AWS API
    session, creds = start_aws_session(credentials)

    # Configure roles in AWS
    iam = session.client('iam')
    roles = configure_iam_roles(iam, verb=verb)

    # Ensure the provided Bucket exists, and that we have write access
    s3 = session.client('s3')
    # configure_s3(s3, verb=verb)

    # Configure Batch/EC2 setting
    ec2 = session.client('ec2')
    batch = session.client('batch')
    configure_batch(ec2, batch, roles, verb=verb)

    # Push invocation, descriptor, metadata to S3
    data_bucket = datapath.split("/")[2]

    ts = time.time()
    dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H:%M:%S')
    randx = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))

    metadict = {}
    metadict["output_loc"] = op.join(outpath, randx)

    dpath = "clowdrtask/{}-{}/descriptor.json".format(dt, randx)
    s3.upload_file(descriptor, data_bucket, dpath)
    metadict["descriptor"] = "s3://{}/{}".format(data_bucket, dpath)

    if bids:
        tmpinvo = json.load(open(invocation))

        pref = "/".join(datapath.split('/')[3:])
        # dataset_info = s3.list_objects(Bucket=data_bucket, Prefix=pref, Delimiter="sub-")
        # dataset_list = ["s3://{}/{}".format(data_bucket, item['Key'])
        #                 for item in dataset_info['Contents']]

        parties = tmpinvo.get("participant_label")
        if parties is None:
            party_info = s3.list_objects(Bucket=data_bucket, Prefix="{}sub-".format(pref), Delimiter="/")
            parties = [party['Prefix'].split('/')[-2].split('sub-')[-1]
                       for party in party_info['CommonPrefixes']]

        for party in parties:
            tmpinvo["participant_label"] = [party]

            metadict["input_data"] = ["{}/sub-{}/".format(datapath.strip('/'), party)]
            # metadict["input_data"] += dataset_list

            invocation = "/tmp/invocation-{}-{}.json".format(randx, party)
            with open(invocation, 'w') as fhandle:
                fhandle.write(json.dumps(tmpinvo))

            ipath = "clowdrtask/{}-{}/invocation-{}.json".format(dt, randx, party)
            s3.upload_file(invocation, data_bucket, ipath)
            metadict["invocation"] = "s3://{}/{}".format(data_bucket, ipath)
            
            metadata = '/tmp/metadata-{}-{}.json'.format(randx, party)
            with open(metadata, 'w') as fhandle:
                fhandle.write(json.dumps(metadict))

            s3.upload_file(metadata, data_bucket, "clowdrtask/{}-{}/metadata-{}.json".format(dt, randx, party))
            loc = "s3://{}/clowdrtask/{}-{}/metadata-{}.json".format(data_bucket, dt, randx, party)

            # Submit job
            jid = launch_job(batch, creds, loc)
            if not detach or verb:
                print("Launched job with ID: {}".format(jid))

    else:
        metadict["input_data"] = [datapath]

        if op.isdir(invocation):
            print('handle like group of invocations')
        else:
            ipath = "clowdrtask/{}/{}/invocation-{}.json".format(dt, randx)
            s3.upload_file(invocation, data_bucket, ipath)
            metadict["invocation"] = "s3://{}/{}".format(data_bucket, ipath)
            
            metadata = '/tmp/metadata-{}.json'.format(randx)
            with open(metadata, 'w') as fhandle:
                fhandle.write(json.dumps(metadict))

            s3.upload_file(metadata, data_bucket, "clowdrtask/{}/{}/metadata.json".format(dt, randx))
            loc = "s3://{}/clowdrtask/{}/{}/metadata.json".format(data_bucket, dt, randx)

            # Submit job
            jid = launch_job(batch, creds, loc)
            if not detach or verb:
                print("Launched job with ID: {}".format(jid))

    if not detach:
        log = session.client('logs')
        rec = wait_for_job(batch, jid, "RUNNING")

        # Wait a second to let the logStreamName get updated
        time.sleep(1)
        logStreamName = batch.describe_jobs(jobs=[jid])['jobs'][0]['container']['logStreamName']
        print("Logstream ID: {}".format(logStreamName))

        stream = []
        while batch.describe_jobs(jobs=[jid])['jobs'][0]['status'] == "RUNNING":
            stream = monitor_job(log, logStreamName, stream)
        stream = monitor_job(log, logStreamName, stream)

        print("Job finished with status: {}".format(batch.describe_jobs(jobs=[jid])['jobs'][0]['status']))


def main(args=None):
    parser = ArgumentParser(description="Clowdr Controller")
    parser.add_argument("descriptor", action="store", help="Boutiques descriptor "\
                        "for tool.")
    parser.add_argument("invocation", action="store", help="Parameters for "\
                        "desired invocations.")
    parser.add_argument("credentials", action="store", help="Credentials file"\
                        " for AWS.")
    parser.add_argument("datapath", action="store", help="Path to data on S3 "\
                        "Bucket.")
    parser.add_argument("outpath", action="store", help="S3 bucket and path"\
                        " for storing outputs.")
    parser.add_argument("--bids", action="store_true", help="Indicates BIDS "
                        "app and dataset.")
    parser.add_argument("-d", "--detach", action="store_true",
                        help="Toggles detached mode.")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Toggles verbose outputs.")
    result = parser.parse_args(args) if args is not None else parser.parse_args()

    aws_driver(result.descriptor, result.invocation, result.credentials,
               result.datapath, result.outpath, result.bids,
               result.verbose, result.detach)


if __name__ == "__main__":
    try:
        session = main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except:
            os._exit(0)

