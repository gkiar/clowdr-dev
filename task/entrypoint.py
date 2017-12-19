#!/usr/bin/env python

from argparse import ArgumentParser
import os.path as op
import boutiques as bosh
import boto3
import os
import json


def process_task(metadata):
    # Get metadata
    # local = "/task/"
    # aws_get(metadata, local)

    # Parse metadata
    metadata = json.load(open(metadata))
    descriptor = metadata['descriptor']
    invocation = metadata['invocation']
    input_data = metadata['input_data']
    bids       = metadata['bids']

    # Get descriptor and invocation
    # aws_get(descriptor, local)
    # aws_get(invocation, local)

    # Get input data
    # local = "/data/"
    # for dataloc in input_data:
    #     aws_get(dataloc, local)

    # Move to correct location
    os.chdir('/data')

    # Validate descriptor + invocation + input data combo
    bosh.validate(descriptor)

    # Launch task
    bosh.execute('launch {} {}'.format(descriptor, invocation))

    # Get list of bosh exec outputs

    # Push outputs
    # local = "/path/to/some/outputs"
    # remote = "s3://something"
    # aws_post(local, remote)


def aws_get(remote, local):
    s3 = boto3.resource("s3")

    split = re.split("s3://([a-zA-Z0-9_-]+)/([a-zA-Z0-9/_-]+)", remote)
    bucket, rpath = split[1], split[2]

    buck = s3.Bucket(bucket)
    files = [obj.key for obj in buck.objects.filter(Prefix=rpath)]
    for fl in files:
        fl_local = op.join(local, fl)
        os.makedirs(op.dirname(fl_local), exist_ok=True)
        buck.download_file(fl, fl_local)


def aws_post(local, remote):
    s3 = boto3.client("s3")

    split = re.split("s3://([a-zA-Z0-9_-]+)/([a-zA-Z0-9/_-]+)", remote)
    bucket, rpath = split[1], split[2]

    s3.upload(local, bucket, rpath)


def main(args=None):
    parser = ArgumentParser(description="Entrypoint for Clowdr-task")
    parser.add_argument("metadata", action="store", help="S3 URL to metadata")
    results = parser.parse_args() if args is None else parser.parse_args(args)

    process_task(results.metadata)


if __name__ == "__main__":
    main()

