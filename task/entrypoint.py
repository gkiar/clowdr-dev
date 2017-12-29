#!/usr/bin/env python

from argparse import ArgumentParser
import re
import os.path as op
import boutiques as bosh
import boto3
import os
import json


def process_task(metadata):
    # Get metadata
    local_task_dir = "/task/"
    # aws_get(metadata, local)

    # Parse metadata
    metadata   = json.load(open(metadata))
    descriptor = metadata['descriptor']
    invocation = metadata['invocation']
    input_data = metadata['input_data']
    bids       = metadata['bids']

    # Get descriptor and invocation
    desc_local = op.join(local_task_dir, "descriptor.json")
    os.system("cp {} {}".format(descriptor, desc_local))
    # aws_get(descriptor, desc_local)

    invo_local = op.join(local_task_dir, "invocation.json")
    os.system("cp {} {}".format(invocation, invo_local))
    # aws_get(invocation, local)

    # Get input data
    local_data_dir = "/data/"
    # for dataloc in input_data:
    #     aws_get(dataloc, op.join(local, input_data[dataloc]))

    # Move to correct location
    os.chdir(local_data_dir)

    # Validate descriptor + invocation + input data combo
    bosh.validate(desc_local)

    if bids:
        parties = json.load(open(invo_local)).get("participant_label")
        if len(parties) > 0:
            for part in parties:
                continue
                #TODO: BIDS thing

    # Launch task
    bosh.execute('launch',  desc_local, invo_local)

    # Get list of bosh exec outputs
    with open(desc_local) as fhandle:
        outputs_all = json.load(fhandle)["output-files"]

    outputs_present = []
    for outfile in outputs_all:
        outputs_present += [outfile] if op.exists(outfile) else []

    # Push outputs
    for local_output in outputs_present:
        remote_output = op.relpath(local_data_dir, local_output)
        print(remote_output)
    # local = "/path/to/some/outputs"
    # remote = "s3://something"
    # aws_post(local, remote)


def aws_get(remote, local):
    s3 = boto3.resource("s3")

    split = re.split("s3://([a-zA-Z0-9_-]+)/([a-zA-Z0-9/_-]+)", remote)
    bucket, rpath = split[0], split[1]

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

