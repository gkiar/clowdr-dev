#!/usr/bin/env python

from argparse import ArgumentParser
from shutil import copy, copytree
import re
import os.path as op
import boutiques as bosh
import boto3
import os
import json

    

def process_task(metadata):
    # Get metadata
    local_task_dir = "/task/"
    print("Fetching metadata...")
    metadata = get(metadata, local_task_dir)[0]

    # Parse metadata
    metadata   = json.load(open(metadata))
    descriptor = metadata['descriptor']
    invocation = metadata['invocation']
    input_data = metadata['input_data']
    output_loc = metadata['output_loc']

    print("Fetching descriptor and invocation...")
    # Get descriptor and invocation
    desc_local = get(descriptor, local_task_dir)[0]
    invo_local = get(invocation, local_task_dir)[0]

    print("Fetching input data...")
    # Get input data
    local_data_dir = "/clowdata/"
    for dataloc in input_data:
        get(dataloc, local_data_dir)

    # Move to correct location
    os.chdir(local_data_dir)

    print("Beginning execution...")
    # Launch task
    try:
        cmd = 'reprozip trace -w --dir={}clowprov/ bosh exec launch {} {}'
        os.system(cmd.format(local_data_dir, desc_local, invo_local))

        cmd = 'reprozip pack {}clowprov/clowgraph.rpz'.format(local_data_dir)
        os.system(cmd)

        # from reprozip.main import main as rzip
        # import sys
        # sys.argv = ['reprozip', 'trace', '-w',
        #             '--dir={}clowprov/'.format(local_data_dir),
        #             'bosh exec launch {} {}'.format(desc_local, invo_local)]
        # print(" ".join(sys.argv))
        # rzip()
    except ImportError:
        print("(Reprozip not installed, no provenance tracing)")
        std = bosh.execute('launch',  desc_local, invo_local)

    # Get list of bosh exec outputs
    with open(desc_local) as fhandle:
        outputs_all = json.load(fhandle)["output-files"]

    outputs_present = []
    outputs_all = bosh.evaluate(desc_local, invo_local, 'output-files/')
    for outfile in outputs_all.values():
        outputs_present += [outfile] if op.exists(outfile) else []

    print("Uploading outputs...")
    # Push outputs
    for local_output in outputs_present:
        print("{} --> {}".format(local_output, output_loc))
        post(local_output, output_loc)


def get(remote, local):
    if remote.startswith("s3://"):
        return aws_get(remote, local)
    elif op.isdir(remote):
        copytree(remote, local)
    else:
        copy(remote, local)


def post(local, remote):
    if "s3://" in remote:
        return aws_post(local, remote)
    elif op.isdir(local):
        copytree(local, remote)
    else:
        copy(local, remote)


def aws_get(remote, local):
    s3 = boto3.resource("s3")

    bucket, rpath = remote.split('/')[2], remote.split('/')[3:]
    rpath = "/".join(rpath)

    buck = s3.Bucket(bucket)
    files = [obj.key for obj in buck.objects.filter(Prefix=rpath) if not os.path.isdir(obj.key)]
    files_local = []
    for fl in files:
        fl_local = op.join(local, fl)
        files_local += [fl_local]
        os.makedirs(op.dirname(fl_local), exist_ok=True)
        if fl_local.strip('/') == op.dirname(fl_local).strip('/'):
            continue;  # Create, but don't try to download directories
        buck.download_file(fl, fl_local)

    return files_local


def aws_post(local, remote):
    # Credit: https://github.com/boto/boto3/issues/358#issuecomment-346093506
    local_files = [op.join(root, f)
                   for root, dirs, files in os.walk(local)
                   for f in files]

    s3 = boto3.client("s3")
    bucket, rpath = remote.split('/')[2], remote.split('/')[3:]
    rpath = "/".join(rpath)

    for flocal in local_files:
        rempat = op.join(rpath, op.relpath(flocal, local))
        s3.upload_file(flocal, bucket, rempat, {'ACL': 'public-read'})


def main(args=None):
    parser = ArgumentParser(description="Entrypoint for Clowdr-task")
    parser.add_argument("metadata", action="store", help="S3 URL to metadata")
    results = parser.parse_args() if args is None else parser.parse_args(args)

    process_task(results.metadata)


if __name__ == "__main__":
    main()

