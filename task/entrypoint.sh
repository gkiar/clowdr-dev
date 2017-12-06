#!/bin/bash

# Get task metadata from S3
aws s3 cp --recursive "$1" /task/
chmod +x -R /task/

# Parse metadata
cat /task/metadata.clwdr


# Get input data, invocation, and descriptor from S3
# aws s3 cp input data


# Validate descriptor/invocation/input data combination


# TODO: Add reprozip tracing/metadata collection
# Run job
/task/execution.sh

# Push output data and metadata back to S3
# aws s3 cp outputs to input data loc

