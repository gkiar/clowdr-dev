#!/bin/bash

# Get task metadata from S3
aws s3 cp --recursive "$1" /task/

# Parse metadata


# Get input data, invocation, and descriptor from S3


# Validate descriptor/invocation/input data combination


# TODO: Add reprozip tracing/metadata collection
# Run job


# Push output data and metadata back to S3

