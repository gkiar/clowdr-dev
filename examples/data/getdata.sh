#!/bin/bash

# This script requires that the aws-cli is installed.
# This script will download a T1w nifti image from OpenNeuro
# This script should be run from its own directory (so paths don't break)

aws s3 cp s3://openneuro/ds000114/ds000114_R2.0.0/uncompressed/sub-01/ses-test/anat/sub-01_ses-test_T1w.nii.gz ./example_t1.nii.gz --no-sign-request
