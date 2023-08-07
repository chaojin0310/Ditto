#!/bin/bash

# Must be executed in the scripts directory

name_suffix="10g"

if [ $# -ne 2 ]; then
    echo "Usage: ./breakdown.sh <upload|download> <dir_name>"
    exit 1
fi

if [[ $1 == "upload" ]]; then
    aws s3 cp ../logs/ s3://tpcds-profiles-$name_suffix/$2/ --recursive
elif [[ $1 == "download" ]]; then
    mkdir -p ../results/$2
    aws s3 cp s3://tpcds-profiles-$name_suffix/$2/ ../results/$2/ --recursive
fi

