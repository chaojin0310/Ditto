#!/bin/bash

# Must be run at the data-processing directory

name_suffix="10g"

repo_path=..

cd $repo_path/build
sudo ./uploader tpcds-raw-q9495-$name_suffix
sudo ./uploader tpcds-raw-q16-$name_suffix
sudo ./uploader tpcds-raw-q1-$name_suffix
cd -