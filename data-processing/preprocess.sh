#!/bin/bash

# Must be executed in the data-processing directory

if [ $# -eq 2 ]; then
    name_suffix=$1
    suffix=""
    mode=$2
elif [ $# -eq 3 ]; then
    name_suffix="$1-$2"
    suffix="-$2"
    mode=$3
else
    echo "Usage $0 <data_size> <unique_name_suffix> <split|sched|exec>"
    exit 1
fi

repo_path=..

sed -i -e "5s/.*/name_suffix=$name_suffix/" $repo_path/data-processing/upload_redis.sh
sed -i -e "5s/.*/name_suffix=$suffix/" $repo_path/scripts/expr.sh
sed -i -e "5s/.*/name_suffix=$name_suffix/" $repo_path/scripts/breakdown.sh

# Modify several lines of the source code to adapt to the data size
if [[ $1 == "10g" ]]; then
    sed -i -e "37s/.*/#define MAX_SLOTS 16/" $repo_path/include/utils.hpp
    echo 10 > $repo_path/python/scale.txt
else
    sed -i -e "37s/.*/#define MAX_SLOTS 48/" $repo_path/include/utils.hpp
    sed -i -e "37s/.*/#define INITIAL_CAPACITY 256*16384/" $repo_path/include/sh_array.hpp
    sed -i -e "37s/.*/#define INITIAL_CAPACITY 256*16384/" $repo_path/include/r_array.hpp
    sed -i -e "47s/.*/#define NUM_SAMPLES 5/" $repo_path/include/scheduler.hpp
    sed -i -e "57s/.*/#define PROFILE_SERVERS 5/" $repo_path/include/scheduler.hpp
    sed -i -e "60s/.*/int degs[NUM_SAMPLES] = {10, 20, 30, 60, 120};/" $repo_path/include/scheduler.hpp
    sed -i -e "71s/.*/int wait_time = 60000;/" $repo_path/include/io.hpp
    
    echo 1000 > $repo_path/python/scale.txt

    if [[ $3 == "exec" ]]; then
        sudo sysctl -w vm.nr_hugepages=81920
    fi
fi

sed -i -e "40s/.*/const Aws::String rawBucketName95 = \"tpcds-raw-q9495-$name_suffix\";/" $repo_path/include/io.hpp
sed -i -e "41s/.*/const Aws::String rawBucketName94 = \"tpcds-raw-q9495-$name_suffix\";/" $repo_path/include/io.hpp
sed -i -e "42s/.*/const Aws::String rawBucketName16 = \"tpcds-raw-q16-$name_suffix\";/" $repo_path/include/io.hpp
sed -i -e "43s/.*/const Aws::String rawBucketName1 = \"tpcds-raw-q1-$name_suffix\";/" $repo_path/include/io.hpp
sed -i -e "44s/.*/const Aws::String interBucketName95 = \"tpcds-inter95$suffix\";/" $repo_path/include/io.hpp
sed -i -e "45s/.*/const Aws::String interBucketName94 = \"tpcds-inter94$suffix\";/" $repo_path/include/io.hpp
sed -i -e "46s/.*/const Aws::String interBucketName16 = \"tpcds-inter16$suffix\";/" $repo_path/include/io.hpp
sed -i -e "47s/.*/const Aws::String interBucketName1 = \"tpcds-inter1$suffix\";/" $repo_path/include/io.hpp
sed -i -e "54s/.*/const Aws::String logBucketName = \"tpcds-profiles-$name_suffix\";/" $repo_path/include/io.hpp

if [[ $mode == "split" ]]; then
    cd $repo_path/scripts && bash ./build.sh && cd -
    mkdir -p $repo_path/data-processing/log

    if [[ $1 == "10g" ]]; then 
        bash upload_s3.sh web_sales 24 q9495 $name_suffix
        bash upload_s3.sh web_returns 24 q9495 $name_suffix
        bash upload_s3.sh customer_address 12 q9495 $name_suffix
        bash upload_s3.sh web_site 1 q9495 $name_suffix
        bash upload_s3.sh date_dim 1 q9495 $name_suffix

        bash upload_s3.sh catalog_sales 30 q16 $name_suffix
        bash upload_s3.sh catalog_returns 24 q16 $name_suffix
        bash upload_s3.sh customer_address 12 q16 $name_suffix
        bash upload_s3.sh call_center 1 q16 $name_suffix
        bash upload_s3.sh date_dim 1 q16 $name_suffix

        bash upload_s3.sh date_dim 12 q1 $name_suffix
        bash upload_s3.sh store_returns 24 q1 $name_suffix
        bash upload_s3.sh store 12 q1 $name_suffix
        bash upload_s3.sh customer 24 q1 $name_suffix
    else
        bash upload_s3.sh web_sales 240 q9495 $name_suffix
        bash upload_s3.sh web_returns 240 q9495 $name_suffix
        bash upload_s3.sh customer_address 240 q9495 $name_suffix
        bash upload_s3.sh web_site 1 q9495 $name_suffix
        bash upload_s3.sh date_dim 1 q9495 $name_suffix

        bash upload_s3.sh catalog_sales 300 q16 $name_suffix
        bash upload_s3.sh catalog_returns 240 q16 $name_suffix
        bash upload_s3.sh customer_address 240 q16 $name_suffix
        bash upload_s3.sh call_center 1 q16 $name_suffix
        bash upload_s3.sh date_dim 1 q16 $name_suffix

        bash upload_s3.sh date_dim 120 q1 $name_suffix
        bash upload_s3.sh store_returns 240 q1 $name_suffix
        bash upload_s3.sh store 120 q1 $name_suffix
        bash upload_s3.sh customer 240 q1 $name_suffix
    fi

    rm -rf $repo_path/data-processing/log
elif [[ $mode == "sched" ]]; then
    cd $repo_path/scripts && bash ./build.sh && cd -
elif [[ $mode == "exec" ]]; then
    cd $repo_path/scripts && bash ./build.sh && cd -
else
    echo "Usage $0 <data_size> <unique_name_suffix> <split|sched|exec>"
    exit 1
fi
