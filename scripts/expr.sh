#!/bin/bash

# Must be executed at the scripts directory on the scheduler node

name_suffix=10g
redis_endpoint=jc-redis.5rjkoi.ng.0001.use1.cache.amazonaws.com
redis_port=6379

if [ ! $# -eq 3 ] && [ ! $# -eq 4 ]; then
    echo "Usage $0 <s3|redis> <setting_id> <ditto|nimble|group|dop> (optional) <jct|cost>"
    exit 1
fi

if [[ $1 == "s3" ]]; then
    mode="s3"
    qid="95"
    if [[ $2 == "1" ]]; then
        qid="1"
    elif [[ $2 == "2" ]]; then
        qid="16"
    elif [[ $2 == "3" ]]; then
        qid="94"
    fi
    # clean up the intermediate data
    aws s3 rm s3://tpcds-inter$qid$name_suffix --recursive
    # copy the setting configuration file
    if [[ $2 == "1" || $2 == "2" || $2 == "3" || $2 == "4" || $2 == "11" ]]; then 
        cp ../configs/setting-zipf-0.9.txt ../configs/setting.txt
    elif [[ $2 == "5" ]]; then 
        cp ../configs/setting-uni-100.txt ../configs/setting.txt
    elif [[ $2 == "6" ]]; then
        cp ../configs/setting-uni-75.txt ../configs/setting.txt
    elif [[ $2 == "7" ]]; then
        cp ../configs/setting-uni-50.txt ../configs/setting.txt
    elif [[ $2 == "8" ]]; then
        cp ../configs/setting-uni-25.txt ../configs/setting.txt
    elif [[ $2 == "9" ]]; then
        cp ../configs/setting-norm-1.0.txt ../configs/setting.txt
    elif [[ $2 == "10" ]]; then
        cp ../configs/setting-norm-0.8.txt ../configs/setting.txt
    elif [[ $2 == "12" ]]; then
        cp ../configs/setting-zipf-0.99.txt ../configs/setting.txt
    fi
    sed -i -e "11s/.*/external S3/" ../configs/setting.txt

    if [[ $3 == "ditto" ]]; then
        sed -i -e "4s/.*/algorithm elastic/" ../configs/setting.txt
    elif [[ $3 == "nimble" ]]; then
        sed -i -e "4s/.*/algorithm new_nimble/" ../configs/setting.txt
    elif [[ $3 == "group" ]]; then
        sed -i -e "4s/.*/algorithm new_greedy/" ../configs/setting.txt
    elif [[ $3 == "dop" ]]; then
        sed -i -e "4s/.*/algorithm elastic_nimble/" ../configs/setting.txt
    elif [[ $3 == "fixed" ]]; then
        sed -i -e "4s/.*/algorithm nimble/" ../configs/setting.txt
    fi

    if [[ $4 == "jct" ]]; then
        sed -i -e "5s/.*/mode JCT/" ../configs/setting.txt
    elif [[ $4 == "cost" ]]; then
        sed -i -e "5s/.*/mode cost/" ../configs/setting.txt
    fi

    # ensure that the executors are ready
    bash ./run.sh sched $qid
    
elif [[ $1 == "redis" ]]; then
    mode="redis"
    qid="95"
    if [[ $2 == "1" ]]; then
        qid="1"
    elif [[ $2 == "2" ]]; then
        qid="16"
    elif [[ $2 == "3" ]]; then
        qid="94"
    elif [[ $2 == "5" ]]; then
        qid="1"
    elif [[ $2 == "6" ]]; then
        qid="16"
    elif [[ $2 == "7" ]]; then
        qid="94"
    fi

    # clean up the intermediate data
    redis-cli -h $redis_endpoint -p $redis_port KEYS "q*" | xargs redis-cli -h $redis_endpoint -p $redis_port DEL

    cp ../configs/setting-zipf-0.9.txt ../configs/setting.txt
    sed -i -e "11s/.*/external Redis/" ../configs/setting.txt

    if [[ $3 == "ditto" ]]; then
        sed -i -e "4s/.*/algorithm elastic/" ../configs/setting.txt
    elif [[ $3 == "nimble" ]]; then
        sed -i -e "4s/.*/algorithm new_nimble/" ../configs/setting.txt
    fi

    if [[ $4 == "jct" ]]; then
        sed -i -e "5s/.*/mode JCT/" ../configs/setting.txt
    elif [[ $4 == "cost" ]]; then
        sed -i -e "5s/.*/mode cost/" ../configs/setting.txt
    fi

    # ensure that the executors are ready
    bash ./run.sh sched $qid
else 
    echo "Usage $0 <s3|redis> <setting_id>"
    exit 1
fi