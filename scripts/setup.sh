#!/bin/bash

repo_name=Ditto
# cd $HOME/${repo_name}
cd ..
mkdir -p build
mkdir -p logs
mkdir -p profiles
mkdir -p results
mkdir -p profiles/s3
mkdir -p profiles/redis

cd scripts
./dpdk_install.sh $1

./aws_sdk_install.sh

./glog_install.sh

./redis_install.sh

pip3 install numpy scipy matplotlib

cd $HOME
echo "alias compile='rm -rf * && cmake .. && make -j 4'" >> ~/.bashrc
echo "alias rmlogs='cd ../logs && rm -rf * && cd -'" >> ~/.bashrc
echo "please run source ~/.bashrc"
