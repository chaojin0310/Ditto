#!/bin/bash

# Must be run at the data-processing directory

repo_path=..

sed -i -e "26s/.*/const std::string redis_addr = \"$1\";/" ./uploader.cpp
sed -i -e "69s/.*/const std::string redis_addr = \"$1\";/" $repo_path/include/io.hpp
sed -i -e "11s/.*/external Redis/" $repo_path/configs/setting.txt

cd $repo_path/scripts && bash ./build.sh && cd -
