#!/bin/bash

cd $HOME
git clone https://github.com/google/glog.git
cd glog
cmake -S . -B build -G "Unix Makefiles"
cmake --build build
cmake --build build --target test
sudo cmake --build build --target install