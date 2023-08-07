#!/bin/bash

# assume we are in the Ditto/scripts directory 

repo_name=Ditto 

cd ../build
rm -rf *
cmake ..
make -j 4

# remove logs
cd ../logs
rm -rf *
cd -
