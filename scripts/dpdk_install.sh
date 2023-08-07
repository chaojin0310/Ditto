#!/bin/bash

cd $HOME

sudo apt update
sudo apt install -y flex bison build-essential dwarves libssl-dev libelf-dev \
    libnuma-dev pkg-config python3-pip python3-pyelftools \
    libconfig-dev gcc-multilib uuid-dev sysstat net-tools
sudo pip3 install meson ninja

git clone --single-branch git://dpdk.org/dpdk
cd dpdk
git switch --detach v21.11
meson build
cd build
ninja
sudo ninja install
sudo ldconfig

if [[ $1 == "exec" ]]; then
    echo "Set up hugepages"
    # sudo sysctl -w vm.nr_hugepages=81920
    sudo sysctl -w vm.nr_hugepages=32768
fi

cd $HOME
