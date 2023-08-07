#!/bin/bash

# Must be executed in the scripts directory

if [ $# -eq 3 ]; then
    mode=$3
elif [ $# -eq 2 ]; then
    mode="exec"
else
    echo "Usage: ./run.sh <sched|exec> <config_file> (optional) [profile|aggregate]"
    exit 1
fi

repo_path=$HOME/Ditto

if [[ $mode == "profile" ]]; then
    sed -i -e '3s/.*/proc_step profile/' $repo_path/configs/setting.txt
elif [[ $mode == "aggregate" ]]; then
    sed -i -e '3s/.*/proc_step aggregate/' $repo_path/configs/setting.txt
fi

if [[ $1 == "sched" ]]; then
    echo "Running Ditto scheduler"
    cd $repo_path/build
    if tmux has-session -t sched 2>/dev/null; then
        tmux kill-session -t sched
    fi
    tmux new-session -s sched "sudo ./scheduler $2"
else
    echo "Running Ditto executor"
    cd $repo_path/build
    if tmux has-session -t shm 2>/dev/null; then
        tmux kill-session -t shm
    fi
    if tmux has-session -t exec 2>/dev/null; then
        tmux kill-session -t exec
    fi
    tmux new-session -d -s shm "sudo ./shm"
    tmux new-session -d -s exec "sudo ./executor $2"
fi