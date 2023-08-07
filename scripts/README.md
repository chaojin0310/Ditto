## 0. Content

- `dpdk_install.sh`, `aws_sdk_install.sh`, `glog_install.sh`, `redis_install.sh` are the scripts to install DPDK, AWS SDK, Google Logging, and Redis frontend for C++, respectively.

- `bash ./tpcds_gen.sh <scale>` is used to generate the TPC-DS dataset. `scale` can be set to 1, 10, 100 or 1000. Tables (flat data files) are stored at `$HOME/data/`. 

- `bash ./setup.sh <sched/exec>` is used to setup the environment. `sched` is used to setup the control plane and `exec` is used to setup the function execution plane.

- `bash ./build.sh` is used to build Ditto. 

- `run.sh` is used to run Ditto. `bash ./run.sh sched <query_id> (optional) <profile|aggregate>` is used to run the scheduler. `bash ./run.sh exec <server_id>` is used to run the function server.

- `expr.sh` is used to run the experiments. `bash ./expr.sh <s3|redis> <setting_id> <ditto|nimble> (optional) <jct|cost>` is used to run the experiments for S3 and Redis, respectively. `setting_id` can be set to 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, following the order from left to right of the x-axis of all subfigures in Figure 11 in the paper. `ditto` and `nimble` are used to indicate the scheduling algorithms. `jct` and `cost` are used to indicate the objective functions, which is read by Ditto.

- `clean.sh` is used to clean some temporary logs before running the experiments. `sudo bash ./clean.sh <fig_id>` is used to clean the logs before running the experiment for figure `fig_id`.
