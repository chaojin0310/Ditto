# 0. Introduction

This repository contains the artifacts for our SIGCOMM'23 paper [Ditto: Efficient Serverless Analytics with Elastic Parallelism](https://chaojin0310.github.io//files/SIGCOMM23-Ditto.pdf).

Ditto is a serverless analytics system that exploits fine-grained elastic parallelism to achieve efficient and cost-effective execution for DAG-like data analytics jobs. 

# 1. Content

- **include/** and **src/** contain the header files and source code of Ditto, respectively. Please refer to [include/README.md](include/README.md) and [src/README.md](src/README.md) for detailed information.
- **scripts/** contains the scripts to setup the environment, build the project, and run Ditto. Please refer to [scripts/README.md](scripts/README.md) for detailed information.
- **data-processing/** contains the code to preprocess the data. Please refer to [data-processing/README.md](data-processing/README.md) for detailed information. 
- **python/** contains the python code to generate the figures in the paper.
- **DSGen/** contains the four SQL queries used in our experiments and the data generator for TPC-DS benchmark, which is adapted from [TPC-DS v3.2.0](https://www.tpc.org/tpcds/). Please refer to [DSGen/README.md](DSGen/README.md) for detailed information.
- **configs/** contains the experiment configuration files. Please refer to [configs/README.md](configs/README.md) for detailed information.
- **profiles/** contains the profiling results of the queries used in our experiments. Please refer to [profiles/README.md](profiles/README.md) for detailed information.

# 2. Artifact evaluation

## 2.1 Hardware and cloud resource requirements
- Users are expected to own an AWS account to run the experiments.
- Experiments in the paper are conducted on AWS. The entire dataset is 1TB, so we deploy the control plane of Ditto on one m6i.4xlarge EC2 instance configured with 16 vCPUs and 64 GB memory and use eight m6i.24xlarge EC2 instances for function execution, with 96 vCPUs and 384 GB memory each. 
- The 1TB-level experiments are very time-consuming and expensive to run, so we provide a **fast and approximate performance comparison for more cost-effective artifact evaluation**. 
In this way, the 10GB version of the TPC-DS dataset is used. We recommend users to deploy **two m6i.8xlarge instances (each with 32 vCPUs, 128 GB memory and 30 GB SSD) for function execution, and one m6i.4xlarge instance with 16 vCPUs, 64 GB memory and 30 GB SSD for scheduling**. Please install **Ubuntu 20.04** on each instance.

## 2.2 Installation
On each server, please follow the steps below to install Ditto.
1. Clone this repository at the home directory: `git clone https://github.com/chaojin0310/Ditto.git && cd Ditto`
2. Install dependencies: `cd scripts && bash ./setup.sh sched` at the scheduler server and `cd scripts && bash ./setup.sh exec` at each function server.
3. Activate the environment: `source ~/.bashrc`
4. Build the project: `bash ./build.sh`
5. Use `aws configure` and `sudo aws configure` to configure your AWS credentials. Please refer to [this page](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html) for detailed information. An example configuration is shown below:
```
$ aws configure
$ AWS Access Key ID [None]: xxxxxxxxxxxxxxxxxxxx
$ AWS Secret Access Key [None]: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
$ Default region name [None]: us-east-1
$ Default output format [None]: json

$ sudo aws configure
$ AWS Access Key ID [None]: xxxxxxxxxxxxxxxxxxxx
$ AWS Secret Access Key [None]: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
$ Default region name [None]: us-east-1
$ Default output format [None]: json
```
Make sure that the sudo user and the normal user have both been configured.

**Note**: If you accidentally restart the function server, please run `sudo sysctl -w vm.nr_hugepages=32768` to enable huge pages again. `32768` is the number of 2 MB huge pages.

## 2.3 Fast evaluation for the 10GB dataset
0. Create several S3 buckets to store the data and profiling results. An example is as follows: 
```
tpcds-raw-q1-10g-<unique_name> (e.g., tpcds-data-10g-chao)
tpcds-raw-q16-10g-<unique_name>
tpcds-raw-q9495-10g-<unique_name>
tpcds-inter1-<unique_name>
tpcds-inter16-<unique_name>
tpcds-inter9495-<unique_name>
tpcds-profiles-10g-<unique_name>
```
To avoid bucket name conflict, you need to add a unique name as suffix. 
1. Generate the raw data: `cd ~/Ditto/scripts && bash ./tpcds_gen.sh 10` at the scheduler server. The data will be stored at directory `~/data/`.
2. Preprocess the data and initialize the environment: `cd ~/Ditto/data-processing && bash ./preprocess.sh 10g <unique_name> split` at the scheduler server. Run `bash ./preprocess.sh 10g <unique_name> exec` at each function server.
3. Connection preparation: open ports 23333 and 23334 on the scheduler server (control plane) and open port 23333 on each function server to allow custom inbound TCP traffic. You can make use of the security group to open the ports. Please refer to [this page](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/authorizing-access-to-an-instance.html) for detailed information. After that, please rewrite the network configuration file `~/Ditto/configs/netconfig.txt` on each server. An example is as follows:
```
num_exec 2
sched_ip 172.31.35.140
sched_ports 23333 23334

exec_addr 172.31.30.31 23333
exec_addr 172.31.21.87 23333 
```
The first line indicates the number of function servers. The second line indicates the **private** IP address of the scheduler server and the ports used for communication. The following lines indicate the IP addresses and ports of the function servers. The order of the list should be consistent with the id of the function servers, e.g., server 0 should be the first one in the list.

**Note**: The IP addresses of servers can be obtained by running `ifconfig` on each server, or by directly checking the AWS EC2 console.

4. Profile the four queries (Q1, Q16, Q94, and Q95): 
- 4.1 `cd ~/Ditto/scripts` on each server. 
- 4.2 Run `bash ./run.sh exec <server_id>` on each function server (e.g., `bash ./run.sh exec 0` at server 0). 
- 4.3 Run `bash ./run.sh sched <query_id> profile` at the scheduler server (e.g., `bash ./run.sh sched 1 profile`). The profiling results will be stored in the S3 bucket `tpcds-profiles-10g-<unique_name>`. 
- 4.4 Repeat step 4.2 and 4.3 for each query. 

5. Build the performance models for the queries: 
- 5.1 Run `sudo bash clean.sh 11` at the scheduler server to clean logs for Figure 11.
- 5.2 For each query, run `bash ./run.sh sched <query_id> aggregate` at the scheduler server. The results will be stored in `~/Ditto/profiles/`. 

6. Run the S3 experiments: run `sudo bash ./clean.sh 8` at the scheduler server to clean logs.
Below are the steps to run one bar or two bars in the paper figures.
- 6.1 On each function server, at the `scripts` directory, run `bash ./run.sh exec <server_id>`. Make sure that the function servers are ready to execute the functions, i.e., the `exec` tmux session is running. If not, just wait for about 1 minute to let the port 23333 be free and then run the command again. (Note: if the `~/Ditto/logs/executor.INFO` log is generated as soon as you run the command, it means that the function server is not ready.)
- 6.2 On the scheduler server, at the `scripts` directory, run `bash ./expr.sh s3 <setting_id> <ditto|nimble> (optional) <jct|cost>)`. The `setting_id` field can be `1` to `12`, which represents the `setting_id`-th bar's setting in Figure 8-9, from left to right across all subfigs. 

For Figure 8 and 9, you need to run step 6.1 and 6.2 for each `setting_id` from `1` to `12` with the following command:
```
for i in {1..12}; do 
    On function servers: bash ./run.sh exec 0; bash ./run.sh exec 1;
    On scheduler server: bash ./expr.sh s3 $i nimble

    # Wait 2 minutes
    On function servers: bash ./run.sh exec 0; bash ./run.sh exec 1;
    On scheduler server: bash ./expr.sh s3 $i ditto jct

    # Wait 2 minutes
    On function servers: bash ./run.sh exec 0; bash ./run.sh exec 1;
    On scheduler server: bash ./expr.sh s3 $i ditto cost

    # Wait 2 minutes
done
```

For Figure 12, you need to run step 6.1 and 6.2 for `setting_id` from `1` to `4` with the complementary algorithms NIMBLE+Group and NIMBLE+DoP, respectively, i.e., run the following commands:
```
for i in {1..4}; do 
    On function servers: bash ./run.sh exec 0; bash ./run.sh exec 1;
    On scheduler server: bash ./expr.sh s3 $i group

    # Wait 2 minutes
    On function servers: bash ./run.sh exec 0; bash ./run.sh exec 1;
    On scheduler server: bash ./expr.sh s3 $i dop jct

    # Wait 2 minutes
    On function servers: bash ./run.sh exec 0; bash ./run.sh exec 1;
    On scheduler server: bash ./expr.sh s3 $i dop cost

    # Wait 2 minutes
done
```

For Figure 15, you need to run step 6.1 and 6.2 twice, one for Ditto and one for fixed parallelism. After each run, you need to upload the breakdown logs to S3., i.e., run the following commands:
```
# The first run
On function servers: bash ./run.sh exec 0; bash ./run.sh exec 1;
On scheduler server: bash ./expr.sh s3 4 fixed
# After execution...
On each function server: bash ./breakdown.sh upload fixed
```
```
# The second run
On function servers: bash ./run.sh exec 0; bash ./run.sh exec 1;
On scheduler server: bash ./expr.sh s3 4 ditto jct
# After execution...
On each function server: bash ./breakdown.sh upload ditto
```
Finally, download the breakdown logs from S3 to `~/Ditto/results/` to the scheduler server, i.e., run the following commands:
```
bash ./breakdown.sh download fixed
bash ./breakdown.sh download ditto
```

7. Run the Redis experiment. 
- 7.0 Set up the Redis server in AWS Elasticache and get the hostname of your Redis server. Please refer to [data-processing/README.md](data-processing/README.md) for detailed information. 
- 7.1 On each server, run `cd ~/Ditto/data-processing && bash ./update_redis.sh <redis_hostname>`. The hostname is `endpoint:port` of your Redis server, e.g., `jc-redis.5rjkoi.ng.0001.use1.cache.amazonaws.com:6379`. Then on the scheduler server, run `bash ./upload_redis.sh` to upload the raw data from S3 to Redis. Modify the 6-th line of `~/Ditto/scripts/expr.sh` to `redis_endpoint=<endpoint>`, e.g., `redis_endpoint=jc-redis.5rjkoi.ng.0001.use1.cache.amazonaws.com`.
- 7.2 Profile the queries: refer to step 4 (Profile the four queries) above.
- 7.3 Build the performance models: refer to step 5.2 (Build the performance models for the queries) above. **Note**: do **NOT** run `sudo bash clean.sh 11`.
- 7.4 Run the Redis experiments: refer to step 6.1 to run the function servers and refer to step 6.2 to run the scheduler server. The `setting_id` field can be `1` to `4`, which represents the `setting_id`-th bar's setting in Figure 10, from left to right across all subfigs. And change the `s3` field to `redis`. The detailed commands are as follows:
```
for i in {1..4}; do 
    On function servers: bash ./run.sh exec 0; bash ./run.sh exec 1;
    On scheduler server: bash ./expr.sh redis $i nimble

    # Wait 2 minutes
    On function servers: bash ./run.sh exec 0; bash ./run.sh exec 1;
    On scheduler server: bash ./expr.sh redis $i ditto jct

    # Wait 2 minutes
    On function servers: bash ./run.sh exec 0; bash ./run.sh exec 1;
    On scheduler server: bash ./expr.sh redis $i ditto cost

    # Wait 2 minutes
done
```

8. Get the experimental results: enter the `python` directory (`cd ~/Ditto/python`).
- Run `python3 fig8.py` to generate Figure 8 and Table 1 in the paper.
- Run `python3 fig9.py` to generate Figure 9 in the paper.
- Run `python3 fig10.py` to generate Figure 10 in the paper.
- Run `python3 fig11.py` to generate Figure 11 and Table 2 in the paper.
- Run `python3 fig12.py` to generate Figure 12 in the paper.
- Run `python3 fig14.py` to generate Figure 14 in the paper.
- Run `python3 fig15.py` to generate Figure 15 in the paper.

## 2.4 Evaluation for the 1TB dataset
Currently, we do not publicly release the 1TB dataset due to the storage and external I/O cost. 
If you are interested and have enough budget, please contact us to get the preprocessed 1TB dataset for limited time use.

# 3. Contact

For any question, please contact Chao Jin (`chaojin at pku dot edu dot cn`).

# 4. Reference

```
@inproceedings{jin2023ditto,
  title={Ditto: Efficient Serverless Analytics with Elastic Parallelism},
  author={Jin, Chao and Zhang, Zili and Xiang, Xingyu and Zou, Songyun and Huang, Gang and Liu, Xuanzhe and Jin, Xin},
  booktitle={Proceedings of the ACM SIGCOMM 2023 Conference},
  pages={406--419},
  year={2023}
}
```
