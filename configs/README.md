# 0. Content

The `configs` directory contains the experiment configuration files. 
- `setting.txt` will be read by the scheduler and function servers. It contains the following important information:
  - `proc_step`: the processing step. Options are `profile`, `aggregate` and `algorithm`, which means profiling, performance model building, and run a specific algorithm, respectively.
  - `algorithm`: the algorithm to run. Options are `elastic` (Ditto), `new_nimble` (NIMBLE). This field is only used when `proc_step` is `algorithm`.
  - `mode`: the objective that the user concern about. Options are `jct` and `cost`. This field is only used when `proc_step` is `algorithm` and `algorithm` is `elastic`.
  - `distribution`: the distribution of the number of function slots on each function server. It is a list of integers separated by space. The length of the list should be equal to the number of function servers. 
  - `external`: the type of external storage. Options are `S3` and `Redis`.

The `netconfig.txt` file describes the network configuration. It will be read by the scheduler and function servers. The format is as mentioned in the root [README.md](../README.md).

The `dsq*.txt` files are the DAG description files for the foru queries.

Currently, the configuration files are only used for the 10GB dataset. For the 1TB dataset, the configuration files are hard-coded in the `configs-1t` directory. Users need to copy the files to the `configs` directory before running the 1TB experiments.
