# Source code of Ditto

## 0. Content

- **src/** contains the source code of Ditto. The content is organized as follows: 

### Shared memory 

- **spright_wrapper.cpp** wraps the shared memory communication interface of [SPRIGHT](https://dl.acm.org/doi/abs/10.1145/3544216.3544259) based on the DPDK multi-process communication support. 
- **shm_mgr.cpp** implements the shared memory management module, which allocates and frees the shared memory regions on each function server.

### Data analytics engine

- **operator.cpp** implements the SQL operators, including join, groupby, projection, sort, etc.

### Workload

- **query.cpp** implements the four TPC-DS queries (Q1, Q16, Q94, Q95) used in our experiments.

### Scheduling

- **scheduler.cpp** implements the elastic parallelism scheduler of Ditto, which takes the job DAG and the user goal as input and performs the joint optimization of the parallelism configuration and task placement. It then sends the scheduling requests to the function servers and waits for the scheduling responses. It also implements the profiling module, which collects the profiling information of the tasks and builds the execution time model for the job DAG. 

### Execution

- **executor.cpp** implements the executor of Ditto, which receives the scheduling requests from the scheduler and executes the corresponding tasks. It monitors the execution status of the tasks and sends the scheduling responses to the scheduler.