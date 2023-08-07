# Header files of Ditto

## 0. Content

**include/** contains the header files of Ditto. The content is organized as follows:

### Shared memory

- **spright_inc.hpp** contains the headers of [SPRIGHT](https://dl.acm.org/doi/abs/10.1145/3544216.3544259), a serverless framework that provides shared memory communication among co-located functions. 

### Communication

- **aws_inc.hpp** contains some basic I/O functions of Aws::S3.
- **io.hpp** contains the I/O functions, including the functions to list, read and write data through S3, Redis and local shared memory. It also provides a transparent API for data communication through these three storage systems.
- **net.hpp** contains basic network communication headers.

### Data analytics engine

- **r_array.hpp** and **sh_array.hpp** define the data structures of private and shared arrays, respectively. The customed arrays behave like STL and is adapted to fit with the requirements of the DPDK hugepage memory management.
- **table.hpp** and **sh_table.hpp** define the data structures of private and shared tables, respectively. The tables are implemented on top of the customed arrays and store the data in the form of columns. 
- **schema.hpp** contains different table schemas of the TPC-DS benchmark.
- **operator.hpp** contains the data structures used in SQL operators.
- **query.hpp** defines the invocation interface of TPC-DS queries.

### Scheduling

- **scheduler.hpp** implements the DoP ratio computing algorithm, the greedy grouping algorithm and the joint optimization algorithm of Ditto, as well as the baseline algorithms (e.g., [NIMBLE](https://www.usenix.org/conference/nsdi21/presentation/zhang-hong)). It also implements Ditto's performance model and the execution time predictor.
- **task.hpp** contains the metadata of stages, tasks and profiles.
- **utils.hpp** contains some utility functions, including the functions to get the duration of a time interval, to set CPU affinity and to robustly transfer a scheduling request.
