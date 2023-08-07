# 0. Content

The `profiles` directory contains the profiling results of the queries. By default, Ditto will read the profiling data from the `s3` and `redis` directories, depending on the external storage system used. 

Currently, the `s3` and `redis` directories contain the results from the 10GB dataset. If you conduct your own experiments, you have the option to overwrite these results. For evaluation purposes, we also provide the s3-1t and redis-100g directories, which contain results from the 1TB dataset on S3 and the 100GB dataset on Redis, respectively. If you wish to reproduce our paper's results but lack the time (exceeding 24 hours) to profile the queries again, you can directly copy the results to their corresponding directories.