# Data preprocessing

## 0. Content

**splitter.cpp** is the code to split each individual table into multiple equal-sized files. 

**uploader.cpp** is the code to download the partitioned data from S3 and upload the data to Redis.

## 1. Elasticache Redis server setup

0. Create a security group for your redis cluster. In the security group, create an inbound rule to allow TCP connections from any host to port 6379. The detailed steps are as follows:
    + In Amazon EC2, select `Security Groups` on the left panel, and click on `Create security group` on the right. Let the name to be `redis-sg` and the description to be `security group for redis`.
    + The most important thing here's to **create an inbound rule** in the setting of that security group.
    + The contents of the inbound rule: Custom TCP, port 6379; for `source`, choose `custom` and type `0.0.0.0/0`, which stands for any possible host.
    + This rule makes sure that all TCP connections to your redis cluster (which uses port 6379 by default) will not be rejected.
1. In Amazon Elasticache, select `Redis clusters` on the left panel, and click on `Create Redis cluster` on the right.
2. In `Choose a cluster creation method`, choose `Configure and create a new cluster`, the one in the middle. Disable cluster mode. Type any name and description you like.
3. In `Cluster settings`, choose a machine with sufficient memory. For the 10GB dataset (i.e., the fast evaluation), we use `cache.r7g.2xlarge` with ~50 GB memory. For the 100GB dataset (i.e., the evaluation in our paper), we use `cache.r5.4xlarge` with ~100 GB memory. The port number should be 6379.
4. In `Connectivity`, create a new subnet group. Choose the default VPC or create a new one. Ensure that the VPC shares the same subnet prefix as the EC2 instances you are running.
 Leave the rest options as default and click on `Next` in the bottom.
5. For `Security`, choose the security group (e.g., named `redis-sg`) you created in the step 0. Leave the rest as default and create the cluster. After a few minutes, the cluster will be ready to use.

## 2. Get the Redis server hostname

The hostname of your redis server can be acquired by clicking on the cluster name in the `Redis clusters` page. It is the value of **primary endpoint**. Typically, the hostname is composed of endpoint url and port number, i.e., `endpoint:port`.
