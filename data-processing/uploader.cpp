/*
 * Copyright (c) Computer Systems Research Group @ PKU (xingyu xiang)

 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/s3/S3Client.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/Object.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>

#include <glog/logging.h>
#include <sw/redis++/redis++.h>

#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>

const std::string redis_addr = "jc-redis.5rjkoi.ng.0001.use1.cache.amazonaws.com:6379";

int main(int argc, char *argv[]) {
  FLAGS_alsologtostderr = true;
  google::InitGoogleLogging(argv[0]);
  FLAGS_colorlogtostderr = true;
  FLAGS_log_dir = "../logs";

  Aws::SDKOptions options;
  Aws::InitAPI(options);
  Aws::Vector<Aws::S3::Model::Object> objects;
  Aws::S3::S3Client client;
  Aws::S3::Model::ListObjectsRequest list_request;

  Aws::String bucketName = argv[1];

  list_request.SetBucket(bucketName);
  auto outcome = client.ListObjects(list_request);
  if (!outcome.IsSuccess()) {
    std::cerr << "Error: ListObjects: " <<
          outcome.GetError().GetMessage() << std::endl;
    return -1;
  } else { 
    objects = outcome.GetResult().GetContents();
  }

  for (Aws::S3::Model::Object &object: objects) {
    Aws::String objectName = object.GetKey();

    LOG(INFO) << "Start to read " << objectName << " from S3";

    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(objectName);
    Aws::S3::Model::GetObjectOutcome outcome = client.GetObject(request);
    if (!outcome.IsSuccess()) {
      std::cerr << "GetObject error: " << outcome.GetError().GetExceptionName()
        << " " << outcome.GetError().GetMessage() << std::endl;
      return -1;
    }
    Aws::IOStream &awss = outcome.GetResult().GetBody();
    std::stringstream ss;
    ss << awss.rdbuf();

    LOG(INFO) << "Read from S3 finished";

    LOG(INFO) << "Start to write " << objectName << " to vector<string>";
    
    std::vector<std::string> strs;
    strs.clear();
    
    std::string buffer, tmp;
    while (getline(ss, buffer)) {
      tmp += buffer;
      tmp += '\n';
      if (tmp.length() > 400 * 1024 * 1024) {
        strs.push_back(tmp);
        tmp.clear();
      }
    }
    if (tmp.length() > 0) {
      strs.push_back(tmp);
    }

    LOG(INFO) << "Write to vector<string> finished";

    LOG(INFO) << "Start to write " << objectName << " to Redis";
    auto redis = sw::redis::Redis("tcp://" + redis_addr);
    std::string redisName = objectName;
    redis.del(redisName);
    redis.rpush(redisName, strs.begin(), strs.end());
    LOG(INFO) << "Write to Redis finished";
  }

  Aws::ShutdownAPI(options);
  return 0;
}
