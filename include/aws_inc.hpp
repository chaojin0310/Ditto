/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

/* This file contains some basic I/O functions of Aws::S3.
 * It is deprecated due to poor performance, caused by the race condition of writing to
 * the same disk. 
 * Find the new version in io.hpp.
 */

#ifndef AWS_INC_HPP
#define AWS_INC_HPP

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/s3/S3Client.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>

#include "utils.hpp"


bool GetObject(const Aws::String &bucketName, const Aws::String &objectName, 
                const Aws::String &fileName) {
    Aws::S3::Model::GetObjectRequest request;
    Aws::S3::S3Client client;

    request.SetBucket(bucketName);
    request.SetKey(objectName);

    Aws::S3::Model::GetObjectOutcome outcome =
            client.GetObject(request);
        
    // get the content of the object
    if (outcome.IsSuccess()) { 
        Aws::OFStream local_file;
        local_file.open(fileName.c_str(), std::ios::out | std::ios::binary);
        local_file << outcome.GetResult().GetBody().rdbuf();
        local_file.close();
        // std::cout << "Get Object" << objectName << " Done!" << std::endl;
        return true;
    } else {
        std::cerr << "GetObject error: " << outcome.GetError().GetExceptionName()
            << " " << outcome.GetError().GetMessage() << std::endl;
        return false;
    }

    return outcome.IsSuccess();
}

bool PutObject(const Aws::String &bucketName, const Aws::String &objectName, 
                const Aws::String &fileName) {
    Aws::S3::S3Client s3_client;

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(objectName);

    std::shared_ptr<Aws::IOStream> inputData =
            Aws::MakeShared<Aws::FStream>("SampleAllocationTag",
                                          fileName.c_str(),
                                          std::ios_base::in | std::ios_base::binary);

    if (!*inputData) {
        std::cerr << "Error unable to read file " << fileName << std::endl;
        return false;
    }

    request.SetBody(inputData);

    Aws::S3::Model::PutObjectOutcome outcome =
            s3_client.PutObject(request);

    if (!outcome.IsSuccess()) {
        std::cerr << "Error: PutObject: " <<
                  outcome.GetError().GetMessage() << std::endl;
    } else {
        // std::cout << "Added object '" << objectName << "' to bucket '"
        //           << bucketName << "'." << std::endl;
    }

    return outcome.IsSuccess();
}

bool GetObjectBuffer(const Aws::String &bucketName, const Aws::String &objectName, 
                const Aws::String &fileName) {
    Aws::S3::Model::GetObjectRequest request;
    Aws::S3::S3Client client;

    request.SetBucket(bucketName);
    request.SetKey(objectName);

    Aws::S3::Model::GetObjectOutcome outcome =
            client.GetObject(request);
        
    return outcome.IsSuccess();
}

bool PutObjectBuffer(const Aws::String &bucketName, const Aws::String &objectName,
    const std::string &objectContent) {
    
    Aws::S3::S3Client s3_client;

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(objectName);

    const std::shared_ptr<Aws::IOStream> inputData = Aws::MakeShared<Aws::StringStream>("");
    *inputData << objectContent.c_str();

    request.SetBody(inputData);

    Aws::S3::Model::PutObjectOutcome outcome = s3_client.PutObject(request);

    if (!outcome.IsSuccess()) {
        std::cerr << "Error: PutObjectBuffer: " <<
                  outcome.GetError().GetMessage() << std::endl;
        return false;
    }
    else {
        std::cout << "Success: Object '" << objectName << "' with content '"
                  << objectContent << "' uploaded to bucket '" << bucketName << "'.";
        return true;
    }
}

bool PutObjectBuffer(const Aws::String &bucketName, const Aws::String &objectName,
    const Aws::StringStream &objectContent) {
    
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = "us-east-1";
    Aws::S3::S3Client s3_client(clientConfig);

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(objectName);

    const std::shared_ptr<Aws::IOStream> inputData =
            Aws::MakeShared<Aws::StringStream>("");
    // convert Aws::StringStream to shared_ptr<Aws::IOStream>

    *inputData << objectContent.rdbuf();

    request.SetBody(inputData);

    Aws::S3::Model::PutObjectOutcome outcome = s3_client.PutObject(request);

    if (!outcome.IsSuccess()) {
        std::cerr << "Error: PutObjectBuffer: " <<
                  outcome.GetError().GetMessage() << std::endl;
        return false;
    }
    else {
        // std::cout << "Success: Object '" << objectName << "' with content '" 
        //    << objectContent << "' uploaded to bucket '" << bucketName << "'.";
        return true;
    }
}

void _get_multiple_splits_from_s3(const Aws::String &objectNamePrefix, const Aws::String &bucketName, 
    const Aws::String &fileNamePrefix, int start_id, int num_splits) {
    for (int i = start_id; i < start_id + num_splits; i++) {
        Aws::String fileName = fileNamePrefix + "_" + std::to_string(i) + ".dat";
        Aws::String objectName = objectNamePrefix + "_" + std::to_string(i) + ".dat";
        if (!GetObject(bucketName, objectName, fileName)) {
            std::cerr << "Failed to get object " << objectName + std::to_string(i);
            exit(1);
        }
    }
}

void _get_multiple_splits_threads(const Aws::String &objectNamePrefix, const Aws::String &bucketName, 
    const Aws::String &fileNamePrefix, int start_id, int num_splits) {
    std::vector<std::thread> threads;
    for (int i = start_id; i < start_id + num_splits; i++) {
        Aws::String fileName = fileNamePrefix + "_" + std::to_string(i) + ".dat";
        Aws::String objectName = objectNamePrefix + "_" + std::to_string(i) + ".dat";
        threads.push_back(std::thread(GetObject, bucketName, objectName, fileName));
    }
    for (auto &t : threads) {
        t.join();
    }
}

#endif /* AWS_INC_HPP */