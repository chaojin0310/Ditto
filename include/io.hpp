/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#ifndef IO_HPP
#define IO_HPP

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

#include "spright_inc.hpp"
#include "table.hpp"
#include "shtable.hpp"
#include "task.hpp"
#include "utils.hpp"

enum class ExternalType {
	S3,
	Redis
};

// modify this variable to switch between S3 and Redis
ExternalType external_storage_type = ExternalType::S3; 

/* Current version */
const Aws::String rawBucketName95 = "tpcds-raw-q9495-10g";
const Aws::String rawBucketName94 = "tpcds-raw-q9495-10g";
const Aws::String rawBucketName16 = "tpcds-raw-q16-10g";
const Aws::String rawBucketName1 = "tpcds-raw-q1-10g";
const Aws::String interBucketName95 = "tpcds-inter95";
const Aws::String interBucketName94 = "tpcds-inter94";
const Aws::String interBucketName16 = "tpcds-inter16";
const Aws::String interBucketName1 = "tpcds-inter1";
const Aws::String rawDir = "";
const Aws::String interDir95 = "";
const Aws::String interDir94 = "";
const Aws::String interDir16 = "";
const Aws::String interDir1 = "";

const Aws::String logBucketName = "tpcds-profiles-10g";
const std::string logPath = "../logs/";
const std::string profilePath = "../profiles/";

/* For test */
const int web_sales_chunks = 40;
const int web_returns_chunks = 6;
const int customer_address_chunks = 6;
const int catalog_sales_chunks = 60;
const int catalog_returns_chunks = 6;
const int date_dim_chunks = 6;
const int store_chunks = 6;
const int store_returns_chunks = 60;
const int customer_chunks = 60;

const std::string redis_addr = "jc-redis.5rjkoi.ng.0001.use1.cache.amazonaws.com:6379";

int wait_time = 10000;  // for profile

struct IORes {
    bool res; // true for table, false for shtable
    time_t tgp;  // time for get or put
    time_t tsd;  // time for serializing or deserializing
    IORes() : res(true), tgp(0), tsd(0) {}
    IORes(IORes &other) : res(other.res), tgp(other.tgp), tsd(other.tsd) {}
};

struct ChunksToTransfer {
	bool res;
	int start_id;
	int num_chunks;
	ChunksToTransfer() : res(false), start_id(0), num_chunks(0) {}
	ChunksToTransfer(bool r, int _start_id, int _num_chunks) : 
		res(r), start_id(_start_id), num_chunks(_num_chunks) {}
	ChunksToTransfer(const ChunksToTransfer &other) {
		res = other.res;
		start_id = other.start_id;
		num_chunks = other.num_chunks;
	}
};

ChunksToTransfer compute_chunks_to_read(Transfer &trans) {
	ChunksToTransfer chunks_to_read;

	int input_chunks = trans.from_stage.num_tasks;
	int num_tasks = trans.to_stage.num_tasks;
	int task_id = trans.task_id;
	
	if (trans.op == TransferOp::AllGather) {
		chunks_to_read.res = true;
		chunks_to_read.start_id = 0;
		chunks_to_read.num_chunks = input_chunks;
	} else if (trans.op == TransferOp::Gather) {
		if (input_chunks < num_tasks) {
			// LOG(FATAL) << "input_chunks < num_tasks " << input_chunks << " " << num_tasks;
			// return chunks_to_read;
            num_tasks = input_chunks;
		}
		chunks_to_read.res = true;
		int chunks_per_task = input_chunks / num_tasks;
		int chunks_left = input_chunks % num_tasks;
		if (task_id < chunks_left) {
			chunks_to_read.start_id = task_id * (chunks_per_task + 1);
			chunks_to_read.num_chunks = chunks_per_task + 1;
		} else {
			chunks_to_read.start_id = chunks_left * (chunks_per_task + 1) + 
				(task_id - chunks_left) * chunks_per_task;
			chunks_to_read.num_chunks = chunks_per_task;
		}
	} else if (trans.op == TransferOp::Shuffle) {
		chunks_to_read.res = true;
		chunks_to_read.start_id = task_id * input_chunks;
		chunks_to_read.num_chunks = input_chunks;
	} else if (trans.op == TransferOp::Broadcast) {
        chunks_to_read.res = true;
        chunks_to_read.start_id = task_id;
        chunks_to_read.num_chunks = 1;
    }

	return chunks_to_read;
}

ChunksToTransfer compute_chunks_to_receive(Transfer &trans) {
    ChunksToTransfer chunks_to_write;

    int output_chunks = trans.from_stage.num_tasks;
	int num_tasks = trans.to_stage.num_tasks;
    int task_id = trans.task_id;

    if (trans.op == TransferOp::AllGather) {
        chunks_to_write.res = true;
        chunks_to_write.start_id = 0;
        chunks_to_write.num_chunks = trans.to_stage.num_tasks;
    } else if (trans.op == TransferOp::Gather) {
        chunks_to_write.res = true;
        chunks_to_write.num_chunks = 1;
        int chunks_per_task = output_chunks / num_tasks;
        int chunks_left = output_chunks % num_tasks;
        int s = 0;
        int i = 0;
        for (; i < num_tasks; ++i) {
            if (i < chunks_left) {
                s += chunks_per_task + 1;
            } else {
                s += chunks_per_task;
            }
            if (s > task_id)
                break;
        }
        chunks_to_write.start_id = i;
    } else {  // Shuffle & Broadcast
        chunks_to_write.res = true;
        chunks_to_write.start_id = 0;
        chunks_to_write.num_chunks = trans.to_stage.num_tasks;
    }

    return chunks_to_write;
}


/* Note: ListObjects returns up to 1000 objects */
bool FindTableFromS3(const Aws::String &bucketName, const Aws::String &targetName) {

    Aws::S3::S3Client s3_client;
    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(bucketName);
    auto outcome = s3_client.ListObjects(request);

    if (!outcome.IsSuccess()) {
        std::cerr << "Error: ListObjects: " <<
                  outcome.GetError().GetMessage() << std::endl;
    } else {
        Aws::Vector<Aws::S3::Model::Object> objects =
                outcome.GetResult().GetContents();
        // std::cout << objects.size() << " objects found in bucket " << std::endl;
        for (Aws::S3::Model::Object &object: objects) {
            if (object.GetKey() == targetName) {
                return true;
            }
        }
    }

    return false;
}

IORes GetTableFromS3(const Aws::String &bucketName, 
    const Aws::String &objectName, Table &table) {

    IORes res;
    
    auto start = get_time();
    Aws::S3::Model::GetObjectRequest request;
    Aws::S3::S3Client client;
    request.SetBucket(bucketName);
    request.SetKey(objectName);
    Aws::S3::Model::GetObjectOutcome outcome = 
        client.GetObject(request);
    auto end = get_time();
    res.tgp = get_duration(start, end);
    
    start = get_time();
    if (outcome.IsSuccess()) {
        Aws::IOStream &stream = outcome.GetResult().GetBody();
        res.res = table.readFromStream(stream);
    } else {
        std::cerr << "GetObject error: " << outcome.GetError().GetExceptionName()
            << " " << outcome.GetError().GetMessage() << std::endl;
        res.res = false;
    }
    end = get_time();
    res.tsd = get_duration(start, end);

    return res;
}

IORes PutTableToS3(const Aws::String &bucketName, 
    const Aws::String &objectName, ShTable &table) {

    IORes res;

    auto start = get_time();
    Aws::StringStream ss;
    table.writeToStream(ss);
    const std::shared_ptr<Aws::IOStream> inputData = 
        Aws::MakeShared<Aws::StringStream>("");
    if (table.rows == 0) {
        *inputData << "";
    } else {
        *inputData << ss.rdbuf();
    }
    auto end = get_time();
    res.tsd = get_duration(start, end);

    start = get_time();
    Aws::S3::Model::PutObjectRequest request;
    Aws::S3::S3Client client;
    request.SetBucket(bucketName);
    request.SetKey(objectName);
    request.SetBody(inputData);
    Aws::S3::Model::PutObjectOutcome outcome = client.PutObject(request);
    if (!outcome.IsSuccess()) {
        std::cerr << "Error: PutObjectBuffer: " <<
                  outcome.GetError().GetMessage() << std::endl;
        // Put an empty object to S3
        Aws::StringStream ss2;
        const std::shared_ptr<Aws::IOStream> inputData2 = 
            Aws::MakeShared<Aws::StringStream>("");
        *inputData2 << ss2.rdbuf();
        Aws::S3::Model::PutObjectRequest request2;
        Aws::S3::S3Client client2;
        request2.SetBucket(bucketName);
        request2.SetKey(objectName);
        request2.SetBody(inputData2);
        outcome = client2.PutObject(request);
        if (!outcome.IsSuccess()) {
            std::cerr << "Error: PutObjectBuffer2: " <<
                      outcome.GetError().GetMessage() << std::endl;
        }
        res.res = false;
    }
    end = get_time();
    res.tgp = get_duration(start, end);

    return res;
}

bool FindTableFromRedis(const std::string &objectName) {
    auto redis = sw::redis::Redis("tcp://" + redis_addr + ":6379");
    return redis.exists(objectName);
}

bool GetTableFromRedis(const std::string &objectName, Table &table) {
    auto redis = sw::redis::Redis("tcp://" + redis_addr + ":6379");
    std::vector<std::string> strs;
    strs.clear();
    redis.lrange(objectName, 0, -1, std::back_inserter(strs));
    table.readFromRedis(strs);
    return true;
}

bool PutTableToRedis(const std::string &objectName, ShTable &table) {
    auto redis = sw::redis::Redis("tcp://" + redis_addr + ":6379");
    std::vector<std::string> strs;
    strs.clear();
    table.writeToRedis(strs);
    redis.del(objectName);
    redis.rpush(objectName, strs.begin(), strs.end());
    return true;
}

bool GetExternalTable(const Aws::String &bucketName, 
    const Aws::String &objectName, Table &table) {
    if (external_storage_type == ExternalType::S3) {
        IORes res = GetTableFromS3(bucketName, objectName, table);
    } else if (external_storage_type == ExternalType::Redis) {
        bool res = GetTableFromRedis(objectName, table);
    }
    return true;
}

bool GetTable(Transfer trans, 
    const Aws::String &bucketName, const Aws::String &objectNamePrefix, 
    Table *table, int schema_type, rte_ring **ring) {
    
    ChunksToTransfer chunks = compute_chunks_to_read(trans);
    if (!chunks.res)
        return false;
    
    std::vector<int> external_chunks;
    std::vector<int> local_chunks;
    external_chunks.clear();
    local_chunks.clear();
    int local_start_id = trans.from_stage.task_id_start;
    int local_end_id = local_start_id + trans.from_stage.num_local_tasks;
    int num_local_sender = trans.from_stage.num_local_tasks;
    if (trans.op == TransferOp::Shuffle || trans.op == TransferOp::Broadcast) {
        local_start_id *= trans.to_stage.num_tasks;
        local_end_id *= trans.to_stage.num_tasks;
    }
    for (int i = 0; i < chunks.num_chunks; i++) {
        int chunk_id = chunks.start_id + i;
        if (num_local_sender == 0 || chunk_id < local_start_id || chunk_id >= local_end_id) {
            external_chunks.push_back(chunk_id);
        } else {
            local_chunks.push_back(chunk_id);
        }
    }

    size_t num_local = local_chunks.size();
    size_t num_external = external_chunks.size();
    // LOG(INFO) << "Local chunks: " << num_local << ", external chunks: " << num_external;

    std::vector<ShTable> local_tables;
    local_tables.clear();
    std::vector<Table> external_tables;
    external_tables.clear();
    // Read local chunks
    if (num_local > 0) {
        int recv_ring_id = trans.ring_id;
        for (int i = 0; i < num_local; i++) {
            ShTable *local_tb = nullptr;
            io_rx(ring, (void **)&local_tb, recv_ring_id);
            // LOG(INFO) << "Received local table " << i << " from ring " << recv_ring_id;
            local_tables.push_back(*local_tb);
        }
    }
    // Read external chunks
    if (num_external > 0) {
        if (external_storage_type == ExternalType::S3) {
            for (int i = 0; i < num_external; i++) {
                int chunk_id = external_chunks[i];
                Aws::String objectName = objectNamePrefix + "_" + 
                    std::to_string(chunk_id) + ".dat";
                // LOG(INFO) << objectName;
                Table tb(schema_type);
                auto start = get_time();
                bool found = true;
                while (!FindTableFromS3(bucketName, objectName)) {
                    // LOG(INFO) << "Failed to find table from S3, retrying...";
                    auto end = get_time();
                    if (get_duration(start, end) > wait_time) {
                        found = false;
                        break;
                    }
                }
                if (found) {
                    IORes gres = GetTableFromS3(bucketName, objectName, tb);
                }
                external_tables.push_back(tb);
            }
        } else if (external_storage_type == ExternalType::Redis) {
            for (int i = 0; i < num_external; i++) {
                int chunk_id = external_chunks[i];
                std::string objectName = objectNamePrefix + "_" + 
                    std::to_string(chunk_id) + ".dat";
                // LOG(INFO) << objectName;
                Table tb(schema_type);
                auto start = get_time();
                bool found = true;
                while (!FindTableFromRedis(objectName)) {
                    sleep(1);
                    auto end = get_time();
                    if (get_duration(start, end) > wait_time) {
                        found = false;
                        break;
                    }
                }
                if (found) {
                    bool gres = GetTableFromRedis(objectName, tb);
                }
                external_tables.push_back(tb);
            }
        }
    }

    table->hybrid_multi_combine(local_tables, external_tables);

    if (num_local > 0) {
        int *response = nullptr;
        if (trans.op != TransferOp::Broadcast) {
            for (int i = 0; i < num_local; i++) {
                int chunk_id = local_chunks[i];
                int send_task_id = chunk_id;
                if (trans.op == TransferOp::Shuffle) {
                    send_task_id /= trans.to_stage.num_tasks;
                }
                int send_ring_id = trans.from_stage.ring_id_start + 
                    (send_task_id - trans.from_stage.task_id_start);
                io_tx(ring, (void *)response, send_ring_id);
                // LOG(INFO) << "Sent response to ring " << send_ring_id;
            }
        } else {
            int send_ring_id = trans.from_stage.ring_id_start;
            io_tx(ring, (void *)response, send_ring_id);
            // LOG(INFO) << "Sent response to ring " << send_ring_id;
        }
    }
    if (num_external > 0) {
        for (int i = 0; i < num_external; ++i) {
            external_tables[i].deleteTable();
        }
    }

    return true;
}

bool PutTable(Transfer trans, 
    const Aws::String bucketName, const Aws::String &objectNamePrefix, 
    ShTable *shtable, rte_ring **ring, int schema_type=q1_s3) {

    ChunksToTransfer chunks = compute_chunks_to_receive(trans);
    
    if (trans.op == TransferOp::Gather) {  // write one chunk to one receiver
        int chunk_id = trans.task_id;
        // check if the chunk is local
        int send_ring_id = trans.ring_id;
        if (chunks.start_id >= trans.to_stage.task_id_start && 
            chunks.start_id < trans.to_stage.task_id_start + trans.to_stage.num_local_tasks) {
            int recv_ring_id = trans.to_stage.ring_id_start + 
                (chunks.start_id - trans.to_stage.task_id_start);
            // LOG(INFO) << "Recv " << recv_ring_id << " Send " << send_ring_id;
            io_tx(ring, (void *)shtable, recv_ring_id);
            int *response = nullptr;
            io_rx(ring, (void **)&response, send_ring_id);
        } else {
            if (external_storage_type == ExternalType::S3) {
                Aws::String objectName = objectNamePrefix + "_" + 
                    std::to_string(chunk_id) + ".dat";
                IORes pres = PutTableToS3(bucketName, objectName, *shtable);
            } else if (external_storage_type == ExternalType::Redis) {
                std::string objectName = objectNamePrefix + "_" + 
                    std::to_string(chunk_id) + ".dat";
                bool pres = PutTableToRedis(objectName, *shtable);
            }
        }
    } else if (trans.op == TransferOp::AllGather) {  // Write one chunk to all receivers
        std::vector<int> external_chunks;
        external_chunks.clear();
        for (int i = 0; i < trans.to_stage.num_tasks; ++i) {
            if (i < trans.to_stage.task_id_start || 
                i >= trans.to_stage.task_id_start + trans.to_stage.num_local_tasks) {
                external_chunks.push_back(i);
            }
        }
        size_t num_local = trans.to_stage.num_local_tasks;
        size_t num_external = external_chunks.size();
        int send_ring_id = trans.ring_id;
        if (num_local > 0) {
            for (int i = 0; i < num_local; ++i) {
                int recv_ring_id = trans.to_stage.ring_id_start + 
                    trans.to_stage.task_id_start + i;
                io_tx(ring, (void *)shtable, recv_ring_id);
            }
        }
        if (num_external > 0) {
            if (external_storage_type == ExternalType::S3) {
                int chunk_id = trans.task_id;
                Aws::String objectName = objectNamePrefix + "_" + 
                    std::to_string(chunk_id) + ".dat";
                IORes pres = PutTableToS3(bucketName, objectName, *shtable);
            } else if (external_storage_type == ExternalType::Redis) {
                int chunk_id = trans.task_id;
                std::string objectName = objectNamePrefix + "_" + 
                    std::to_string(chunk_id) + ".dat";
                bool pres = PutTableToRedis(objectName, *shtable);
            }
        }
        if (num_local > 0) {
            for (int i = 0; i < num_local; ++i) {
                int *response = nullptr;
                io_rx(ring, (void **)&response, send_ring_id);
            }
        }
    } else if (trans.op == TransferOp::Broadcast) {
        std::vector<ShTable *> partitions;
        partitions.clear();
        for (int i = 0; i < trans.to_stage.num_tasks; ++i) {
            ShTable *tb = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
            tb->initialize(schema_type);
            shtable->partition(i, trans.to_stage.num_tasks, *tb);
            partitions.push_back(tb);
        }
        std::vector<int> external_chunks;
        std::vector<int> local_chunks;
        external_chunks.clear();
        local_chunks.clear();
        for (int i = 0; i < trans.to_stage.num_tasks; ++i) {
            if (i < trans.to_stage.task_id_start || 
                i >= trans.to_stage.task_id_start + trans.to_stage.num_local_tasks) {
                external_chunks.push_back(i);
            } else {
                local_chunks.push_back(i);
            }
        }
        size_t num_local = local_chunks.size();
        size_t num_external = external_chunks.size();
        // LOG(INFO) << "Num local " << num_local << " Num external " << num_external;
        int send_ring_id = trans.ring_id;
        if (num_local > 0) {
            for (int i = 0; i < num_local; ++i) {
                int recv_ring_id = trans.to_stage.ring_id_start + 
                    trans.to_stage.task_id_start + i;
                io_tx(ring, (void *)partitions[local_chunks[i]], recv_ring_id);
                // LOG(INFO) << "Recv " << recv_ring_id << " Send " << send_ring_id;
            }
        }
        if (num_external > 0) {
            if (external_storage_type == ExternalType::S3) {
                for (int i = 0; i < num_external; ++i) {
                    int chunk_id = external_chunks[i];
                    Aws::String objectName = objectNamePrefix + "_" + 
                        std::to_string(chunk_id) + ".dat";
                    IORes pres = PutTableToS3(bucketName, objectName, *partitions[chunk_id]);
                }
            } else if (external_storage_type == ExternalType::Redis) {
                for (int i = 0; i < num_external; ++i) {
                    int chunk_id = external_chunks[i];
                    std::string objectName = objectNamePrefix + "_" + 
                        std::to_string(chunk_id) + ".dat";
                    bool pres = PutTableToRedis(objectName, *partitions[chunk_id]);
                }
            }
        }
        if (num_local > 0) {
            for (int i = 0; i < num_local; ++i) {
                int *response = nullptr;
                io_rx(ring, (void **)&response, send_ring_id);
                LOG(INFO) << "Send " << send_ring_id;
            }
        }
        for (int i = 0; i < trans.to_stage.num_tasks; ++i) {
            partitions[i]->deleteTable();
        }
    } else {
        LOG(INFO) << "Shuffle is to be implemented";
        return false;
    }

    return true;
}

bool GetProfileObject(const Aws::String &bucketName, const Aws::String &objectName, 
                const Aws::String &fileName, int num_samples=4) {
    Aws::S3::Model::GetObjectRequest request;
    Aws::S3::S3Client client;

    request.SetBucket(bucketName);
    request.SetKey(objectName);

    Aws::S3::Model::GetObjectOutcome outcome =
            client.GetObject(request);
        
    // get the content of the object
    if (outcome.IsSuccess()) {
        // append the object to the file 
        Aws::OFStream local_file;
        local_file.open(fileName.c_str(), std::ios::out | std::ios::app);
        local_file << outcome.GetResult().GetBody().rdbuf();
        local_file.close();
        return true;
    } else {
        std::cerr << "GetObject error: " << outcome.GetError().GetExceptionName()
            << " " << outcome.GetError().GetMessage() << std::endl;
        // Write an empty profile
        Aws::OFStream local_file;
        local_file.open(fileName.c_str(), std::ios::out | std::ios::app);
        for (int i = 0; i < num_samples; ++i) {
            local_file << "PreTime.tpre 1" << std::endl;
            local_file << "PreTime.tread 1" << std::endl;
            local_file << "PreTime.tcomp 1" << std::endl;
            local_file << "EffectTime.tread1 1" << std::endl;
            local_file << "EffectTime.tread2 1" << std::endl;
            local_file << "EffectTime.tcomp 1" << std::endl;
            local_file << "EffectTime.twrite 1" << std::endl;
            local_file << "EffectTime.tpost 1" << std::endl;
        }
        local_file.close();
        return false;
    }

    return outcome.IsSuccess();
}

/* This function will cause bugs */
bool GetProfile(const Aws::String &bucketName, const Aws::String &objectName, 
                NimbleProfile &profile) {
    Aws::S3::Model::GetObjectRequest request;
    Aws::S3::S3Client client;

    request.SetBucket(bucketName);
    request.SetKey(objectName);

    Aws::S3::Model::GetObjectOutcome outcome =
            client.GetObject(request);
        
    if (outcome.IsSuccess()) { 
        Aws::IOStream &stream = outcome.GetResult().GetBody();
        std::string line;
        while (std::getline(stream, line)) {
            if (line.empty()) {
                continue;
            }
            std::istringstream iss(line);
            std::string key;
            std::string value;
            if (!(iss >> key >> value)) {
                LOG(INFO) << "Error in parsing profile";
                return false;
            }
            if (key == "PreTime.tpre") {
                profile.pre_time.tpre = std::stol(value);
            } else if (key == "PreTime.tread") {
                profile.pre_time.tread = std::stol(value);
            } else if (key == "PreTime.tcomp") {
                profile.pre_time.tcomp = std::stol(value);
            } else if (key == "EffectTime.tread1") {
                profile.effect_time.tread1 = std::stol(value);
            } else if (key == "EffectTime.tread2") {
                profile.effect_time.tread2 = std::stol(value);
            } else if (key == "EffectTime.tcomp") {
                profile.effect_time.tcomp = std::stol(value);
            } else if (key == "EffectTime.twrite") {
                profile.effect_time.twrite = std::stol(value);
            } else if (key == "EffectTime.tpost") {
                profile.effect_time.tpost = std::stol(value);
            } else {
                LOG(INFO) << "Unknown key " << key;
                return false;
            }
        }
        return true;
    } else {
        // std::cerr << "GetObject error: " << outcome.GetError().GetExceptionName()
        //     << " " << outcome.GetError().GetMessage() << std::endl;
        return false;
    }

    return outcome.IsSuccess();
}

bool PutProfile(const Aws::String &bucketName, const Aws::String &objectName, 
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
        // std::cerr << "Error unable to read file " << fileName << std::endl;
        return false;
    }
    request.SetBody(inputData);
    Aws::S3::Model::PutObjectOutcome outcome =
            s3_client.PutObject(request);

    if (!outcome.IsSuccess()) {
        std::cerr << "Error: PutObject: " <<
                  outcome.GetError().GetMessage() << std::endl;
    }

    return outcome.IsSuccess();
}

#endif /* IO_HPP */