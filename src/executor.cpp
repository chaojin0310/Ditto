/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin & xingyu xiang)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#include "query.hpp"
#include "net.hpp"
#include "scheduler.hpp"


std::string sched_addr;
uint16_t sched_ports[MAX_SERVERS];
std::string exec_addrs[MAX_SERVERS];
uint16_t exec_ports[MAX_SERVERS];
int sock;

int query_id; 
std::vector<pid_t> pids;
std::vector<TasksToExecute> tasks_to_exec;

std::string setting_path = "../configs/setting.txt";

void InitExecNetConfig() {
    int num_executors = 1;
    std::ifstream net_conf("../configs/netconfig.txt");
    if (!net_conf.is_open()) {
        std::cerr << "Cannot open netconfig file";
        exit(1);
    }
    std::string line;
    int n = 0;
    while (std::getline(net_conf, line)) {
        std::stringstream ss(line);
        std::string token;
        std::vector<std::string> tokens;
        tokens.clear();
        while (std::getline(ss, token, ' ')) {
            tokens.push_back(token);
        }
        if (tokens.size() == 0) {
            continue;
        }
        // skip comments
        if (tokens[0][0] == '#') {
            continue;
        }
        if (tokens[0] == "num_exec") {
            num_executors = std::stoi(tokens[1]);
        } else if (tokens[0] == "sched_ip") {
            sched_addr = tokens[1];
        } else if (tokens[0] == "sched_ports") {
            for (int i = 0; i < num_executors; ++i) {
                sched_ports[i] = std::stoi(tokens[i + 1]);
            }
        } else if (tokens[0] == "exec_addr") {
            exec_addrs[n] = tokens[1];
            exec_ports[n] = std::stoi(tokens[2]);
            n++;
        } else {
            std::cerr << "Unknown net config: " << tokens[0] << std::endl;
            exit(1);
        }
    }
    net_conf.close();
}

int ConnectToScheduler(std::string exec_addr, uint16_t exec_port) {
    int listenfd, connfd;
    socklen_t schedlen;
    sockaddr_in schedaddr, execaddr;

    listenfd = socket(AF_INET, SOCK_STREAM, 0);

    bzero(&execaddr, sizeof(execaddr));
    execaddr.sin_family = AF_INET;
    execaddr.sin_port = htons(exec_port);
    inet_pton(AF_INET, exec_addr.c_str(), &execaddr.sin_addr);

    if (bind(listenfd, (sockaddr *)&execaddr, sizeof(execaddr)) == -1) {
        LOG(ERROR) << "Executor bind failed";
        close(listenfd);
        return -1;
    }

    listen(listenfd, 1024);

    schedlen = sizeof(schedaddr);
    connfd = accept(listenfd, (sockaddr *)&schedaddr, &schedlen);
    return connfd;
}

void ReadExternalType(const std::string &file_name) {
    std::ifstream fin(file_name);
    if (!fin.is_open()) {
        std::cerr << "Cannot open setting file";
        exit(1);
    }
    std::string line;
    while (std::getline(fin, line)) {
        std::stringstream ss(line);
        std::string token;
        std::vector<std::string> tokens;
        tokens.clear();
        while (std::getline(ss, token, ' ')) {
            tokens.push_back(token);
        }
        if (tokens.size() == 0) {
            continue;
        }
        for (int i = 0; i < tokens.size(); ++i) {
            trimEnd(tokens[i]);
        }
        if (tokens[0] == "external") {
            if (tokens[1] == "S3") {
                external_storage_type = ExternalType::S3;
            } else if (tokens[1] == "Redis") {
                external_storage_type = ExternalType::Redis;
            } else {
                std::cerr << "Unknown external storage type";
                exit(1);
            }
        }
    }
    fin.close();
}

int main(int argc, char *argv[]) {
    if (setuid(0) != 0) {
        std::cout << "setuid failed" << std::endl;
        return 1;
    }

    FLAGS_alsologtostderr = true;
    google::InitGoogleLogging(argv[0]);
    FLAGS_colorlogtostderr = true;
    FLAGS_log_dir = "../logs";

    int _;

    InitExecNetConfig();
    ReadExternalType(setting_path);
    
    int exec_id = std::stoi(argv[1]);
    sock = ConnectToScheduler(exec_addrs[exec_id], exec_ports[exec_id]);

    SchedPacket packet;
    bool need_profile = false;
    while (RecvPacket(packet, sock) == 0) {
        if (packet.tag == EXEC) {
            query_id = packet.query_id;
            if (packet.tasks.curr_stage.num_local_tasks > 0)
                tasks_to_exec.push_back(packet.tasks);
            if (query_id == 95) {
                execute_q95(packet.tasks, pids);
            } else if (query_id == 94) {
                execute_q94(packet.tasks, pids);
            } else if (query_id == 16) {
                execute_q16(packet.tasks, pids);
            } else if (query_id == 1) {
                execute_q1(packet.tasks, pids);
            } else {
                LOG(ERROR) << "Unknown query id";
            }
        } else if (packet.tag == END_CONT) {
            need_profile = true;
            for (pid_t &pid : pids)
                waitpid(pid, NULL, 0);
            pids.clear();
            Aws::SDKOptions options;
            Aws::InitAPI(options);
            for (auto &tasks: tasks_to_exec) {
                std::string part_name = "q" + std::to_string(query_id) + 
                    "_stage" + std::to_string(tasks.curr_stage.stage_id) + 
                    "_parall" + std::to_string(tasks.curr_stage.num_tasks);
                for (int i = 0; i < tasks.curr_stage.num_local_tasks; ++i) {
                    const Aws::String base_name = part_name + "_task" + 
                        std::to_string(tasks.curr_stage.task_id_start + i) + ".log";
                    const Aws::String obj_name = "q" + std::to_string(query_id) + "/" +
                        base_name;
                    const Aws::String file_name = logPath + base_name;
                    PutProfile(logBucketName, obj_name, file_name);
                }
            }
            Aws::ShutdownAPI(options);
            SendPacket(SchedPacket(PROFILED, TasksToExecute()), sock);
            LOG(INFO) << "Profiles uploaded";
        } else if (packet.tag == TESTPKT) {
            packet.tasks.print();
        } else if (packet.tag == END) {
            break;
        }
    }

    for (pid_t &pid : pids)
        waitpid(pid, NULL, 0);
    SendPacket(SchedPacket(ACK, TasksToExecute()), sock);
    LOG(INFO) << "Tasks completed";
    if (!need_profile) {
        StageDAG sdag;
        ReadDAGConfig(sdag, "../configs/dsq" + std::to_string(query_id) + ".txt");
        double total_cost = 0;
        for (auto &tasks: tasks_to_exec) {
            std::string part_name = "q" + std::to_string(query_id) + 
                "_stage" + std::to_string(tasks.curr_stage.stage_id) + 
                "_parall" + std::to_string(tasks.curr_stage.num_tasks);
            StageNode &st = sdag.stages[tasks.curr_stage.stage_id];
            double stage_cost = 0;
            int write_shm = tasks.to_stage.num_local_tasks > 0 ? 1 : 0;
            int read1_shm = tasks.from_stage[0].num_local_tasks > 0 ? 1 : 0;
            int read2_shm = tasks.from_stage[1].num_local_tasks > 0 ? 1 : 0;
            double mem_size = st.mem_model.fixed + st.mem_model.parall / tasks.curr_stage.num_tasks;
            for (int i = 0; i < tasks.curr_stage.num_local_tasks; ++i) {
                const std::string obj_name = part_name + "_task" + 
                    std::to_string(tasks.curr_stage.task_id_start + i) + ".log";
                const std::string file_name = logPath + obj_name;
                NimbleProfile profile;
                read_from_profiles(file_name, &profile, 1);
                // auto exec_time = profile.get_total_time();
                auto exec_time = profile.get_charge_time(read1_shm, read2_shm, write_shm, query_id);
                stage_cost += exec_time * mem_size / 1000; // ms to s
            }
            total_cost += stage_cost;
            LOG(INFO) << "Stage " << tasks.curr_stage.stage_id << " cost: " << stage_cost;
        }
        SendPacket(SchedPacket(COST, total_cost), sock);
        LOG(INFO) << "Cost sent";
    }

    close(sock);

    google::ShutdownGoogleLogging();
    return 0;
}
