/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin & xingyu xiang)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#include "query.hpp"
#include "net.hpp"
#include "scheduler.hpp"

#define SIMULATE 0

std::string algorithm = "sysname";
std::string proc_step = "algorithm";
// processing step: profile, aggregate, algorithm
std::string sched_addr;
uint16_t sched_ports[MAX_SERVERS];
std::string exec_addrs[MAX_SERVERS];
uint16_t exec_ports[MAX_SERVERS];
int socks[MAX_SERVERS];
std::string setting_path = "../configs/setting.txt";
std::string dag_config_path = "../configs/dsq";


void InitSchedNetConfig() {
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

void InitScheduler() {
    InitSchedNetConfig();
    num_slots.clear();
    for (int i = 0; i < num_executors; ++i) {
        num_slots.push_back(MAX_SLOTS - 1);  // leave one slot for shm
        slots_pre_sum.push_back(0);
    }
}

void DistributeResource(const std::string &distri) {
    if (distri == "uniform-1") {
        return;
    } else if (distri == "uniform-0.5") {
        for (int i = 0; i < num_executors; ++i)
            num_slots[i] = MAX_SLOTS / 2 - 1;
    } else if (distri == "zipf-0.99") {
        std::vector<double> probs;
        double sum = 0;
        for (int i = 0; i < num_executors; ++i) {
            probs.push_back(1.0 / pow(i + 1, 0.99));
            sum += probs[i];
        }
        for (int i = 0; i < num_executors; ++i) {
            probs[i] /= sum;
            num_slots[i] = (int) (2 * MAX_SLOTS * probs[i]);
        }
    } else if (distri == "zipf-0.9") {
        std::vector<double> probs;
        double sum = 0;
        for (int i = 0; i < num_executors; ++i) {
            probs.push_back(1.0 / pow(i + 1, 0.9));
            sum += probs[i];
        }
        for (int i = 0; i < num_executors; ++i) {
            probs[i] /= sum;
            num_slots[i] = (int) (2 * MAX_SLOTS * probs[i]);
        }
    } else {
        std::cerr << "Unknown distribution: " << distri << std::endl;
        exit(1);
    }
    for (int i = 0; i < num_executors; ++i) {
        std::cout << "Executor " << i << " has " << num_slots[i] << " slots\n";
    }
}

void ReadSetting(const std::string &file_name) {
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
        // skip comments
        if (tokens[0][0] == '#') {
            continue;
        }
        for (int i = 0; i < tokens.size(); ++i) {
            trimEnd(tokens[i]);
        }
        std::cout << tokens[0] << std::endl;
        if (tokens[0] == "num_cpus") {
            num_cpus = std::stoi(tokens[1]);
        } else if (tokens[0] == "proc_step") {
            proc_step = tokens[1];
        } else if (tokens[0] == "algorithm") {
            algorithm = tokens[1];
        } else if (tokens[0] == "mode") {
            if (tokens[1] == "JCT") {
                sched_mode = JCT;
            } else if (tokens[1] == "cost") {
                sched_mode = Cost;
            } else {
                std::cerr << "Unknown scheduling mode " << tokens[1].size() << std::endl;
                exit(1);
            }
        } else if (tokens[0] == "resource") {
            resource_distri = tokens[1];
            // DistributeResource(resource_distri);
        } else if (tokens[0] == "distribution") {
            for (int i = 0; i < num_executors; ++i) {
                num_slots[i] = std::stoi(tokens[i + 1]);
            }
        } else if (tokens[0] == "base_deg") {
            base_deg = std::stoi(tokens[1]);
        } else if (tokens[0] == "external") {
            if (tokens[1] == "S3") {
                external_storage_type = ExternalType::S3;
            } else if (tokens[1] == "Redis") {
                external_storage_type = ExternalType::Redis;
            } else {
                std::cerr << "Unknown external storage type";
                exit(1);
            }
        } else {
            continue;
        }
    }
    fin.close();
    if (algorithm == "nimble" || algorithm == "greedy") {
        for (int i = 0; i < num_executors; ++i) {
            num_slots[i] = MAX_SLOTS - 1;  // they are placement-agnostic
        }
    }
    slots_pre_sum[0] = num_slots[0];
    for (int i = 1; i < num_executors; ++i) {
        slots_pre_sum[i] = slots_pre_sum[i - 1] + num_slots[i];
    }
}

void AdjustSetting(StageDAG &sdag) {
    if (sdag.query_id == 16) {
        int total_slots = 0;
        for (int i = 0; i < num_executors; ++i) {
            total_slots += num_slots[i];
        }
        for (int i = 0; i < num_executors; ++i) {
            num_slots[i] = total_slots / num_executors;
        }
        for (int i = 0; i < total_slots % num_executors; ++i) {
            num_slots[i] += 1;
        }
        slots_pre_sum[0] = num_slots[0];
        for (int i = 1; i < num_executors; ++i) {
            slots_pre_sum[i] = slots_pre_sum[i - 1] + num_slots[i];
        }
    }
}

int ConnectToExecutor(std::string sched_addr, uint16_t sched_port, 
    std::string exec_addr, uint16_t exec_port) {
    
    int sockfd;
    struct sockaddr_in schedaddr, execaddr;

    bzero(&execaddr, sizeof(execaddr));
    execaddr.sin_family = AF_INET;
    execaddr.sin_port = htons(exec_port);
    inet_pton(AF_INET, exec_addr.c_str(), &execaddr.sin_addr);

    bzero(&schedaddr, sizeof(schedaddr));
    schedaddr.sin_family = AF_INET;
    schedaddr.sin_port = htons(sched_port);
    inet_pton(AF_INET, sched_addr.c_str(), &schedaddr.sin_addr);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (bind(sockfd, (sockaddr *)&schedaddr, sizeof(schedaddr)) == -1) {
        LOG(ERROR) << "Scheduler bind failed";
        close(sockfd);
        return -1;
    }

    if (connect(sockfd, (sockaddr *)&execaddr, sizeof (execaddr)) == -1) {
        LOG(ERROR) << "Scheduler connect failed";
        close(sockfd);
        return -1;
    }
    
    return sockfd;
}

void GenerateProfiles(StageDAG &sdag) {
    // Execute many times to generate profiles and aggregate the results
    for (int j = 0; j < NUM_SAMPLES; j++) {
        if (degs[j] % PROFILE_SERVERS != 0 && degs[j] != 1) {
            LOG(ERROR) << "Degree must be divisible by " << PROFILE_SERVERS;
            exit(1);
        }
    }
    for (int j = 0; j < NUM_SAMPLES; j++) {
        for (int i = 1; i <= sdag.num_stages; i++) {  // For each stage
            StageNode &st = sdag.stages[i];
            if (st.is_single == 0 && degs[j] != 1) {
                int parall_deg = st.time_model.parall_degrees[j] / PROFILE_SERVERS;
                for (int k = 0; k < PROFILE_SERVERS; k++) {
                    Stage s_curr(i, parall_deg * PROFILE_SERVERS, k * parall_deg, 1, parall_deg);
                    Stage s_from[MAX_IN_DEGREE], s_to;
                    if (st.is_leaf) {
                        s_from[0].num_tasks = st.input_chunks;
                    } else {
                        for (int p = 0; p < st.num_from; p++) {
                            s_from[p].stage_id = st.from_id[p];
                            s_from[p].num_tasks = sdag.stages[st.from_id[p]].time_model.parall_degrees[j];
                        }
                    }
                    s_to.stage_id = st.to_id;
                    if (st.to_id != -1) {
                        s_to.num_tasks = sdag.stages[st.to_id].time_model.parall_degrees[j];
                    } else {
                        s_to.num_tasks = 1;
                    }
                    TasksToExecute tasks(s_curr, s_to);
                    if (st.is_leaf) {
                        tasks.from_stage[0] = s_from[0];
                    } else {
                        for (int p = 0; p < st.num_from; p++) {
                            tasks.from_stage[p] = s_from[p];
                        }
                    }
                    SendPacket(SchedPacket(EXEC, sdag.query_id, tasks), socks[k]);
                    SendPacket(SchedPacket(END_CONT, TasksToExecute()), socks[k]);
                }
                for (int k = 0; k < PROFILE_SERVERS; k++) {
                    SchedPacket packet;
                    RecvPacket(packet, socks[k]);
                    if (packet.tag == PROFILED)
                        LOG(INFO) << "Machine " << k << " Stage " << i << " Profile prepared";
                }
            } else {
                // Single stage
                Stage s_curr(i, 1, 0, 1, 1);
                Stage s_from(st.from_id[0], 1, 0, 1, 0);
                s_from.num_tasks = sdag.stages[st.from_id[0]].time_model.parall_degrees[j];
                Stage s_to(st.to_id, 1, 0, 1, 0);
                if (st.to_id != -1) {
                    s_to.num_tasks = sdag.stages[st.to_id].time_model.parall_degrees[j];
                } else {
                    s_to.num_tasks = 1;
                }
                TasksToExecute tasks(s_curr, s_to, s_from);
                
                SendPacket(SchedPacket(EXEC, sdag.query_id, tasks), socks[0]);
                SendPacket(SchedPacket(END_CONT, TasksToExecute()), socks[0]);
                SchedPacket packet;
                RecvPacket(packet, socks[0]);
                if (packet.tag == PROFILED)
                    LOG(INFO) << "Machine 0" << " Stage " << i << " Profile prepared";
            }
        }
    }
}

/* Get time profiles, aggregate them and print to files */
void AggregateProfiles(StageDAG &sdag) {
    std::string external_type_;
    if (external_storage_type == ExternalType::S3) {
        external_type_ = "s3/";
    } else if (external_storage_type == ExternalType::Redis) {
        external_type_ = "redis/";
    } else {
        LOG(ERROR) << "Unknown external storage type";
        exit(1);
    }
    for (int i = 1; i <= sdag.num_stages; i++) {  // For each stage
        StageNode &st = sdag.stages[i];
        if (st.is_single == 0) {
            for (int j = 0; j < NUM_SAMPLES; j++) {
                int parall_deg = st.time_model.parall_degrees[j];
                int used_parall_deg = parall_deg;
                for (int k = 0; k < parall_deg; k++) {
                    std::string profile_name = "q" + std::to_string(sdag.query_id) + "/" +
                        "q" + std::to_string(sdag.query_id) + 
                        "_stage" + std::to_string(i) + 
                        "_parall" + std::to_string(parall_deg) + 
                        "_task" + std::to_string(k) + ".log";
                    NimbleProfile profile;
                    auto r = GetProfile(logBucketName, profile_name, profile);
                    if (!r) {
                        used_parall_deg--;
                    }
                    st.time_model.profiles[j].add(profile);
                }
                if (used_parall_deg == 0) {
                    LOG(ERROR) << "Stage " << i << " " << j << " parall_deg is 0";
                    exit(1);
                }
                st.time_model.profiles[j].div(used_parall_deg);
                std::string local_log_name = profilePath + external_type_ +
                    "q" + std::to_string(sdag.query_id) + 
                    "_stage" + std::to_string(i) + ".profile";
                st.time_model.profiles[j].print_to_file(local_log_name);
            }
        } else {
            std::string profile_name = "q" + std::to_string(sdag.query_id) + "/" + 
                "q" + std::to_string(sdag.query_id) + 
                "_stage" + std::to_string(i) + "_parall1_task0.log";
            std::string local_log_name = profilePath + external_type_ + "q" +
                std::to_string(sdag.query_id) + 
                "_stage" + std::to_string(i) + ".profile";
            auto r = GetProfileObject(logBucketName, profile_name, local_log_name, NUM_SAMPLES);
        }
    }
}

/* Read from profiles, use for model fitting */
void ReadProfiles(StageDAG &sdag) {
    std::string external_type_;
    if (external_storage_type == ExternalType::S3) {
        external_type_ = "s3/";
    } else if (external_storage_type == ExternalType::Redis) {
        external_type_ = "redis/";
    } else {
        LOG(ERROR) << "Unknown external storage type";
        exit(1);
    }
    for (int i = 1; i <= sdag.num_stages; i++) {  // For each stage
        StageNode &st = sdag.stages[i];
        std::string local_log_name = profilePath + external_type_ + 
            "q" + std::to_string(sdag.query_id) + 
            "_stage" + std::to_string(i) + ".profile";
        read_from_profiles(local_log_name, st.time_model.profiles, NUM_SAMPLES);
    }
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

    LOG(INFO) << "Scheduler started";

    Aws::SDKOptions options;
    Aws::InitAPI(options);

    int qid = std::stoi(argv[1]);

    InitScheduler();
    ReadSetting(setting_path);

    if (proc_step == "profile")
        num_executors = PROFILE_SERVERS; 

    if (!SIMULATE && proc_step != "aggregate" && algorithm != "optimal") {
        for (int i = 0; i < num_executors; ++i) {
            socks[i] = ConnectToExecutor(sched_addr, sched_ports[i], 
                exec_addrs[i], exec_ports[i]);
            LOG(INFO) << "Connected to executor " << i;
        }
    }

    StageDAG sdag;
    ReadDAGConfig(sdag, dag_config_path + std::to_string(qid) + ".txt");
    AdjustSetting(sdag);

    if (proc_step == "profile") {
        GenerateProfiles(sdag);
        for (int j = 0; j < num_executors; ++j) {
            SendPacket(SchedPacket(END, TasksToExecute()), socks[j]);
        }
        for (int i = 0; i < num_executors; ++i) {
            SchedPacket packet;
            RecvPacket(packet, socks[i]);
            if (packet.tag == ACK)
                LOG(INFO) << i << " ACK received";
        }
    } else if (proc_step == "aggregate") {
        AggregateProfiles(sdag);
        auto t1 = get_time();
        ReadProfiles(sdag);
        sdag.fitTimeModel();
        auto t2 = get_time();
        auto du = get_us_duration(t1, t2);
        // Print the accuracy of model for the IO- and comput- stages to "effect_perf.log"
        if (external_storage_type == ExternalType::S3) {
            int comp_stage_id = 6;  // for q95 and q16
            int io_stage_id = 1;  // for q95 and q16
            if (qid == 94) {
                io_stage_id = 4;
            } else if (qid == 1) {
                io_stage_id = 2;
                comp_stage_id = 4;
            }
            double comp_a = 0, comp_b = 0, io_a = 0, io_b = 0;
            double comp_actuals[NUM_SAMPLES];
            double io_actuals[NUM_SAMPLES];
            sdag.printStageTimeModel(comp_stage_id, comp_a, comp_b, comp_actuals);
            sdag.printStageTimeModel(io_stage_id, io_a, io_b, io_actuals);
            // Write to '../results/effect_perf.log'
            std::ofstream fout("../results/effect_perf.log", std::ios::app);
            fout << "Query " << qid << std::endl;
            fout << "Time " << du << std::endl;
            fout << "IO Stage " << io_stage_id << std::endl;
            fout << "IO a: " << io_a << " " << io_b << std::endl;
            fout << "IO actuals: ";
            for (int i = 0; i < NUM_SAMPLES; ++i) {
                fout << io_actuals[i] << " ";
            }
            fout << std::endl;
            fout << "Comp Stage " << comp_stage_id << std::endl;
            fout << "Comp a: " << comp_a << " " << comp_b << std::endl;
            fout << "Comp actuals: ";
            for (int i = 0; i < NUM_SAMPLES; ++i) {
                fout << comp_actuals[i] << " ";
            }
            fout << std::endl;
            if (qid == 95) {
                double a[5*9];
                double b[5*9];
                sdag.getTimeModel(a, b);
                std::ofstream fout2("../results/time_breakdown.log", std::ios::out);
                fout2 << "a: ";
                for (int i = 0; i < 5*9; ++i) {
                    fout2 << a[i] << " ";
                }
                fout2 << std::endl;
                fout2 << "b: ";
                for (int i = 0; i < 5*9; ++i) {
                    fout2 << b[i] << " ";
                }
                fout2 << std::endl;
            }
        }
    } else if (proc_step == "algorithm") {
        auto t1 = get_time();
        ReadProfiles(sdag);
        sdag.fitTimeModel();
        // sdag.printTimeModel();
        auto t2 = get_time();
        LOG(INFO) << "Time to fit time model: " << t2 - t1 << " ms";

        auto start = get_time();
        if (algorithm == "elastic") {
            ElasticParallel(sdag);
            PredictCost(sdag);
        } else if (algorithm == "elastic_nimble") {
            ElasticParallel(sdag, 1);
            PredictCost(sdag);
        } else if (algorithm == "nimble") {
            int d = base_deg;
            RandomUniformPlacement(sdag, d);
            NaiveNimble(sdag, d);
        } else if (algorithm == "greedy") {
            int d = base_deg;
            GreedyPlacement(sdag, d);
            GreedyNimble(sdag, d);
        } else if (algorithm == "optimal") {
            SimulateLowerBound(sdag);
        } else if (algorithm == "new_nimble") {
            NewNimble(sdag);
        } else if (algorithm == "new_greedy") {
            NewGreedy(sdag);
        } else if (algorithm == "new_elas") {
            double f = 2.0;
            ElasticParallelNew(sdag, f);
            PredictCost(sdag, f);
        } else {
            LOG(FATAL) << "Unknown algorithm " << algorithm;
        }
        auto end = get_time();
        auto du = get_us_duration(start, end);
        LOG(INFO) << "Sched Time: " << du << " us";

        start = get_time();

        if (!SIMULATE && algorithm != "optimal") {
            int k = 1;
            while (k <= sdag.num_stages) {
                auto curr = get_time();
                if (get_us_duration(start, curr) > start_times[k].start_time) {
                    for (int i = 0; i < num_executors; ++i)
                        SendPacket(SchedPacket(EXEC, sdag.query_id, 
                            exec_plan[i][start_times[k].stage_id].tasks), socks[i]);
                    k++;
                }
            }
            
            for (int j = 0; j < num_executors; ++j) {
                SendPacket(SchedPacket(END, TasksToExecute()), socks[j]);
            }
            for (int i = 0; i < num_executors; ++i) {
                SchedPacket packet;
                RecvPacket(packet, socks[i]);
                if (packet.tag == ACK)
                    LOG(INFO) << i << " ACK received";
            }
            end = get_time();
            auto jct = get_duration(start, end);
            LOG(INFO) << "JCT: " << jct << " ms";
            double total_cost = 0;
            for (int i = 0; i < num_executors; ++i) {
                SchedPacket packet;
                RecvPacket(packet, socks[i]);
                if (packet.tag == COST) {
                    total_cost += packet.cost;
                    LOG(INFO) << i << " COST " << packet.cost << " received";
                }
            }
            LOG(INFO) << "Execution Cost: " << total_cost;

            std::string log_name = "../results/q" + std::to_string(qid) + "_";
            if (external_storage_type == ExternalType::S3) {
                log_name += "s3_";
            } else if (external_storage_type == ExternalType::Redis) {
                log_name += "redis_";
            }
            if (algorithm == "elastic" || algorithm == "elastic_nimble" ) {
                if (sched_mode == JCT) {
                    log_name += algorithm + "_jct.log";
                } else if (sched_mode == Cost) {
                    log_name += algorithm + "_cost.log";
                }
            } else {
                log_name += algorithm + ".log";
            }

            std::ofstream fout(log_name, std::ios::app);

            fout << "Resource: " << resource_distri << std::endl;
            fout << "Sched_time: " << du << std::endl;
            fout << "JCT: " << jct << std::endl;
            fout << "Cost: " << total_cost << std::endl;
        }
    } else {
        LOG(FATAL) << "Unknown proc_step " << proc_step;
    }

    if (proc_step != "aggregate") {
        for (int i = 0; i < num_executors; ++i) {
            close(socks[i]);
        }
    }
    Aws::ShutdownAPI(options);
    google::ShutdownGoogleLogging();
    return 0;
}
