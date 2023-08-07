/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin & xingyu xiang)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */
#include "task.hpp"

/*====================Scheduling Packet========================*/
enum PktType {
    EXEC = 0,  // execute tasks
    END = 1,  // job end 
    END_CONT = 2,  // tasks end & continue to execute
    ACK = 3,  // job/tasks ack
    PROFILED = 4,  // job profiled
    COST = 5,  // cost
    TESTPKT = 6   // test packet
};

struct SchedPacket {
    int tag;
    int query_id;
    TasksToExecute tasks;
    double cost;
    SchedPacket() : tag(0), query_id(0), tasks(), cost(0) {}
    SchedPacket(int _tag, TasksToExecute _tasks) :
        tag(_tag), query_id(0), tasks(_tasks), cost(0) {}
    SchedPacket(int _tag, int _query_id, TasksToExecute _tasks) :
        tag(_tag), query_id(_query_id), tasks(_tasks), cost(0) {}
    SchedPacket(int _tag, double _cost) :
        tag(_tag), query_id(0), tasks(), cost(_cost) {}
};

int SendPacket(SchedPacket packet, int sockfd) {
    if (send_robust(sockfd, &packet, sizeof(packet), 0) == -1) {
        LOG(ERROR) << "Scheduler send failed";
        return -1;
    }
    return 0;
}

int RecvPacket(SchedPacket &packet, int sockfd) {
    return recv_robust(sockfd, &packet, sizeof(packet), 0);
}

/*====================Macros and Definitions for Scheduling========================*/
#define NUM_SAMPLES 4  // the number of samples for profiling
#define MAX_SERVERS 10  // the max number of servers

#define MAX_STAGES_PER_GROUP 3  // the max number of stages in a group
#define SHM_RATIO 50  // the ratio of shared memory reading time to total memory
#define ERROR_RATIO 1.05  // the ratio between practical execution time and theoretical time

// We get profiles using 5 servers, so notice that the degrees should be multiples of 5
// We profile q16 using 6 servers, since its memory footprint is too high
// #define PROFILE_SERVERS 5
#define PROFILE_SERVERS 2

int base_deg = 45;  // the base parallel degree, which should be read from config file
int degs[NUM_SAMPLES] = {2, 4, 6, 12};  // for profiling

/*===================Models and Basic Stage Definition=====================*/

enum SCHED_MODE {
    JCT = 0,
    Cost = 1
};  // the mode of scheduling

enum PRED_MODE {
    ReadComp = 0,  // read1 + read2 + comp
    CompWrite = 1, // comp + write
    R1CompWrite = 2,  // read1 + comp + write
    R2CompWrite = 3,  // read2 + comp + write
    RCW = 4,  // read + comp + write
    OnlyWrite = 5,  // write
    Pre = 6,  // pre
    OnlyRead = 7,  // read1 + read2
    OnlyComp = 8, // comp
    R1Comp = 9,  // read1 + comp
    R2Comp = 10,  // read2 + comp
    OnlyR1 = 11, // read1
    OptComp = 12,  // comp + r/w in shm
};  // the mode of prediction

// Execution time model of a stage
struct TimeModel {
    double pa, pb;
    double r1a, r1b;
    double r2a, r2b;
    double ca, cb;
    double wa, wb;
    int parall_degrees[NUM_SAMPLES];
    NimbleProfile profiles[NUM_SAMPLES];
    TimeModel() : pa(0), pb(0), r1a(0), r1b(0), r2a(0), r2b(0), ca(0), cb(0), wa(0), wb(0) {
        for (int i = 0; i < NUM_SAMPLES; i++) {
            parall_degrees[i] = degs[i];
        }
    }
    void fit() {  // the profiles has been set
        // Use lesat square method to fit the model: y = a/x + b
        // fit pre_time
        double x_, sum_x = 0, sum_y = 0, sum_xy = 0, sum_xx = 0;
        for (int i = 0; i < NUM_SAMPLES; i++) {
            x_ = 1.0 / parall_degrees[i];
            sum_x += x_;
            auto y_ = profiles[i].pre_time.tpre + profiles[i].pre_time.tread + 
                profiles[i].pre_time.tcomp;
            sum_y += y_;
            sum_xy += x_ * y_;
            sum_xx += x_ * x_;
        }
        pa = (NUM_SAMPLES * sum_xy - sum_x * sum_y) / (NUM_SAMPLES * sum_xx - sum_x * sum_x);
        pb = (sum_y - pa * sum_x) / NUM_SAMPLES;
        // fit read_time1
        sum_x = 0, sum_y = 0, sum_xy = 0, sum_xx = 0;
        for (int i = 0; i < NUM_SAMPLES; i++) {
            x_ = 1.0 / parall_degrees[i];
            sum_x += x_;
            auto y_ = profiles[i].effect_time.tread1;
            sum_y += y_;
            sum_xy += x_ * y_;
            sum_xx += x_ * x_;
        }
        r1a = (NUM_SAMPLES * sum_xy - sum_x * sum_y) / (NUM_SAMPLES * sum_xx - sum_x * sum_x);
        r1b = (sum_y - r1a * sum_x) / NUM_SAMPLES;
        // fit read_time2
        if (profiles[0].effect_time.tread2 > 0) {
            sum_x = 0, sum_y = 0, sum_xy = 0, sum_xx = 0;
            for (int i = 0; i < NUM_SAMPLES; i++) {
                x_ = 1.0 / parall_degrees[i];
                sum_x += x_;
                auto y_ = profiles[i].effect_time.tread2;
                sum_y += y_;
                sum_xy += x_ * y_;
                sum_xx += x_ * x_;
            }
            r2a = (NUM_SAMPLES * sum_xy - sum_x * sum_y) / (NUM_SAMPLES * sum_xx - sum_x * sum_x);
            r2b = (sum_y - r2a * sum_x) / NUM_SAMPLES;
        }
        // fit comp_time
        sum_x = 0, sum_y = 0, sum_xy = 0, sum_xx = 0;
        for (int i = 0; i < NUM_SAMPLES; i++) {
            x_ = 1.0 / parall_degrees[i];
            sum_x += x_;
            auto y_ = profiles[i].effect_time.tcomp;
            sum_y += y_;
            sum_xy += x_ * y_;
            sum_xx += x_ * x_;
        }
        ca = (NUM_SAMPLES * sum_xy - sum_x * sum_y) / (NUM_SAMPLES * sum_xx - sum_x * sum_x);
        cb = (sum_y - ca * sum_x) / NUM_SAMPLES;
        // fit write_time
        sum_x = 0, sum_y = 0, sum_xy = 0, sum_xx = 0;
        for (int i = 0; i < NUM_SAMPLES; i++) {
            x_ = 1.0 / parall_degrees[i];
            sum_x += x_;
            auto y_ = profiles[i].effect_time.twrite;
            sum_y += y_;
            sum_xy += x_ * y_;
            sum_xx += x_ * x_;
        }
        wa = (NUM_SAMPLES * sum_xy - sum_x * sum_y) / (NUM_SAMPLES * sum_xx - sum_x * sum_x);
        wb = (sum_y - wa * sum_x) / NUM_SAMPLES;
    }
    double predict_total_time(int parall_degree) {
        double pre_time = pa / parall_degree + pb;
        double read_time1 = r1a / parall_degree + r1b;
        double read_time2 = r2a / parall_degree + r2b;
        double comp_time = ca / parall_degree + cb;
        double write_time = wa / parall_degree + wb;
        return pre_time + read_time1 + read_time2 + comp_time + write_time;
    }
    double predict_partial_factor(int pred_mode) {
        if (pred_mode == ReadComp) {
            return r1a + r2a + ca;
        } else if (pred_mode == CompWrite) {
            return r1a / SHM_RATIO + r2a / SHM_RATIO + ca + wa;
        } else if (pred_mode == R1CompWrite) {
            return r1a + r2a / SHM_RATIO + ca + wa;
        } else if (pred_mode == R2CompWrite) {
            return r1a / SHM_RATIO + r2a + ca + wa;
        } else if (pred_mode == RCW) {
            return r1a + r2a + ca + wa;
        } else if (pred_mode == R1Comp) {
            return r1a + r2a / SHM_RATIO + ca;
        } else if (pred_mode == R2Comp) {
            return r1a / SHM_RATIO + r2a + ca;
        } else if (pred_mode == OptComp) {
            return (r1a + r2a + wa) / SHM_RATIO + ca;
        } else if (pred_mode == Pre) {
            return pa;
        } else {
            std::cerr << "Error: invalid pred_mode" << std::endl;
            exit(1);
        }
    }
    double predict_time(int pred_mode, int parall_degree) {
        double pre_time = pa / parall_degree + pb;
        double read_time1 = r1a / parall_degree + r1b;
        double read_time2 = r2a / parall_degree + r2b;
        double comp_time = ca / parall_degree + cb;
        double write_time = wa / parall_degree + wb;
        if (pred_mode == ReadComp) {
            return read_time1 + read_time2 + comp_time;
        } else if (pred_mode == CompWrite) {
            return (read_time1 + read_time2) / SHM_RATIO + comp_time + write_time;
        } else if (pred_mode == R1CompWrite) {
            return read_time1 + read_time2 / SHM_RATIO + comp_time + write_time;
        } else if (pred_mode == R2CompWrite) {
            return read_time1 / SHM_RATIO + read_time2 + comp_time + write_time;
        } else if (pred_mode == RCW) {
            return read_time1 + read_time2 + comp_time + write_time;
        } else if (pred_mode == OnlyWrite) {
            return write_time;
        } else if (pred_mode == Pre) {
            return pre_time;
        } else if (pred_mode == OnlyRead) {
            return read_time1 + read_time2;
        } else if (pred_mode == OnlyComp) {
            return comp_time;
        } else if (pred_mode == R1Comp) {
            return read_time1 + read_time2 / SHM_RATIO + comp_time;
        } else if (pred_mode == R2Comp) {
            return read_time1 / SHM_RATIO + read_time2 + comp_time;
        } else if (pred_mode == OnlyR1) {
            return read_time1;
        } else if (pred_mode == OptComp) {
            return comp_time + (read_time1 + read_time2 + write_time) / SHM_RATIO;
        } else {
            std::cerr << "Error: invalid pred_mode" << std::endl;
            exit(1);
        }
    }
    void print() {
        LOG(INFO) << "";
        LOG(INFO) << "pre_time: " << pa << " / x + " << pb;
        LOG(INFO) << "read_time1: " << r1a << " / x + " << r1b;
        LOG(INFO) << "read_time2: " << r2a << " / x + " << r2b;
        LOG(INFO) << "comp_time: " << ca << " / x + " << cb;
        LOG(INFO) << "write_time: " << wa << " / x + " << wb;
    }
};

struct MemoryModel {
    double parall;  // can be divided into parallel parts
    double fixed;  // fixed memory
    double input_size; // input size
    MemoryModel() : parall(0), fixed(0), input_size(0) {}
};

struct StageNode {
    int id; // stage id
    int is_single;  // if the stage is single, then its degree of parallelism has to be 1
    int is_leaf;
    int input_chunks;  // if is_leaf == 1, the input_chunks is the number of input chunks
    int to_id;
    TransferOp to_op;
    int num_from;
    int from_id[MAX_IN_DEGREE];
    int pre_id;  // the id of the stage that is executed in the prev phase
    struct MemoryModel mem_model;
    struct TimeModel time_model;
    StageNode() : id(0), is_single(0), is_leaf(0), input_chunks(0), 
        to_id(0), num_from(0), from_id(), pre_id(0) {
        to_op = TransferOp::Gather;
    }
    void fit_time_model() {
        for (int i = 0; i < NUM_SAMPLES; ++i) {
            time_model.parall_degrees[i] = degs[i];  // for profile use
        }
        time_model.fit();
    }
    double predict_total_time(int parall_degree) {
        return time_model.predict_total_time(parall_degree);
    }
    // remember to set the values from config file
    void print() {
        LOG(INFO) << "Stage id: " << id;
        LOG(INFO) << "is_single: " << is_single;
        LOG(INFO) << "is_leaf: " << is_leaf;
        LOG(INFO) << "input_chunks: " << input_chunks;
        LOG(INFO) << "to_id: " << to_id;
        LOG(INFO) << "to_op: " << int(to_op);
        LOG(INFO) << "num_from: " << num_from;
        for (int i = 0; i < num_from; i++) {
            LOG(INFO) << "from_id: " << from_id[i];
        }
        LOG(INFO) << "mem_model: " << mem_model.parall << " " << mem_model.fixed;
        LOG(INFO) << "======================================";
    }
};

struct StageDAG {
    int query_id;
    int num_stages;
    int num_leaves;
    int leaf_ids[MAX_STAGE_NUM];  // start from 0
    StageNode stages[MAX_STAGE_NUM];  // start from 1
    StageDAG() : num_stages(0), num_leaves(0) {}
    void fitTimeModel() {
        for (int i = 1; i <= num_stages; i++) {
            stages[i].fit_time_model();
        }
    }
    void printTimeModel() {
        for (int i = 1; i <= num_stages; i++) {
            stages[i].time_model.print();
            for (int j = 0; j < NUM_SAMPLES; j++) {
                auto predict = stages[i].time_model.predict_total_time(degs[j]);
                auto actual = stages[i].time_model.profiles[j].get_total_time();
                LOG(INFO) << "Predicted " << predict << " Actual " << actual << 
                    " Diff " << abs(predict - actual) / double(actual) * 100 << "%";
            }
        }
    }
    void getTimeModel(double *a, double *b) {
        for (int i = 0; i < num_stages; i++) {
            auto &m = stages[i + 1].time_model;
            a[i*5] = m.pa;
            a[i*5+1] = m.r1a;
            a[i*5+2] = m.r2a;
            a[i*5+3] = m.ca;
            a[i*5+4] = m.wa;
            b[i*5] = m.pb;
            b[i*5+1] = m.r1b;
            b[i*5+2] = m.r2b;
            b[i*5+3] = m.cb;
            b[i*5+4] = m.wb;
        }
    }
    void printStageTimeModel(int stage_id, double &a, double &b, double *actuals) {
        auto &m = stages[stage_id].time_model;
        a = m.pa + m.r1a + m.r2a + m.ca + m.wa;
        b = m.pb + m.r1b + m.r2b + m.cb + m.wb;
        for (int i = 0; i < NUM_SAMPLES; i++) {
            actuals[i] = m.predict_total_time(degs[i]);
        }
    }
    void print() {
        LOG(INFO) << "Query id: " << query_id;
        LOG(INFO) << "Number of stages: " << num_stages;
        LOG(INFO) << "Number of leaves: " << num_leaves;
        for (int i = 0; i < num_leaves; i++) {
            LOG(INFO) << "Leaf id: " << leaf_ids[i];
        }
        LOG(INFO) << "======================================";
        for (int i = 1; i <= num_stages; i++) {
            stages[i].print();
        }
    }
};

void ReadDAGConfig(StageDAG &sdag, const std::string &file_name) {
    std::ifstream fin(file_name);
    if (!fin.is_open()) {
        LOG(ERROR) << "Cannot open DAG config file";
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
        if (tokens[0] == "Query") {
            sdag.query_id = std::stoi(tokens[1]);
            continue;
        }
        if (tokens[0] == "Stage") {
            sdag.num_stages++;
            sdag.stages[sdag.num_stages].id = std::stoi(tokens[1]);
        } else if (tokens[0] == "is_single") {
            sdag.stages[sdag.num_stages].is_single = std::stoi(tokens[1]);
            if (sdag.stages[sdag.num_stages].is_single == 1) {
                for (int i = 0; i < NUM_SAMPLES; i++) {
                    sdag.stages[sdag.num_stages].time_model.parall_degrees[i] = 1;
                }
            }
        } else if (tokens[0] == "is_leaf") {
            sdag.stages[sdag.num_stages].is_leaf = std::stoi(tokens[1]);
            if (sdag.stages[sdag.num_stages].is_leaf == 1) {
                sdag.leaf_ids[sdag.num_leaves] = sdag.num_stages;  
                sdag.num_leaves++;
            }
        } else if (tokens[0] == "input_chunks") {
            sdag.stages[sdag.num_stages].input_chunks = std::stoi(tokens[1]);
        } else if (tokens[0] == "to_id") {
            sdag.stages[sdag.num_stages].to_id = std::stoi(tokens[1]);
        } else if (tokens[0] == "transfer_op") {
            if (tokens[1] == "gather") {
                sdag.stages[sdag.num_stages].to_op = TransferOp::Gather;
            } else if (tokens[1] == "allgather") {
                sdag.stages[sdag.num_stages].to_op = TransferOp::AllGather;
            } else if (tokens[1] == "broadcast") {
                sdag.stages[sdag.num_stages].to_op = TransferOp::Broadcast;
            } else if (tokens[1] == "shuffle") {
                sdag.stages[sdag.num_stages].to_op = TransferOp::Shuffle;
            } else {
                LOG(ERROR) << "Unknown transfer op";
                exit(1);
            }
        } else if (tokens[0] == "from_id") {
            sdag.stages[sdag.num_stages].num_from = tokens.size() - 1;
            for (int i = 1; i < tokens.size(); i++) {
                sdag.stages[sdag.num_stages].from_id[i - 1] = std::stoi(tokens[i]);
            }
        } else if (tokens[0] == "pre_id") {
            sdag.stages[sdag.num_stages].pre_id = std::stoi(tokens[1]);
        } else if (tokens[0] == "mem_parall") {
            sdag.stages[sdag.num_stages].mem_model.parall = std::stod(tokens[1]);
        } else if (tokens[0] == "mem_fixed") {
            sdag.stages[sdag.num_stages].mem_model.fixed = std::stod(tokens[1]);
        } else if (tokens[0] == "input_size") {
            sdag.stages[sdag.num_stages].mem_model.input_size = std::stod(tokens[1]);
        } else {
            LOG(ERROR) << "Unknown DAG config";
            exit(1);
        }
    }
    fin.close();
}

/*======================Scheduling Algorithm=======================*/
int sched_mode;  // 0 for JCT mode, 1 for cost mode
int num_executors;  // number of executors
std::vector<int> num_slots;  // num_slots[i] = number of slots in executor i
std::vector<int> slots_pre_sum;  // used in baselines' placement
int grouped[MAX_STAGE_NUM];  // grouped[i] = 1 means stage i is grouped

Stage stg[MAX_STAGE_NUM][MAX_SERVERS];  // To transform to ExecPlan

int exec_modes[MAX_STAGE_NUM];  // Execution mode of each stage, set after grouping
int degree_alloc[MAX_STAGE_NUM];  // Degree allocation of each stage
double time_weights[MAX_STAGE_NUM];  // time_weights[i] = parall_factor
double cost_weights[MAX_STAGE_NUM];  // cost_weights[i] = mem.parall * parall_factor

// used in baselines' placement
struct PlacePos {
    int executor_id;
    int slot_id;  // start from 1
};
void pos2id(struct PlacePos pos, int &id) {
    if (pos.executor_id == 0) {
        id = pos.slot_id - 1;
    } else {
        id = slots_pre_sum[pos.executor_id - 1] + pos.slot_id - 1;
    }
}
void id2pos(int id, struct PlacePos &pos) {
    int i = 0;
    while (id >= slots_pre_sum[i]) {
        i++;
    }
    pos.executor_id = i;
    pos.slot_id = id - slots_pre_sum[i - 1] + 1;
}

// start time of each stage
struct StartTime {
    int stage_id;
    long start_time;
    StartTime() : stage_id(0), start_time(0) {}
    StartTime(int id, long time) : stage_id(id), start_time(time) {}
    StartTime(const StartTime &rhs) : stage_id(rhs.stage_id), start_time(rhs.start_time) {}
    bool operator < (const StartTime &rhs) const {
        return start_time < rhs.start_time;
    }
};
StartTime start_times[MAX_STAGE_NUM];

struct ExecPlan {
    int exec_id;
    TasksToExecute tasks;
    ExecPlan() : exec_id(0), tasks() {}
    ExecPlan(const ExecPlan &rhs) : exec_id(rhs.exec_id) {
        tasks = rhs.tasks;
    }
    void print() {
        LOG(INFO) << "exec_id: " << exec_id;
        LOG(INFO) << "tasks: ";
        tasks.print();
        LOG(INFO) << "======================================" << std::endl;
    }
};
ExecPlan exec_plan[MAX_SERVERS][MAX_STAGE_NUM];

struct StagePath {
    int num_stages;
    int ids[MAX_STAGE_NUM];
    StagePath() {
        num_stages = 0;
        memset(ids, 0, sizeof(ids));
    }
    StagePath(const StagePath &sp) {
        num_stages = sp.num_stages;
        memcpy(ids, sp.ids, sizeof(ids));
    }
};
std::vector<StagePath> stage_paths;

struct StageGroup {
    int num_stages;
    int ids[MAX_STAGES_PER_GROUP];  // 2 is the max number of stages in a group
    int total_deg;
    StageGroup() : num_stages(0), total_deg(0) {
        memset(ids, 0, sizeof(int) * MAX_STAGES_PER_GROUP);
    }
    StageGroup(const StageGroup &rhs) {
        num_stages = rhs.num_stages;
        total_deg = rhs.total_deg;
        for (int i = 0; i < MAX_STAGES_PER_GROUP; i++)
            ids[i] = rhs.ids[i];
    }
    bool operator < (const StageGroup &rhs) const {
        return total_deg < rhs.total_deg;
    }
    bool operator > (const StageGroup &rhs) const {
        return total_deg > rhs.total_deg;
    }
    void print() {
        std::cout << "num_stages: " << num_stages << " " << 
            " ids: " << ids[0] << " " << ids[1] << " " << ids[2] << 
            " total_deg: " << total_deg << std::endl;
    }
};
std::vector<StageGroup> groups;

void SingleStageGroup(StageDAG &sdag) {
    for (int i = 1; i <= sdag.num_stages; ++i) {
        StageGroup grp;
        grp.num_stages = 1;
        grp.ids[0] = i;
        grp.total_deg = 0;
        groups.push_back(grp);
    }
}

void BundleToGroups(StageDAG &sdag) {
    memset(grouped, 0, sizeof(grouped));
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (!grouped[i]) {
            StageGroup grp;
            int k = 0, j = i;
            while (k < MAX_STAGES_PER_GROUP && sdag.stages[j].to_op == TransferOp::Gather) {
                grp.ids[k] = j;
                k++;
                grouped[j] = 1;
                j = sdag.stages[j].to_id;
            }
            if (k == 0) {
                grp.ids[k] = i;
                k++;
                grouped[i] = 1;
            } else if (k < MAX_STAGES_PER_GROUP) {
                grp.ids[k] = j;
                k++;
                grouped[j] = 1;
            }
            grp.num_stages = k;
            grp.total_deg = 0;
            groups.push_back(grp);
        }
    }
}

void DistributeParallelismByDataSize(StageDAG &sdag) {
    int total_slots = 0;  // total slots except for single stages
    for (int i = 0; i < num_executors; ++i) {
        total_slots += num_slots[i];
    }
    total_slots -= 2;
    double total_weight = 0;
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single)
            continue;
        // total_weight += sdag.stages[i].mem_model.parall;
        total_weight += sdag.stages[i].mem_model.input_size;
    }
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single) {
            degree_alloc[i] = 1;
            continue;
        }
        degree_alloc[i] = (int)(sdag.stages[i].mem_model.input_size 
            / total_weight * total_slots);
        if (degree_alloc[i] == 0)
            degree_alloc[i] = 1;
    }
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single)
            continue;
        std::cout << "Stage " << i << " " << degree_alloc[i] << std::endl;
    }
    std::cout << std::endl;
}

void SetWeights(StageDAG &sdag, int mode=0, double f=1) {
    memset(degree_alloc, 0, sizeof(degree_alloc));
    memset(time_weights, 0, sizeof(time_weights));
    memset(cost_weights, 0, sizeof(cost_weights));
    memset(exec_modes, 0, sizeof(exec_modes));
    for (int i = 0; i < groups.size(); i++) {
        if (groups[i].num_stages == 1) {
            StageNode &st = sdag.stages[groups[i].ids[0]];
            exec_modes[st.id] = RCW;
            time_weights[st.id] = abs(st.time_model.predict_partial_factor(RCW));
            double pre_time_weight = abs(st.time_model.predict_partial_factor(Pre));
            if (mode == 1 && (st.id == 2 || st.id == 6)) 
                time_weights[st.id] *= f;
            cost_weights[st.id] = st.mem_model.parall * (time_weights[st.id] + pre_time_weight);
        } else {
            for (int j = 0; j < groups[i].num_stages; j++) {
                StageNode &st = sdag.stages[groups[i].ids[j]];
                if (j == 0) {
                    exec_modes[st.id] = ReadComp;
                } else {
                    int pos_pre = 0, pos_id = 0;
                    StageNode &child = sdag.stages[groups[i].ids[j - 1]];
                    for (int j = 0; j < st.num_from; j++) {
                        if (st.from_id[j] == child.id)
                            pos_id = j;
                        if (st.from_id[j] == st.pre_id)
                            pos_pre = j;
                    }
                    if (pos_id == 0 || (pos_id == 1 && pos_pre == 0)) {
                        if (j == groups[i].num_stages - 1)
                            exec_modes[st.id] = R2CompWrite;
                        else
                            exec_modes[st.id] = R2Comp;
                    } else {
                        if (j == groups[i].num_stages - 1)
                            exec_modes[st.id] = R1CompWrite;
                        else
                            exec_modes[st.id] = R1Comp;
                    }
                }
                time_weights[st.id] = abs(st.time_model.predict_partial_factor(exec_modes[st.id]));
                if (mode == 1 && (st.id == 2 || st.id == 6)) 
                    time_weights[st.id] *= f;
                double pre_time_weight = abs(st.time_model.predict_partial_factor(Pre));
                cost_weights[st.id] = st.mem_model.parall * (time_weights[st.id] + pre_time_weight);
            }
        }
    }
}

void ChangeSize(StageDAG &sdag, double f) {
    sdag.stages[2].mem_model.parall *= f;
    sdag.stages[6].mem_model.parall *= f;
}

void DistributeParallelismByJCT(StageDAG &sdag) {
    double para_ratios[MAX_STAGE_NUM];
    memset(para_ratios, 0, sizeof(para_ratios));
    // calculate the parallelism ratio for each stage
    double r[MAX_STAGE_NUM];  // r[i]: the weight of the subtree rooted at i, given total degree = 1
    memset(r, 0, sizeof(r));
    for (int i = 1; i <= sdag.num_stages; ++i) {
        StageNode &st = sdag.stages[i];
        if (st.is_leaf) {
            r[i] = time_weights[i];
            para_ratios[i] = 1;
            continue;
        }
        if (st.is_single) {
            int child_id = st.from_id[0];
            r[i] = r[child_id] + time_weights[i] / para_ratios[child_id];
            para_ratios[i] = para_ratios[child_id];
            continue;
        }
        if (st.num_from == 1) {
            int child_id = st.from_id[0];
            std::vector<int> offsprings;
            std::queue<int> q;
            offsprings.clear();
            q.push(i);
            double pw = sqrt(time_weights[i]);
            double cw = sqrt(r[child_id]);
            r[i] = (r[child_id] / cw + time_weights[i] / pw) * (cw + pw);
            // update para_ratios
            para_ratios[i] = pw / (cw + pw);
            double sf = cw / (cw + pw);
            while (!q.empty()) {
                int cur_id = q.front();
                q.pop();
                StageNode &cur = sdag.stages[cur_id];
                for (int j = 0; j < cur.num_from; ++j) {
                    int cid = cur.from_id[j];
                    q.push(cid);
                    offsprings.push_back(cid);
                }
            }
            for (int j = 0; j < offsprings.size(); ++j) {
                int cid = offsprings[j];
                para_ratios[cid] *= sf;
            }
        } else {
            int child_id = st.from_id[0];
            double cws[st.num_from];
            memset(cws, 0, sizeof(cws));
            double tot_cw = 0;
            for (int j = 0; j < st.num_from; ++j) {
                int cid = st.from_id[j];
                cws[j] = r[cid];
                tot_cw += cws[j];
            }
            std::vector<int> alloffsprings;
            alloffsprings.clear();
            for (int j = 0; j < st.num_from; ++j) {
                cws[j] /= tot_cw;
                std::vector<int> offsprings;
                std::queue<int> q;
                offsprings.clear();
                q.push(st.from_id[j]);
                offsprings.push_back(st.from_id[j]);
                while (!q.empty()) {
                    int cur_id = q.front();
                    alloffsprings.push_back(cur_id);
                    q.pop();
                    StageNode &cur = sdag.stages[cur_id];
                    for (int k = 0; k < cur.num_from; ++k) {
                        int cid = cur.from_id[k];
                        q.push(cid);
                        offsprings.push_back(cid);
                    }
                }
                for (int k = 0; k < offsprings.size(); ++k) {
                    int cid = offsprings[k];
                    para_ratios[cid] *= cws[j];
                }
            }
            double cw = sqrt(r[child_id] / cws[0]);
            double pw = sqrt(time_weights[i]);
            r[i] = (r[child_id] / cws[0] / cw + time_weights[i] / pw) * (cw + pw);
            // update para_ratios
            para_ratios[i] = pw / (cw + pw);
            double sf = cw / (cw + pw);
            for (int j = 0; j < alloffsprings.size(); ++j) {
                int cid = alloffsprings[j];
                para_ratios[cid] *= sf;
            }
        }
    }
    
    int total_slots = 0;  // total slots except for single stages
    for (int i = 0; i < num_executors; ++i) {
        total_slots += num_slots[i];
    }
    total_slots -= 2;  // 2 single stages
    double total_weight = 0;
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single)
            continue;
        total_weight += para_ratios[i];
    }
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single) {
            degree_alloc[i] = 1;
            continue;
        }
        degree_alloc[i] = (int)(total_slots * para_ratios[i] / total_weight);
        if (degree_alloc[i] == 0) 
            degree_alloc[i] = 1;
    }
    for (int i = 1; i <= sdag.num_stages; ++i) {
        std::cout << "Stage " << i << " " << time_weights[i] << " " << degree_alloc[i] << std::endl;
    }
    std::cout << std::endl;
}

void DistributeParallelismByCost(StageDAG &sdag) {
    int total_slots = 0;  // total slots except for single stages
    for (int i = 0; i < num_executors; ++i) {
        total_slots += num_slots[i];
    }
    total_slots -= 2;  // 2 single stages
    double total_weight = 0;
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single)
            continue;
        total_weight += sqrt(cost_weights[i]);
    }
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single) {
            degree_alloc[i] = 1;
            continue;
        }
        cost_weights[i] = sqrt(cost_weights[i]) / total_weight;
        degree_alloc[i] = (int)(total_slots * cost_weights[i]);
        if (degree_alloc[i] == 0)
            degree_alloc[i] = 1;
    }
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single)
            continue;
        std::cout << "Stage " << i << " " << cost_weights[i] << " " << degree_alloc[i] << std::endl;
    }
    std::cout << std::endl;
}

void FineTuneDegreeAlloc(StageDAG &sdag) {
    // Fine tune the degree_alloc so that it could adapt to the input chunks
    int total_slots = 0;  // total slots except for single stages
    for (int i = 0; i < num_executors; ++i) {
        total_slots += num_slots[i];
    }
    total_slots -= 2;  // 2 single stages
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single) {
            degree_alloc[i] = 1;
        } else if (sdag.stages[i].is_leaf) {
            // round to the nearest factor of the input chunks
            StageNode &st = sdag.stages[i];
            int min_deg = degree_alloc[i];
            int max_deg = degree_alloc[i];
            while (sdag.stages[i].input_chunks % max_deg != 0)
                max_deg++;
            while (sdag.stages[i].input_chunks % min_deg != 0)
                min_deg--;
            if (max_deg - degree_alloc[i] < 5) {
                degree_alloc[i] = max_deg;
            } else {
                degree_alloc[i] = min_deg;
            }
        } else {
            for (int j = 0; j < sdag.stages[i].num_from; ++j) {
                int from_id = sdag.stages[i].from_id[j];
                if (sdag.stages[from_id].to_op == TransferOp::Gather) {
                    int new_deg = degree_alloc[i];
                    // if (new_deg > 3 && degree_alloc[from_id] % (new_deg - 1) == 0)
                    //     new_deg--;  // a small step back to decrease the total distance
                    if (degree_alloc[from_id] < new_deg)
                        new_deg = degree_alloc[from_id];
                    while (degree_alloc[from_id] % new_deg != 0) {
                        new_deg++;
                    }
                    degree_alloc[i] = new_deg;
                }
            }
        }
    }
    int allocated = 0;
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single)
            continue;
        allocated += degree_alloc[i];
    }
    int remain = total_slots - allocated;
    std::cout << "Total slots: " << total_slots << " Allocated: " << allocated << " Remain: " << remain << std::endl;
    if (remain > 0) {
        // expand the degree_alloc
        for (int i = 1; i <= sdag.num_stages; ++i) {
            if (sdag.stages[i].is_single || sdag.stages[i].is_leaf)
                continue;
            if (degree_alloc[i] > remain)
                continue;
            if (remain <= 0)
                break;
            for (int j = 0; j < sdag.stages[i].num_from; ++j) {
                int from_id = sdag.stages[i].from_id[j];
                if (sdag.stages[from_id].to_op == TransferOp::Gather) {
                    // try *2, 4, 6, 10 since they are the most common factors
                    if (remain > 9*degree_alloc[i] && 
                        degree_alloc[from_id] % (degree_alloc[i]*10) == 0) {
                        remain -= 9*degree_alloc[i];
                        degree_alloc[i] *= 10;
                    } else if (remain > 3*degree_alloc[i] && 
                        degree_alloc[from_id] % (degree_alloc[i]*4) == 0) {
                        remain -= 3*degree_alloc[i];
                        degree_alloc[i] *= 4;
                    } else if (degree_alloc[from_id] % (degree_alloc[i]*2) == 0) {
                        remain -= degree_alloc[i];
                        degree_alloc[i] *= 2;
                    } else if (remain > 5*degree_alloc[i] && 
                        degree_alloc[from_id] % (degree_alloc[i]*6) == 0) {
                        remain -= 5*degree_alloc[i];
                        degree_alloc[i] *= 6;
                    } else if (remain > 2*degree_alloc[i] && 
                        degree_alloc[from_id] % (degree_alloc[i]*3) == 0) {
                        remain -= 2*degree_alloc[i];
                        degree_alloc[i] *= 3;
                    }
                }
            }
        }
    } else if (remain < 0) {
        // shrink non-leaf stage degree
        for (int i = sdag.num_stages; i >= 1; --i) {
            if (sdag.stages[i].is_single || sdag.stages[i].is_leaf) {
                if (sdag.stages[i].is_leaf && degree_alloc[i] >= 5 && degree_alloc[i] <= 8) {
                    remain += degree_alloc[i] - 2;
                    degree_alloc[i] = 2;
                    if (remain >= 0)
                        break;
                }
                continue;
            }
            if (remain >= 0)
                break;
            for (int j = 0; j < sdag.stages[i].num_from; ++j) {
                int from_id = sdag.stages[i].from_id[j];
                if (sdag.stages[from_id].to_op == TransferOp::Gather) {
                    // try /2 since they are the most common factors
                    if (degree_alloc[i] % 2 == 0) {
                        degree_alloc[i] /= 2;
                        remain += degree_alloc[i];
                    }
                }
            }
        }
    }
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single)
            continue;
        std::cout << "Stage " << i << " " << degree_alloc[i] << std::endl;
    }
    std::cout << std::endl;
}

void FineTuneDegreeAlloc2(StageDAG &sdag) {
    // Fine tune the degree_alloc so that it could adapt to the input chunks
    int total_slots = 0;  // total slots except for single stages
    for (int i = 0; i < num_executors; ++i) {
        total_slots += num_slots[i];
    }
    total_slots -= 2;  // 2 single stages
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single) {
            degree_alloc[i] = 1;
        } else if (sdag.stages[i].is_leaf) {
            // round to the nearest factor of the input chunks
            StageNode &st = sdag.stages[i];
            int min_deg = degree_alloc[i];
            while (sdag.stages[i].input_chunks % min_deg != 0)
                min_deg--;
            degree_alloc[i] = min_deg;
        } else {
            for (int j = 0; j < sdag.stages[i].num_from; ++j) {
                int from_id = sdag.stages[i].from_id[j];
                if (sdag.stages[from_id].to_op == TransferOp::Gather) {
                    int new_deg = degree_alloc[i];
                    if (new_deg > 3 && degree_alloc[from_id] % (new_deg - 1) == 0)
                        new_deg--;  // a small step back to decrease the total distance
                    if (degree_alloc[from_id] < new_deg)
                        new_deg = degree_alloc[from_id];
                    while (degree_alloc[from_id] % new_deg != 0) {
                        new_deg++;
                    }
                    degree_alloc[i] = new_deg;
                }
            }
        }
    }
    int allocated = 0;
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single)
            continue;
        allocated += degree_alloc[i];
    }
    int remain = total_slots - allocated;
    std::cout << "Total slots: " << total_slots << " Allocated: " << allocated << " Remain: " << remain << std::endl;
    if (remain > 0) {
        // expand the degree_alloc
        for (int i = 1; i <= sdag.num_stages; ++i) {
            if (sdag.stages[i].is_single || sdag.stages[i].is_leaf)
                continue;
            if (degree_alloc[i] > remain)
                continue;
            if (remain <= 0)
                break;
            for (int j = 0; j < sdag.stages[i].num_from; ++j) {
                int from_id = sdag.stages[i].from_id[j];
                if (sdag.stages[from_id].to_op == TransferOp::Gather) {
                    // try *2, 4, 6, 10 since they are the most common factors
                    if (degree_alloc[from_id] % (degree_alloc[i]*2) == 0) {
                        remain -= degree_alloc[i];
                        degree_alloc[i] *= 2;
                    }
                }
            }
        }
    } else if (remain < 0) {
        // shrink non-leaf stage degree
        for (int i = sdag.num_stages; i >= 1; --i) {
            if (sdag.stages[i].is_single || sdag.stages[i].is_leaf)
                continue;
            if (remain >= 0)
                break;
            for (int j = 0; j < sdag.stages[i].num_from; ++j) {
                int from_id = sdag.stages[i].from_id[j];
                if (sdag.stages[from_id].to_op == TransferOp::Gather) {
                    // try /2 since they are the most common factors
                    if (degree_alloc[i] % 2 == 0) {
                        degree_alloc[i] /= 2;
                        remain += degree_alloc[i];
                    }
                }
            }
        }
    }
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_single)
            continue;
        std::cout << "Stage " << i << " " << degree_alloc[i] << std::endl;
    }
    std::cout << std::endl;
}

bool ElasticPlacement(StageDAG &sdag) {
    int min_id[MAX_SERVERS];  // min available ring id for each server
    for (int i = 0; i < num_executors; ++i)
        min_id[i] = 1;
    int num_placed[MAX_SERVERS];  // number of tasks placed on each server
    memset(num_placed, 0, sizeof(num_placed));
    // Sort the groups by the total degree
    for (int i = 0; i < groups.size(); ++i) {
        groups[i].total_deg = 0;
        for (int j = 0; j < groups[i].num_stages; ++j) {
            auto id = groups[i].ids[j];
            groups[i].total_deg += degree_alloc[id];
        }
    }
    std::sort(groups.begin(), groups.end(), std::greater<StageGroup>());

    int distri[MAX_STAGE_NUM][MAX_SERVERS]; 
    // distri[i][j]: number of tasks of stage i on server j
    memset(distri, 0, sizeof(distri));
    // place larger groups first
    for (int i = 0; i < groups.size(); ++i) {
        if (groups[i].num_stages == 1)
            continue;
        // calculate the number of task groups
        int end_id = groups[i].ids[groups[i].num_stages - 1];
        int num_task_grp = degree_alloc[end_id];  // end_deg = number of task groups
        // try to place the task groups by polling the servers
        int slots_per_grp = groups[i].total_deg / num_task_grp;
        // std::cout << "GroupEnd " << end_id << " " << num_task_grp << " " << slots_per_grp << std::endl;
        if (groups[i].total_deg % num_task_grp != 0) {
            StageGroup new_grp;
            new_grp.num_stages = 1;
            new_grp.ids[0] = groups[i].ids[groups[i].num_stages - 1];
            new_grp.total_deg = 0;
            groups.push_back(new_grp);
            groups[i].ids[groups[i].num_stages - 1] = 0;
            groups[i].num_stages -= 1;
            return false;
        }
        int tasks_per_grp[MAX_STAGES_PER_GROUP];
        memset(tasks_per_grp, 0, sizeof(tasks_per_grp));
        for (int j = 0; j < groups[i].num_stages; ++j) {
            if (j < groups[i].num_stages - 1) {
                tasks_per_grp[j] = degree_alloc[groups[i].ids[j]] / 
                    degree_alloc[groups[i].ids[groups[i].num_stages - 1]];
                if (degree_alloc[groups[i].ids[j]] % 
                    degree_alloc[groups[i].ids[groups[i].num_stages - 1]] != 0) {
                    std::cerr << "Error: degree_alloc j / j+1" << std::endl;
                    exit(1);
                }
            } else {
                tasks_per_grp[j] = 1;
            }
        }
        std::cout << "TasksPerGrp ";
        for (int j = 0; j < groups[i].num_stages; ++j) {
            std::cout << tasks_per_grp[j] << " ";
        }
        std::cout << std::endl;
        int cur_executor = -1;
        for (int j = 0; j < num_task_grp; ++j) {
            bool plc = false;
            for (int k = 0; k < num_executors; ++k) {
                cur_executor = (cur_executor + 1) % num_executors;
                if (num_placed[cur_executor] + slots_per_grp <= num_slots[cur_executor]) {
                    plc = true;
                    num_placed[cur_executor] += slots_per_grp;
                    break;
                }
            }
            if (!plc) {
                std::cout << "Cannot place group " << i << ", retrying..." << std::endl;
                // cut the last edge in the group
                StageGroup new_grp;
                new_grp.num_stages = 1;
                new_grp.ids[0] = groups[i].ids[groups[i].num_stages - 1];
                new_grp.total_deg = 0;
                groups.push_back(new_grp);
                groups[i].ids[groups[i].num_stages - 1] = 0;
                groups[i].num_stages -= 1;
                return false;
            } else {
                for (int k = 0; k < groups[i].num_stages; ++k) {
                    int stage_id = groups[i].ids[k];
                    distri[stage_id][cur_executor] += tasks_per_grp[k];
                }
            }
        }
        for (int j = 0; j < groups[i].num_stages; ++j) {
            std::cout << "Stage" << groups[i].ids[j] << " ";
            for (int k = 0; k < num_executors; ++k) {
                std::cout << distri[groups[i].ids[j]][k] << " ";
            }
            std::cout << std::endl;
        }
    }
    // place single stages
    for (int i = 0; i < groups.size(); ++i) {
        if (groups[i].num_stages != 1)
            continue;
        int stage_id = groups[i].ids[0];
        int d = degree_alloc[stage_id];
        int cur_executor = 0;  // start from 1
        for (int j = 0; j < d; ++j) {
            bool plc = false;
            for (int k = 0; k < num_executors; ++k) {
                cur_executor = (cur_executor + 1) % num_executors;
                if (num_placed[cur_executor] + 1 <= num_slots[cur_executor]) {
                    plc = true;
                    num_placed[cur_executor] += 1;
                    break;
                }
            }
            if (!plc) {
                // Place at the first executor that has enough slots, not time overlap between stages
                distri[stage_id][0] += 1;
            } else {
                distri[stage_id][cur_executor] += 1;
            }
        }
        std::cout << "Stage" << groups[i].ids[0] << " ";
        for (int k = 0; k < num_executors; ++k) {
            std::cout << distri[groups[i].ids[0]][k] << " ";
        }
        std::cout << std::endl;
    }

    std::cout << "NumPlaced ";
    for (int i = 0; i < num_executors; ++i) {
        std::cout << num_placed[i] << " ";
    }
    std::cout << std::endl;

    // transform distri to stage vector
    int distri_presum[MAX_STAGE_NUM][MAX_SERVERS];
    memset(distri_presum, 0, sizeof(distri_presum));
    for (int i = 1; i <= sdag.num_stages; ++i) {
        distri_presum[i][0] = distri[i][0];
        for (int j = 1; j < num_executors; ++j) {
            distri_presum[i][j] = distri_presum[i][j - 1] + distri[i][j];
        }
    }
    for (int i = 1; i <= sdag.num_stages; ++i) {
        for (int k = 0; k < num_executors; ++k) {
            stg[i][k].stage_id = i;
            stg[i][k].num_tasks = degree_alloc[i];
            if (k == 0) {
                stg[i][k].task_id_start = 0;
            } else {
                stg[i][k].task_id_start = distri_presum[i][k - 1];
            }
            stg[i][k].ring_id_start = min_id[k];
            stg[i][k].num_local_tasks = distri[i][k];
            min_id[k] += distri[i][k];
        }
    }
    // transform stage vector to tasks2exec vector
    for (int i = 0; i < groups.size(); ++i) {
        if (groups[i].num_stages == 1) {
            int stage_id = groups[i].ids[0];
            for (int j = 0; j < num_executors; ++j) {
                exec_plan[j][stage_id].exec_id = j;
                TasksToExecute tasks;
                tasks.curr_stage = stg[stage_id][j];
                if (stage_id == sdag.num_stages) {
                    tasks.to_stage = Stage(10, 1, 0, 0, 0);
                } else {
                    tasks.to_stage = stg[sdag.stages[stage_id].to_id][j];
                    tasks.to_stage.num_local_tasks = 0;  // not grouped
                }
                if (sdag.stages[stage_id].is_leaf) {
                    tasks.from_stage[0] = Stage(0, sdag.stages[stage_id].input_chunks, 0, 0, 0);
                } else {
                    for (int k = 0; k < sdag.stages[stage_id].num_from; ++k) {
                        tasks.from_stage[k] = stg[sdag.stages[stage_id].from_id[k]][j];
                        tasks.from_stage[k].num_local_tasks = 0;  // not grouped
                    }
                }
                exec_plan[j][stage_id].tasks = tasks;
            }
        } else {
            for (int p = 0; p < groups[i].num_stages; ++p) {
                int stage_id = groups[i].ids[p];
                for (int j = 0; j < num_executors; ++j) {
                    exec_plan[j][stage_id].exec_id = j;
                    TasksToExecute tasks;
                    tasks.curr_stage = stg[stage_id][j];
                    if (stage_id == sdag.num_stages) {
                        tasks.to_stage = Stage(10, 1, 0, 0, 0);
                    } else {
                        tasks.to_stage = stg[sdag.stages[stage_id].to_id][j];
                        if (p == groups[i].num_stages - 1) {
                            tasks.to_stage.num_local_tasks = 0;  // not grouped
                        }
                    }
                    if (sdag.stages[stage_id].is_leaf) {
                        tasks.from_stage[0] = Stage(0, sdag.stages[stage_id].input_chunks, 0, 0, 0);
                    } else {
                        for (int k = 0; k < sdag.stages[stage_id].num_from; ++k) {
                            int from_id = sdag.stages[stage_id].from_id[k];
                            tasks.from_stage[k] = stg[from_id][j];
                            if (p == 0 || (p > 0 && from_id != groups[i].ids[p - 1])) {
                                tasks.from_stage[k].num_local_tasks = 0;  // not grouped
                            }
                        }
                    }
                    exec_plan[j][stage_id].tasks = tasks;
                }
            }
        }
    }

    return true;
}

void ElasticNimble(StageDAG &sdag, int mode=JCT) {
    long exec_times[MAX_STAGE_NUM];
    memset(exec_times, 0, sizeof(exec_times));
    long pre_times[MAX_STAGE_NUM];
    memset(pre_times, 0, sizeof(pre_times));
    for (int i = 1; i <= sdag.num_stages; ++i) {
        start_times[i].stage_id = i;
        start_times[i].start_time = 0;
        int d = degree_alloc[i];
        if (sdag.stages[i].is_single) {
            d = degree_alloc[sdag.stages[i].from_id[0]];
        }
        exec_times[i] = abs(sdag.stages[i].time_model.predict_time(exec_modes[i], d));
        pre_times[i] = abs(sdag.stages[i].time_model.predict_time(Pre, d));
    }

    // dp, ms
    for (int i = 1; i <= sdag.num_stages; ++i) {
        StageNode &s = sdag.stages[i];
        if (s.is_leaf)
            continue;
        long max_time = 0;
        int max_child_pos = -1;
        for (int j = 0; j < s.num_from; ++j) {
            int from_id = s.from_id[j];
            StageNode &from_s = sdag.stages[from_id];            
            long time = 
                start_times[from_id].start_time + exec_times[from_id] + pre_times[from_id];
            if (time > max_time) {
                max_time = time;
                max_child_pos = j;
            }
        }
        start_times[i].start_time = max_time - pre_times[i];
        if (start_times[i].start_time < 0) {
            start_times[i].start_time = 0;
        }
        if (s.num_from > 1) {
            // delay the computation of other children
            int parent_grp = -1;
            for (int j = 0; j < groups.size(); ++j) {
                if (groups[j].num_stages == 1)
                    continue;
                for (int k = 0; k < groups[j].num_stages; ++k) {
                    if (groups[j].ids[k] == i) {
                        parent_grp = j;
                        break;
                    }
                }
            }
            if (parent_grp >= 0) {
                for (int j = 0; j < s.num_from; ++j) {
                    if (j == max_child_pos)
                        continue;
                    int from_id = s.from_id[j];
                    StageNode &from_s = sdag.stages[from_id];
                    if (from_s.to_op != TransferOp::Gather) 
                        continue;
                    start_times[from_id].start_time = 
                        start_times[i].start_time - exec_times[from_id] - pre_times[from_id];
                    // check if from_id and s_id are in the same group
                    int child_grp = -1;
                    for (int k = 0; k < groups[parent_grp].num_stages; ++k) {
                        if (groups[parent_grp].ids[k] == from_id) {
                            child_grp = parent_grp;
                            break;
                        }
                    }
                    if (child_grp >= 0) {
                        int early_time = 1500;
                        if (mode == JCT) {
                            early_time = 6000;
                        }
                        start_times[from_id].start_time += 
                            long(s.time_model.predict_time(Pre, degree_alloc[i])) - early_time;
                        if (j > 0) {
                            start_times[from_id].start_time += 
                                long(s.time_model.predict_time(OnlyR1, degree_alloc[i]));
                        }
                    }
                    if (start_times[from_id].start_time < 0) {
                        start_times[from_id].start_time = 0;
                    }
                }
            }
        }
    }
    // us
    for (int i = 1; i <= sdag.num_stages; ++i) {
        start_times[i].start_time *= 1000;
    }
    std::sort(start_times + 1, start_times + sdag.num_stages + 1);
    for (int i = 1; i <= sdag.num_stages; ++i) {
        LOG(INFO) << "Stage " << start_times[i].stage_id << " starts at " << start_times[i].start_time;
    }
}

// the main function
void ElasticParallel(StageDAG &sdag, int spec=0) {
    if (!spec)
        BundleToGroups(sdag);  // bundle the stages as possible as we can
    else
        SingleStageGroup(sdag);
    bool place_success = false;  // whether the placement is successful
    while (!place_success) {
        SetWeights(sdag);
        if (sched_mode == JCT) {
            DistributeParallelismByJCT(sdag);
        } else {
            DistributeParallelismByCost(sdag);
        }
        FineTuneDegreeAlloc(sdag);
        place_success = ElasticPlacement(sdag);
    }
    ElasticNimble(sdag, sched_mode);
}

void ElasticParallelNew(StageDAG &sdag, double f, int spec=0) {
    if (!spec)
        BundleToGroups(sdag);  // bundle the stages as possible as we can
    else
        SingleStageGroup(sdag);
    bool place_success = false;  // whether the placement is successful
    ChangeSize(sdag, f);
    while (!place_success) {
        SetWeights(sdag, 1, f);
        if (sched_mode == JCT) {
            DistributeParallelismByJCT(sdag);
        } else {
            DistributeParallelismByCost(sdag);
        }
        FineTuneDegreeAlloc(sdag);
        place_success = ElasticPlacement(sdag);
    }
    ElasticNimble(sdag, sched_mode);
}

void SetWeightsOptimal(StageDAG &sdag) {
    memset(degree_alloc, 0, sizeof(degree_alloc));
    memset(time_weights, 0, sizeof(time_weights));
    memset(cost_weights, 0, sizeof(cost_weights));
    memset(exec_modes, 0, sizeof(exec_modes));
    for (int i = 1; i <= sdag.num_stages; ++i) {
        StageNode &s = sdag.stages[i];
        if (i == sdag.num_stages) {
            exec_modes[i] = CompWrite;
        } else if (s.is_single) {
            exec_modes[i] = RCW;
        } else if (s.is_leaf) {
            exec_modes[i] = ReadComp;
        } else {
            if (sdag.stages[s.to_id].is_single) {
                exec_modes[i] = CompWrite;
            } else {
                int p = -1;
                for (int j = 0; j < s.num_from; ++j) {
                    if (sdag.stages[s.from_id[j]].is_single) {
                        p = j;
                        break;
                    }
                }
                if (p == -1) {
                    exec_modes[i] = OptComp;
                } else if (p == 0) {
                    exec_modes[i] = R1Comp;
                } else {
                    exec_modes[i] = R2Comp;
                }
            }
        }
        time_weights[i] = abs(s.time_model.predict_partial_factor(exec_modes[i]));
        double pre_time_weight = abs(s.time_model.predict_partial_factor(Pre));
        cost_weights[i] = s.mem_model.parall * (time_weights[i] + pre_time_weight);
    }
    for (int i = 1; i <= sdag.num_stages; ++i) {
        LOG(INFO) << "Stage " << i << " exec_modes: " << exec_modes[i];
    }
}

void CalculateOptimalJCT(StageDAG &sdag) {
    stage_paths.clear();
    for (int i = 1; i <= sdag.num_stages; ++i) {
        if (sdag.stages[i].is_leaf) {
            StagePath path;
            int j = i;
            while (j != -1) {
                path.ids[path.num_stages] = j;
                path.num_stages++;
                j = sdag.stages[j].to_id;
            }
            stage_paths.push_back(path);
        }
    }
    double max_time = 0;
    for (int i = 0; i < stage_paths.size(); ++i) {
        double total_time = 0;
        // std::cout << "Path " << i << ": ";
        for (int j = 0; j < stage_paths[i].num_stages; ++j) {
            int s_id = stage_paths[i].ids[j];
            StageNode &s = sdag.stages[s_id];
            int d = degree_alloc[s_id];
            if (s.is_single) {
                d = degree_alloc[s.from_id[0]];
            }
            double exec_time = abs(s.time_model.predict_time(exec_modes[s_id], d));
            if (s.is_leaf)
                exec_time += abs(s.time_model.predict_time(Pre, degree_alloc[s_id]));
            // std::cout << exec_time << " ";
            total_time += exec_time;
        }
        std::cout << std::endl;
        if (total_time > max_time) {
            max_time = total_time;
        }
    }
    // max_time *= ERROR_RATIO;
    LOG(INFO) << "Lower bound of JCT: " << max_time;
}

void CalculateOptimalCost(StageDAG &sdag) {
    double total_cost = 0;
    for (int i = 1; i <= sdag.num_stages; ++i) {
        StageNode &s = sdag.stages[i];
        double mem_size = s.mem_model.fixed + s.mem_model.parall / degree_alloc[i];
        int d = degree_alloc[i];
        if (s.is_single) {
            d = degree_alloc[s.from_id[0]];
        }
        double exec_time = s.time_model.predict_time(exec_modes[i], d);
        exec_time += abs(s.time_model.predict_time(Pre, d));
        if (!s.is_leaf)
            exec_time += 1500;
        total_cost += degree_alloc[i] * mem_size * exec_time / 1000;
    }
    // total_cost *= ERROR_RATIO;
    LOG(INFO) << "Lower bound of cost: " << total_cost;
}

void PredictCost(StageDAG &sdag, double f=1) {
    double total_cost = 0;
    for (int i = 1; i <= sdag.num_stages; ++i) {
        StageNode &s = sdag.stages[i];
        double mem_size = s.mem_model.fixed + s.mem_model.parall / degree_alloc[i];
        int d = degree_alloc[i];
        if (s.is_single) {
            d = degree_alloc[s.from_id[0]];
        }
        double exec_time = s.time_model.predict_time(exec_modes[i], d);
        if (i == 2 || i == 6)
            exec_time *= f;
        exec_time += abs(s.time_model.predict_time(Pre, d));
        if (!s.is_leaf)
            exec_time += 1500;
        total_cost += degree_alloc[i] * mem_size * exec_time / 1000;
    }
    // total_cost *= ERROR_RATIO;
    LOG(INFO) << "Predicted cost: " << total_cost;
}

void SimulateLowerBound(StageDAG &sdag) {
    SetWeightsOptimal(sdag);
    if (sched_mode == JCT) {
        DistributeParallelismByJCT(sdag);
        FineTuneDegreeAlloc(sdag);
        CalculateOptimalJCT(sdag);
    } else {
        DistributeParallelismByCost(sdag);
        FineTuneDegreeAlloc(sdag);
        CalculateOptimalCost(sdag);
    }
}

/* Naive baseline: Nimble */
void RandomUniformPlacement(StageDAG &sdag, int general_deg) {
    int min_id[MAX_SERVERS];
    memset(min_id, 0, sizeof(min_id));
    int distri[MAX_SERVERS];
    memset(distri, 0, sizeof(distri));
    int total_slots = slots_pre_sum[num_executors - 1];
    std::cout << "Total slots place: " << total_slots << std::endl;
    // Randomly distribute the slots
    int tot = 0;
    for (int i = 0; i < num_executors; ++i) {
        distri[i] = general_deg * (double)num_slots[i] / total_slots;
        tot += distri[i];
    }
    int rem = general_deg - tot;  // remaining tasks
    std::cout << "Remaining tasks: " << rem << std::endl;
    for (int i = 0; i < rem; ++i) {
        distri[i] += 1;
    }
    for (int i = 0; i < num_executors; ++i) {
        std::cout << distri[i] << " ";
    }
    std::cout << std::endl;
    for (int i = 1; i <= sdag.num_stages; ++i) {
        int cnt = 0;
        for (int j = 0; j < num_executors; ++j) {
            stg[i][j].stage_id = i;
            int start_id = min_id[j];
            if (j > 0)
                start_id += slots_pre_sum[j - 1];
            struct PlacePos pos_start;
            id2pos(start_id, pos_start);
            if (sdag.stages[i].is_single) {
                stg[i][j].num_tasks = 1;
                if (j == 0) {
                    stg[i][j].task_id_start = 0;
                    stg[i][j].ring_id_start = pos_start.slot_id;
                    stg[i][j].num_local_tasks = 1;
                } else {
                    stg[i][j].task_id_start = 0;
                    stg[i][j].ring_id_start = 0;
                    stg[i][j].num_local_tasks = 0;
                }
            } else {
                stg[i][j].num_tasks = general_deg;
                stg[i][j].task_id_start = cnt;
                stg[i][j].ring_id_start = pos_start.slot_id;
                stg[i][j].num_local_tasks = distri[j];
            }
            min_id[j] += stg[i][j].num_local_tasks;
            cnt += distri[j];
        }
    }

    for (int i = 0; i < num_executors; ++i) {
        for (int j = 1; j <= sdag.num_stages; ++j) {
            exec_plan[i][j].exec_id = i;
            TasksToExecute tasks;
            tasks.curr_stage = stg[j][i];
            if (j == sdag.num_stages) {
                tasks.to_stage = Stage(10, 1, 0, 0, 0);
            } else {
                tasks.to_stage = stg[sdag.stages[j].to_id][i];
                tasks.to_stage.num_local_tasks = 0;  // to simulate placement-unaware scheduling
            }
            if (sdag.stages[j].is_leaf) {
                tasks.from_stage[0] = Stage(0, sdag.stages[j].input_chunks, 0, 0, 0);
            } else {
                for (int k = 0; k < sdag.stages[j].num_from; ++k) {
                    tasks.from_stage[k] = stg[sdag.stages[j].from_id[k]][i];
                    tasks.from_stage[k].num_local_tasks = 0;
                }
            }
            exec_plan[i][j].tasks = tasks;
        }
    }
}

void RandomUniformPlacement2(StageDAG &sdag) {
    int min_id[MAX_SERVERS];
    memset(min_id, 0, sizeof(min_id));
    int total_slots = slots_pre_sum[num_executors - 1];
    std::cout << "Total slots place: " << total_slots << std::endl;
    // Randomly distribute the slots
    std::cout << std::endl;
    for (int i = 1; i <= sdag.num_stages; ++i) {
        int cnt = 0;
        int distri[MAX_SERVERS];
        memset(distri, 0, sizeof(distri));
        int tot = 0;
        for (int j = 0; j < num_executors; ++j) {
            distri[j] = degree_alloc[i] * (double)num_slots[j] / total_slots;
            tot += distri[j];
        }
        int rem = degree_alloc[i] - tot;  // remaining tasks
        // std::cout << "Remaining tasks: " << rem << std::endl;
        for (int j = 0; j < rem; ++j) {
            distri[j] += 1;
        }
        // for (int j = 0; j < num_executors; ++j) {
        //     std::cout << distri[j] << " ";
        // }
        for (int j = 0; j < num_executors; ++j) {
            stg[i][j].stage_id = i;
            int start_id = min_id[j];
            if (j > 0)
                start_id += slots_pre_sum[j - 1];
            struct PlacePos pos_start;
            id2pos(start_id, pos_start);
            if (sdag.stages[i].is_single) {
                stg[i][j].num_tasks = 1;
                if (j == 0) {
                    stg[i][j].task_id_start = 0;
                    stg[i][j].ring_id_start = pos_start.slot_id;
                    stg[i][j].num_local_tasks = 1;
                } else {
                    stg[i][j].task_id_start = 0;
                    stg[i][j].ring_id_start = 0;
                    stg[i][j].num_local_tasks = 0;
                }
            } else {
                stg[i][j].num_tasks = degree_alloc[i];
                stg[i][j].task_id_start = cnt;
                stg[i][j].ring_id_start = pos_start.slot_id;
                stg[i][j].num_local_tasks = distri[j];
            }
            min_id[j] += stg[i][j].num_local_tasks;
            cnt += distri[j];
        }
    }

    for (int i = 0; i < num_executors; ++i) {
        for (int j = 1; j <= sdag.num_stages; ++j) {
            exec_plan[i][j].exec_id = i;
            TasksToExecute tasks;
            tasks.curr_stage = stg[j][i];
            if (j == sdag.num_stages) {
                tasks.to_stage = Stage(10, 1, 0, 0, 0);
            } else {
                tasks.to_stage = stg[sdag.stages[j].to_id][i];
                tasks.to_stage.num_local_tasks = 0;  // to simulate placement-unaware scheduling
            }
            if (sdag.stages[j].is_leaf) {
                tasks.from_stage[0] = Stage(0, sdag.stages[j].input_chunks, 0, 0, 0);
            } else {
                for (int k = 0; k < sdag.stages[j].num_from; ++k) {
                    tasks.from_stage[k] = stg[sdag.stages[j].from_id[k]][i];
                    tasks.from_stage[k].num_local_tasks = 0;
                }
            }
            exec_plan[i][j].tasks = tasks;
        }
    }
}

void NaiveNimble(StageDAG &sdag, int general_deg) {
    // initialization
    long exec_times[MAX_STAGE_NUM];
    memset(exec_times, 0, sizeof(exec_times));
    long pre_times[MAX_STAGE_NUM];
    memset(pre_times, 0, sizeof(pre_times));
    for (int i = 1; i <= sdag.num_stages; ++i) {
        start_times[i].stage_id = i;
        start_times[i].start_time = 0;
        exec_times[i] = abs(sdag.stages[i].time_model.predict_time(RCW, general_deg));
        pre_times[i] = abs(sdag.stages[i].time_model.predict_time(Pre, general_deg));
    }
    // dp, ms
    for (int i = 1; i <= sdag.num_stages; ++i) {
        StageNode &s = sdag.stages[i];
        if (s.is_leaf)
            continue;
        long max_time = 0;
        for (int j = 0; j < s.num_from; ++j) {
            int from_id = s.from_id[j];
            long time = start_times[from_id].start_time + pre_times[from_id] + exec_times[from_id];
            if (time > max_time)
                max_time = time;
        }
        max_time -= pre_times[i];
        start_times[i].start_time = max_time;
        if (start_times[i].start_time < 0)
            start_times[i].start_time = 0;
    }
    // us
    for (int i = 1; i <= sdag.num_stages; ++i) {
        start_times[i].start_time *= 1000;
    }
    std::sort(start_times + 1, start_times + sdag.num_stages + 1);
    for (int i = 1; i <= sdag.num_stages; ++i) {
        LOG(INFO) << "Stage " << start_times[i].stage_id << " starts at " << start_times[i].start_time;
    }
}

void NaiveNimble2(StageDAG &sdag) {
    // initialization
    long exec_times[MAX_STAGE_NUM];
    memset(exec_times, 0, sizeof(exec_times));
    long pre_times[MAX_STAGE_NUM];
    memset(pre_times, 0, sizeof(pre_times));
    for (int i = 1; i <= sdag.num_stages; ++i) {
        start_times[i].stage_id = i;
        start_times[i].start_time = 0;
        int d = degree_alloc[i];
        if (sdag.stages[i].is_single) {
            d = degree_alloc[sdag.stages[i].from_id[0]];
        }
        exec_times[i] = abs(sdag.stages[i].time_model.predict_time(RCW, d));
        pre_times[i] = abs(sdag.stages[i].time_model.predict_time(Pre, d));
    }

    for (int i = 1; i <= sdag.num_stages; ++i) {
        LOG(INFO) << "Stage " << i << " exec time: " << exec_times[i] << " pre time: " << pre_times[i];
    }

    // dp, ms
    for (int i = 1; i <= sdag.num_stages; ++i) {
        StageNode &s = sdag.stages[i];
        if (s.is_leaf)
            continue;
        long max_time = 0;
        for (int j = 0; j < s.num_from; ++j) {
            int from_id = s.from_id[j];
            long time = start_times[from_id].start_time + pre_times[from_id] + exec_times[from_id];
            if (time > max_time)
                max_time = time;
        }
        max_time -= pre_times[i];
        start_times[i].start_time = max_time;
        if (start_times[i].start_time < 0)
            start_times[i].start_time = 0;
    }
    // us
    for (int i = 1; i <= sdag.num_stages; ++i) {
        start_times[i].start_time *= 1000;
    }
    std::sort(start_times + 1, start_times + sdag.num_stages + 1);
    for (int i = 1; i <= sdag.num_stages; ++i) {
        LOG(INFO) << "Stage " << start_times[i].stage_id << " starts at " << start_times[i].start_time;
    }
}

/* Nimble + Greedy */
void GreedyPlacement(StageDAG &sdag, int general_deg) {
    int min_id[MAX_SERVERS];
    memset(min_id, 0, sizeof(min_id));
    int distri[MAX_SERVERS];
    memset(distri, 0, sizeof(distri));
    int total_slots = slots_pre_sum[num_executors - 1];
    // Randomly distribute the slots
    int tot = 0;
    for (int i = 0; i < num_executors; ++i) {
        distri[i] = general_deg * (double)num_slots[i] / total_slots;
        tot += distri[i];
    }
    int rem = general_deg - tot;  // remaining tasks
    for (int i = 0; i < rem; ++i) {
        distri[i] += 1;
    }
    for (int i = 1; i <= sdag.num_stages; ++i) {
        int cnt = 0;
        for (int j = 0; j < num_executors; ++j) {
            stg[i][j].stage_id = i;
            int start_id = min_id[j];
            if (j > 0)
                start_id += slots_pre_sum[j - 1];
            struct PlacePos pos_start;
            id2pos(start_id, pos_start);
            if (sdag.stages[i].is_single) {
                stg[i][j].num_tasks = 1;
                if (j == 0) {
                    stg[i][j].task_id_start = 0;
                    stg[i][j].ring_id_start = pos_start.slot_id;
                    stg[i][j].num_local_tasks = 1;
                } else {
                    stg[i][j].task_id_start = 0;
                    stg[i][j].ring_id_start = 0;
                    stg[i][j].num_local_tasks = 0;
                }
            } else {
                stg[i][j].num_tasks = general_deg;
                stg[i][j].task_id_start = cnt;
                stg[i][j].ring_id_start = pos_start.slot_id;
                stg[i][j].num_local_tasks = distri[j];
            }
            min_id[j] += stg[i][j].num_local_tasks;
            cnt += distri[j];
        }
    }

    for (int i = 0; i < num_executors; ++i) {
        for (int j = 1; j <= sdag.num_stages; ++j) {
            exec_plan[i][j].exec_id = i;
            TasksToExecute tasks;
            tasks.curr_stage = stg[j][i];
            if (j == sdag.num_stages) {
                tasks.to_stage = Stage(10, 1, 0, 0, 0);
            } else {
                int to_id = sdag.stages[j].to_id;
                tasks.to_stage = stg[to_id][i];
                if (sdag.stages[j].to_op != TransferOp::Gather) {
                    tasks.to_stage.num_local_tasks = 0;
                }
            }
            if (sdag.stages[j].is_leaf) {
                tasks.from_stage[0] = Stage(0, sdag.stages[j].input_chunks, 0, 0, 0);
            } else {
                for (int k = 0; k < sdag.stages[j].num_from; ++k) {
                    int from_id = sdag.stages[j].from_id[k];
                    tasks.from_stage[k] = stg[from_id][i];
                    if (sdag.stages[from_id].to_op != TransferOp::Gather) {
                        tasks.from_stage[k].num_local_tasks = 0;
                    }
                }
            }
            exec_plan[i][j].tasks = tasks;
            // tasks.print();
        }
    }
}

void GreedyNimble(StageDAG &sdag, int general_deg) {
    // initialization
    long exec_times[MAX_STAGE_NUM];
    memset(exec_times, 0, sizeof(exec_times));
    long pre_times[MAX_STAGE_NUM];
    memset(pre_times, 0, sizeof(pre_times));
    int time_modes[MAX_STAGE_NUM];
    memset(time_modes, 0, sizeof(time_modes));
    for (int i = 1; i <= sdag.num_stages; ++i) {
        start_times[i].stage_id = i;
        start_times[i].start_time = 0;
        StageNode &s = sdag.stages[i];
        if (s.is_leaf) {
            time_modes[i] = RCW;
        } else {
            int gather_pos = -1, pre_pos;
            for (int j = 0; j < s.num_from; ++j) {
                int from_id = s.from_id[j];
                StageNode &from_s = sdag.stages[from_id];
                if (from_s.to_op == TransferOp::Gather) {
                    gather_pos = j;
                }
                if (s.pre_id == from_id) {
                    pre_pos = j;
                }
            }
            if (gather_pos == -1) {
                time_modes[i] = RCW;
            } else if (gather_pos == 0 || (gather_pos == 1 && pre_pos == 0)) {
                time_modes[i] = R2CompWrite;
            } else {
                time_modes[i] = R1CompWrite;
            }
        }
        exec_times[i] = abs(sdag.stages[i].time_model.predict_time(time_modes[i], general_deg));
        pre_times[i] = abs(sdag.stages[i].time_model.predict_time(Pre, general_deg));
    }

    // dp, ms
    for (int i = 1; i <= sdag.num_stages; ++i) {
        StageNode &s = sdag.stages[i];
        if (s.is_leaf)
            continue;
        long max_time = 0;
        int max_child_pos = -1;
        for (int j = 0; j < s.num_from; ++j) {
            int from_id = s.from_id[j];
            StageNode &from_s = sdag.stages[from_id];    
            long time = 
                start_times[from_id].start_time + exec_times[from_id] + pre_times[from_id];
            if (from_s.to_op == TransferOp::Gather) {
                time -= long(from_s.time_model.predict_time(OnlyWrite, general_deg));
            }
            if (time > max_time) {
                max_time = time;
                max_child_pos = j;
            }
        }
        max_time -= pre_times[i];
        start_times[i].start_time = max_time;
        if (start_times[i].start_time < 0) {
            start_times[i].start_time = 0;
        }
        if (s.num_from > 1) {
            // delay the computation of the other children
            for (int j = 0; j < s.num_from; ++j) {
                if (j == max_child_pos)
                    continue;
                int from_id = s.from_id[j];
                StageNode &from_s = sdag.stages[from_id];
                if (from_s.to_op != TransferOp::Gather)
                    continue;
                long time = exec_times[from_id] + pre_times[from_id];
                if (from_s.to_op == TransferOp::Gather) {
                    time -= long(from_s.time_model.predict_time(OnlyWrite, general_deg));
                }
                start_times[from_id].start_time = start_times[i].start_time - time;
                // delay as much as possible
                start_times[from_id].start_time += 
                    long(s.time_model.predict_time(Pre, general_deg)) - 3000;
                if (j > 0)
                    start_times[from_id].start_time += 
                        long(s.time_model.predict_time(OnlyR1, general_deg));
                if (start_times[from_id].start_time < 0) {
                    start_times[from_id].start_time = 0;
                }
            }
        }
    }
    // us
    for (int i = 1; i <= sdag.num_stages; ++i) {
        start_times[i].start_time *= 1000;
    }
    std::sort(start_times + 1, start_times + sdag.num_stages + 1);
    for (int i = 1; i <= sdag.num_stages; ++i) {
        LOG(INFO) << "Stage " << start_times[i].stage_id << " starts at " << start_times[i].start_time;
    }
}

void NewNimble(StageDAG &sdag) {
    double f = 1.0;
    DistributeParallelismByDataSize(sdag);
    FineTuneDegreeAlloc2(sdag);
    RandomUniformPlacement2(sdag);
    NaiveNimble2(sdag);
    PredictCost(sdag, f);
}

void NewGreedy(StageDAG &sdag) {
    BundleToGroups(sdag);  // bundle the stages as possible as we can
    bool place_success = false;  // whether the placement is successful
    while (!place_success) {
        SetWeights(sdag);
        DistributeParallelismByDataSize(sdag);
        FineTuneDegreeAlloc2(sdag);
        place_success = ElasticPlacement(sdag);
    }
    ElasticNimble(sdag, sched_mode);
    PredictCost(sdag, 1.0);
}