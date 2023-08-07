/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#ifndef TASK_HPP
#define TASK_HPP

#include <glog/logging.h>
#include "spright_inc.hpp"
#include "utils.hpp"

#define MAX_IN_DEGREE 3
#define MAX_STAGE_NUM 12

struct Stage {
    int stage_id;  // stage_id = -1 means no stage
    int num_tasks;
    int task_id_start;
    int ring_id_start; // one cpu core corresponds to one ring, this is also the cpu_id
    int num_local_tasks;
    Stage() : stage_id(-1), num_tasks(0), task_id_start(0), 
        ring_id_start(1), num_local_tasks(0) {}
    Stage(int _sid, int num, int tid_s, int rid_s, int lnum) : 
        stage_id(_sid), num_tasks(num), task_id_start(tid_s), 
        ring_id_start(rid_s), num_local_tasks(lnum) {}
    Stage(const Stage &s) : 
        stage_id(s.stage_id), num_tasks(s.num_tasks), 
        task_id_start(s.task_id_start), ring_id_start(s.ring_id_start),
        num_local_tasks(s.num_local_tasks) {}
    // override operator =
    Stage& operator=(const Stage &s) {
        stage_id = s.stage_id;
        num_tasks = s.num_tasks;
        task_id_start = s.task_id_start;
        ring_id_start = s.ring_id_start;
        num_local_tasks = s.num_local_tasks;
        return *this;
    }
    void print() {
        LOG(INFO) << "stage_id: " << stage_id << ", num_tasks: " << num_tasks << 
            ", task_id_start: " << task_id_start << ", ring_id_start: " << ring_id_start << 
            ", num_local_tasks: " << num_local_tasks;
    }
};

struct TasksToExecute {
    Stage curr_stage;
    // For now, we only support one upstream and one downstream, i.e. Tree-like DAG
    Stage to_stage;
    Stage from_stage[MAX_IN_DEGREE];
    
    TasksToExecute() :
        curr_stage(), to_stage(), from_stage() {}
    TasksToExecute(Stage _curr, Stage _to) : 
        curr_stage(_curr), to_stage(_to), from_stage() {}
    TasksToExecute(Stage _curr, Stage _to, Stage _from) :
        curr_stage(_curr), to_stage(_to), from_stage() {
        from_stage[0] = _from;
    }
    TasksToExecute(Stage _curr, Stage _to, Stage _from1, Stage _from2) :
        curr_stage(_curr), to_stage(_to), from_stage() {
        from_stage[0] = _from1;
        from_stage[1] = _from2;
    }
    TasksToExecute(Stage _curr, Stage _to, Stage _from1, Stage _from2, Stage _from3) :
        curr_stage(_curr), to_stage(_to), from_stage() {
        from_stage[0] = _from1;
        from_stage[1] = _from2;
        from_stage[2] = _from3;
    }
    TasksToExecute(const TasksToExecute &t) : 
        curr_stage(t.curr_stage), to_stage(t.to_stage) {
        
        for (int i = 0; i < MAX_IN_DEGREE; i++) {
            from_stage[i] = t.from_stage[i];
        }
    }
    // override operator =
    TasksToExecute& operator=(const TasksToExecute &t) {
        curr_stage = t.curr_stage;
        to_stage = t.to_stage;
        for (int i = 0; i < MAX_IN_DEGREE; i++) {
            from_stage[i] = t.from_stage[i];
        }
        return *this;
    }
    void print() {
        LOG(INFO) << "curr_stage: ";
        curr_stage.print();
        LOG(INFO) << "to_stage: ";
        to_stage.print();
        LOG(INFO) << "from_stage: ";
        for (int i = 0; i < MAX_IN_DEGREE; i++) {
            from_stage[i].print();
        }
    }
};

struct Task {
    int task_id;
    int ring_id;  // one cpu core corresponds to one ring, this is also the cpu_id
    TasksToExecute meta;  // meta data of the task
    Task(int _tid, int _rid, TasksToExecute _meta) : 
        task_id(_tid), ring_id(_rid), meta(_meta) {}
};

enum class TransferOp{  // Data transfer operation
    Gather,
    AllGather,
    Broadcast,
    Shuffle
};

struct Transfer {
    int task_id;
    int ring_id;
    Stage from_stage;
    Stage to_stage;
    TransferOp op;
    Transfer(int tid, int rid, Stage _from, Stage _to, TransferOp _op) : 
        task_id(tid), ring_id(rid), 
        from_stage(_from), to_stage(_to), op(_op) {}
    void print() {
        LOG(INFO) << "task_id: " << task_id << ", ring_id: " << ring_id << 
            ", from_stage: " << from_stage.stage_id << ", to_stage: " << to_stage.stage_id << 
            ", op: " << static_cast<int>(op);
    }
};

struct NimbleProfile {
    struct PreTime {
        time_t tpre;
        time_t tread;
        time_t tcomp;
        PreTime() : tpre(0), tread(0), tcomp(0) {}
    } pre_time;
    struct EffectTime {
        time_t tread1;
        time_t tread2;
        time_t tcomp;
        time_t twrite;
        time_t tpost;
        EffectTime() : tread1(0), tread2(0), tcomp(0), twrite(0), tpost(0) {}
    } effect_time;
    NimbleProfile() : pre_time(), effect_time() {}
    void print() {
        LOG(INFO) << "PreTime: tpre: " << pre_time.tpre << ", tread: " << pre_time.tread << 
            ", tcomp: " << pre_time.tcomp;
        LOG(INFO) << "EffectTime: tread1: " << effect_time.tread1 << ", tread2: " << 
            effect_time.tread2 << ", tcomp: " << effect_time.tcomp << ", twrite: " <<
            effect_time.twrite << ", tpost: " << effect_time.tpost;
    }
    void print_to_file(const std::string &filename) {
        std::ofstream fout(filename, std::ios::app);
        fout << "PreTime.tpre " << pre_time.tpre << std::endl;
        fout << "PreTime.tread " << pre_time.tread << std::endl;
        fout << "PreTime.tcomp " << pre_time.tcomp << std::endl;
        fout << "EffectTime.tread1 " << effect_time.tread1 << std::endl;
        fout << "EffectTime.tread2 " << effect_time.tread2 << std::endl;
        fout << "EffectTime.tcomp " << effect_time.tcomp << std::endl;
        fout << "EffectTime.twrite " << effect_time.twrite << std::endl;
        fout << "EffectTime.tpost " << effect_time.tpost << std::endl;
    }
    // override operator =
    NimbleProfile& operator=(const NimbleProfile &p) {
        pre_time.tpre = p.pre_time.tpre;
        pre_time.tread = p.pre_time.tread;
        pre_time.tcomp = p.pre_time.tcomp;
        effect_time.tread1 = p.effect_time.tread1;
        effect_time.tread2 = p.effect_time.tread2;
        effect_time.tcomp = p.effect_time.tcomp;
        effect_time.twrite = p.effect_time.twrite;
        effect_time.tpost = p.effect_time.tpost;
        return *this;
    }
    time_t get_charge_time(int read1_shm=0, int read2_shm=0, int write_shm=0, int qid=0) {
        time_t tot = 0;
        tot += pre_time.tpre + pre_time.tread + pre_time.tcomp + effect_time.tcomp;
        time_t max_io_charge_time = 600;
        if (qid == 1) {  // q1 is small, so we charge less time
            max_io_charge_time = 300;
        }
        if (read1_shm) 
            tot += (effect_time.tread1 > max_io_charge_time ? max_io_charge_time : effect_time.tread1);
        else 
            tot += effect_time.tread1;
        if (read2_shm)
            tot += (effect_time.tread2 > max_io_charge_time ? max_io_charge_time : effect_time.tread2);
        else 
            tot += effect_time.tread2;
        if (write_shm)
            tot += (effect_time.twrite > max_io_charge_time ? max_io_charge_time : effect_time.twrite);
        else 
            tot += effect_time.twrite;
        return tot;
    }
    time_t get_total_time() {
        return pre_time.tpre + pre_time.tread + pre_time.tcomp + effect_time.tread1 + 
            effect_time.tread2 + effect_time.tcomp + effect_time.twrite;
    }
    time_t get_total_effect_time() {
        // not include pre_time and post_time
        return effect_time.tread1 + effect_time.tread2 + 
            effect_time.tcomp + effect_time.twrite;
    }
    void add(NimbleProfile &p) {
        pre_time.tpre += p.pre_time.tpre;
        pre_time.tread += p.pre_time.tread;
        pre_time.tcomp += p.pre_time.tcomp;
        effect_time.tread1 += p.effect_time.tread1;
        effect_time.tread2 += p.effect_time.tread2;
        effect_time.tcomp += p.effect_time.tcomp;
        effect_time.twrite += p.effect_time.twrite;
        effect_time.tpost += p.effect_time.tpost;
    }
    void div(int k) {
        pre_time.tpre /= k;
        pre_time.tread /= k;
        pre_time.tcomp /= k;
        effect_time.tread1 /= k;
        effect_time.tread2 /= k;
        effect_time.tcomp /= k;
        effect_time.twrite /= k;
        effect_time.tpost /= k;
    }
};

bool read_from_profiles(const std::string &filename, NimbleProfile *profiles, int num) {
    std::ifstream fin(filename);
    std::string line;
    int k = 0;
    while (std::getline(fin, line)) {
        if (line.empty()) {
            continue;
        }
        std::istringstream iss(line);
        std::string key;
        std::string value;
        if (!(iss >> key >> value)) {
            std::cerr << "Error in parsing profile\n";
            return false;
        }
        if (key == "PreTime.tpre") {
            profiles[k].pre_time.tpre = std::stol(value);
        } else if (key == "PreTime.tread") {
            profiles[k].pre_time.tread = std::stol(value);
        } else if (key == "PreTime.tcomp") {
            profiles[k].pre_time.tcomp = std::stol(value);
        } else if (key == "EffectTime.tread1") {
            profiles[k].effect_time.tread1 = std::stol(value);
        } else if (key == "EffectTime.tread2") {
            profiles[k].effect_time.tread2 = std::stol(value);
        } else if (key == "EffectTime.tcomp") {
            profiles[k].effect_time.tcomp = std::stol(value);
        } else if (key == "EffectTime.twrite") {
            profiles[k].effect_time.twrite = std::stol(value);
        } else if (key == "EffectTime.tpost") {
            profiles[k].effect_time.tpost = std::stol(value);
            k++;
        } else {
            LOG(INFO) << "Unknown key " << key;
            return false;
        }
    }
    fin.close();
    return true;
}

#endif  /* TASK_HPP */