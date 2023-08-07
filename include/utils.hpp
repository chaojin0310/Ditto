/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#ifndef UTIL_HPP
#define UTIL_HPP

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sched.h>

#include <iostream>
#include <fstream>
#include <sstream>
#include <fstream>
#include <memory>
#include <string>
#include <vector>
#include <queue>
#include <algorithm>
#include <cmath>
#include <unordered_set>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/sysinfo.h>
#include <thread>
#include <errno.h>

// Used by scheduler
#define MAX_SLOTS 16

int num_cpus = 2;
std::string resource_distri;


// Use us as unit
time_t get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

// Use ms as unit
time_t get_duration(time_t start, time_t end) {
    return (end - start) / 1000;
}

time_t get_us_duration(time_t start, time_t end) {
    return (end - start);
}

int set_cpu_affinity(int cpu_id) {
    /* Remember better to schedule affinity of one physical core instead of one hyper-thread */
    int num_cores = get_nprocs();
    if (cpu_id >= num_cores) {
        std::cout << "cpu_id >= num_cores " << num_cores << std::endl;
        return -1;
    }
    cpu_set_t mask;
    CPU_ZERO(&mask);
    /* Mitigate the effect of hyper-threading contention 
     * by scheduling affinity of one physical core instead of one hyper-thread
     * as possible as we can
     * Logical core 0 is allocated to the shm process
     */
    // if (num_cpus == 2) {
    //     int sep = MAX_SLOTS / 2;
    //     if (cpu_id > sep) {
    //         cpu_id += sep;
    //     }
    // }
    if (cpu_id >= MAX_SLOTS) {
        cpu_id++;
    }
    CPU_SET(cpu_id, &mask);
    /* Optional */
    // int another_cpu_id = cpu_id < num_cpus / 2 ? cpu_id + num_cpus / 2 : cpu_id - num_cpus / 2;
    // CPU_SET(another_cpu_id, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        std::cerr << "Error: could not set CPU affinity, " << strerror(errno) << std::endl;
        return -1;
    }
    return 0;
}

int get_cpu_affinity() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    if (sched_getaffinity(0, sizeof(cpuset), &cpuset) == -1) {
        std::cerr << "Error: could not get CPU affinity, " << strerror(errno) << std::endl;
        return -1;
    }
    int num_cores = get_nprocs();  // get total number of CPUs
    // int num_cpus_conf = get_nprocs_conf();
    for (int i = 0; i < num_cores; ++i) {
        if (CPU_ISSET(i, &cpuset)) {
            std::cout << "CPU " << i << " is set" << std::endl;
            return i;
        }
    }
    return -1;
}

int send_robust(int sockfd, void *buf, size_t len, int flags) {
    size_t ret = 0;
    while (ret < len) {
        ssize_t b = send(sockfd, (char *)buf + ret, len - ret, flags);
        if (b <= 0)
            return -1;
        ret += b;
    }
    return 0;
}

int recv_robust(int sockfd, void *buf, size_t len, int flags) {
    size_t ret = 0;
    while (ret < len){
        ssize_t b = recv(sockfd, (char *)buf + ret, len - ret, flags);
        if (b <= 0)
            return -1;
        ret += b;
    }
    return 0;
}

void trimEnd(std::string& str) {
    size_t lastValidCharIndex = str.find_last_not_of(" \t\n\r");

    if (lastValidCharIndex != std::string::npos) {
        str.erase(lastValidCharIndex + 1);
    }
    else {
        str.clear();
    }
}

#endif /* UTIL_HPP */
