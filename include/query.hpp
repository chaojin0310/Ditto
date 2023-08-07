/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#ifndef QUERY_HPP
#define QUERY_HPP

#include <glog/logging.h>
#include "io.hpp"
#include "spright_inc.hpp"
#include "table.hpp"
#include "task.hpp"
#include "utils.hpp"

char *argv_single[1] = {(char *)""};
char *argv_primary[4] = {(char *)"", 
	(char *)"--proc-type=primary", (char *)"--no-telemetry", (char *)"--no-pci"};
char *argv_secondary[4] = {(char *)"",
	(char *)"--proc-type=secondary", (char *)"--no-telemetry", (char *)"--no-pci"};

extern int execute_q1(TasksToExecute tasks, std::vector<pid_t> &pids);
extern int execute_q16(TasksToExecute tasks, std::vector<pid_t> &pids);
extern int execute_q94(TasksToExecute tasks, std::vector<pid_t> &pids);
extern int execute_q95(TasksToExecute tasks, std::vector<pid_t> &pids);

#endif /* QUERY_HPP */