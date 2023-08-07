/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#include "query.hpp"


int main(int argc, char *argv[]) {
    if (setuid(0) != 0) {
        std::cout << "setuid failed" << std::endl;
        return 1;
    }
    system("cd ../logs && rm -rf * && cd -");  // rmlogs
    
    std::cout << "Start to run shared memory manager\n";

    set_cpu_affinity(0);
    get_cpu_affinity();

    io_pre_init(4, argv_primary);

    struct rte_ring **ring;
    int ret = io_init_primary(ring);

    int *finish = NULL;
    ret = io_rx(ring, (void **)&finish, 0);

    ret = io_exit_primary(ring);
    return 0;
}