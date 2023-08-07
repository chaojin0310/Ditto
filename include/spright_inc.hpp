/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin & songyun zou)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#ifndef SPRIGHT_INC_HPP
#define SPRIGHT_INC_HPP

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <rte_branch_prediction.h>
#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_lcore.h>
#include <rte_ring.h>
#include <rte_memzone.h>
#include <rte_mempool.h>
#include <rte_malloc.h>

#define RING_NAME_FORMAT "MYRING_%hhu"
#define RING_NAME_LENGTH_MAX 16
#define RING_LENGTH_MAX (1U << 16)
#define MAX_RING_NUM 60

extern const int num_nodes;
extern int io_pre_init(int argc, char *argv[]);
extern int io_init_primary(struct rte_ring ** &ring);
extern int io_init_secondary(struct rte_ring ** &ring);
extern int io_cleanup();
extern int io_exit_primary(struct rte_ring **ring);
extern int io_exit_secondary(struct rte_ring **ring);
extern int io_rx(struct rte_ring **ring, void **obj, uint8_t node_id);
extern int io_tx(struct rte_ring **ring, void *obj, uint8_t node_id);


#endif /* SPRIGHT_INC_HPP */