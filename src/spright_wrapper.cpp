/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin & songyun zou)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#include "spright_inc.hpp"

const int num_nodes = MAX_RING_NUM;	

int io_pre_init(int argc, char *argv[]) {
	int ret = rte_eal_init(argc, argv);
	if (unlikely(ret == -1)) {
		fprintf(stderr, "rte_eal_init() error: %s\n",
				rte_strerror(rte_errno));
		return -1;
	}
	return 0;
}

int io_init_primary(struct rte_ring ** &ring) {
	int ret = 0;
	ring = (struct rte_ring **)malloc(num_nodes * sizeof(struct rte_ring *));
	if (unlikely(ring == NULL)) {
		fprintf(stderr, "malloc() error: %s\n", strerror(errno));
		return -1;
	}

	char ring_name[RING_NAME_LENGTH_MAX];
	int r = 0;
	for (uint8_t i = 0; i < num_nodes; i++) {
		snprintf(ring_name, RING_NAME_LENGTH_MAX, RING_NAME_FORMAT, i);
		ring[i] = rte_ring_create(ring_name, RING_LENGTH_MAX, rte_socket_id(), 0);
		if (unlikely(ring[i] == NULL)) {
			fprintf(stderr, "rte_ring_create() error: %s\n",
			        rte_strerror(rte_errno));
			for (uint8_t j = 0; j < i; j++) {
				rte_ring_free(ring[j]);
			}
			r = -1;
			break;
		}
	}

	if (unlikely(r == -1)) {
		fprintf(stderr, "init_primary() error\n");
		free(ring);
		return -1;
	}
	return 0;
}

int io_init_secondary(struct rte_ring ** &ring) {
	int ret = 0;
	ring = (struct rte_ring**)malloc(num_nodes * sizeof(struct rte_ring *));
	if (unlikely(ring == NULL)) {
		fprintf(stderr, "malloc() error: %s\n", strerror(errno));
		return -1;
	}

	char ring_name[RING_NAME_LENGTH_MAX];
	int r = 0;
	for (uint8_t i = 0; i < num_nodes; i++) {
		snprintf(ring_name, RING_NAME_LENGTH_MAX, RING_NAME_FORMAT, i);
		ring[i] = rte_ring_lookup(ring_name);
		if (unlikely(ring[i] == NULL)) {
			fprintf(stderr, "rte_ring_lookup() error: %s\n",
			        rte_strerror(rte_errno));
			r = -1;
		}
	}
	if (unlikely(r == -1)) {
		fprintf(stderr, "init_secondary() error\n");
		free(ring);
		return -1;
	}
	return 0;
}

int io_cleanup() {
	int ret = rte_eal_cleanup();
	if (unlikely(ret < 0)) {
		fprintf(stderr, "rte_eal_cleanup() error: %s\n",
				rte_strerror(-ret));
        return -1;
	}
	return 0;
}

int io_exit_primary(struct rte_ring **ring) {
	for (uint8_t i = 0; i < num_nodes; i++) {
		rte_ring_free(ring[i]);
	}
	free(ring);
	return io_cleanup();
}

int io_exit_secondary(struct rte_ring **ring) {
	free(ring);
	return io_cleanup();
}

int io_rx(struct rte_ring **ring, void **obj, uint8_t node_id) {
	while (rte_ring_dequeue(ring[node_id], obj) != 0);
	return 0;
}

int io_tx(struct rte_ring **ring, void *obj, uint8_t node_id) {
	while (rte_ring_enqueue(ring[node_id], obj) != 0);
	return 0;
}