/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#include "query.hpp"

/*==============================Query 1=====================================*/

/*
with customer_total_return as (
  select
    sr_customer_sk as ctr_customer_sk,
    sr_store_sk as ctr_store_sk,
    sum(sr_fee) as ctr_total_return
  from
    store_returns,
    date_dim
  where
    sr_returned_date_sk = d_date_sk
    and d_year = 2002
  group by
    sr_customer_sk,
    sr_store_sk
)
select
  c_customer_id
from
  customer_total_return ctr1,
  store,
  customer
where
  ctr1.ctr_total_return > (
    select
      avg(ctr_total_return) * 1.2
    from
      customer_total_return ctr2
    where
      ctr1.ctr_store_sk = ctr2.ctr_store_sk
  )
  and s_store_sk = ctr1.ctr_store_sk
  and s_state = 'MI'
  and ctr1.ctr_customer_sk = c_customer_sk
order by
  c_customer_id
limit
  100;
 */

void q1_stage1(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table dd(date_dim);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName1, rawDir + "date_dim", &dd, date_dim, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    size_t *idx = new size_t[dd.rows];
    size_t rows_selected = 0;
    std::vector<Selector> selectors;
    selectors.push_back(Selector(d_date, DType::Date, IN_YEAR, date_d(2002, 1, 1)));
    dd.select_to_idx(selectors, idx, rows_selected);
    ShTable *dd_filtered = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    dd_filtered->initialize(int_list);
    std::vector<ColumnDesc> dd_columns;
    dd_columns.push_back(ColumnDesc(d_date_sk, DType::Int64));
    dd.select_with_project(idx, rows_selected, dd_columns, *dd_filtered);
    delete[] idx;
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName1, interDir1 + "q1_s1", dd_filtered, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    dd_filtered->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q1_stage2(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);

    start = get_time();
    Table sr(store_returns);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName1, rawDir + "store_returns", &sr, store_returns, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    ShTable *sr_1 = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    sr_1->initialize(q1_s2);
    std::vector<ColumnDesc> sr_columns;
    sr_columns.push_back(ColumnDesc(sr_customer_sk, DType::Int64));
    sr_columns.push_back(ColumnDesc(sr_store_sk, DType::Int64));
    sr_columns.push_back(ColumnDesc(sr_fee, DType::Float32));
    sr_columns.push_back(ColumnDesc(sr_returned_date_sk, DType::Int64));
    sr.project(sr_columns, *sr_1);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Gather), 
        interBucketName1, interDir1 + "q1_s2", sr_1, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    sr.deleteTable();
    sr_1->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q1_stage3(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table dd_filtered(int_list);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName1, interDir1 + "q1_s1", &dd_filtered, int_list, ring);
    end = get_time();
    profile.pre_time.tread += get_duration(start, end);

    start = get_time();
    Table dd_filter_sorted(int_list);
    ColumnDesc dd_filter_sorted_desc(0, DType::Int64);
    dd_filtered.sort(dd_filter_sorted_desc, dd_filter_sorted);
    dd_filtered.deleteTable();
    end = get_time();
    profile.pre_time.tcomp += get_duration(start, end);

    start = get_time();
    Table sr(q1_s2);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[1], task.meta.curr_stage, TransferOp::Gather), 
        interBucketName1, interDir1 + "q1_s2", &sr, q1_s2, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);
    
    start = get_time();
    Table sr_j(q1_s2);
    Joiner joiner(q1_s2_sr_sr_returned_date_sk, DType::Int64, 0, DType::Int64);
    sr.join_exists_binary(dd_filter_sorted, joiner, sr_j);
    dd_filter_sorted.deleteTable();
    sr.deleteTable();

    ShTable *ctr = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ctr->initialize(q1_s3);
    std::vector<ColumnDesc> ctr_columns;
    ctr_columns.push_back(ColumnDesc(q1_s2_sr_customer_sk, DType::Int64));
    ctr_columns.push_back(ColumnDesc(q1_s2_sr_store_sk, DType::Int64));
    ctr_columns.push_back(ColumnDesc(q1_s2_sr_fee, DType::Float32));
    sr_j.project(ctr_columns, *ctr);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Gather), 
        interBucketName1, interDir1 + "q1_s3", ctr, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    sr_j.deleteTable();
    ctr->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q1_stage4(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ctr(q1_s3);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        interBucketName1, interDir1 + "q1_s3", &ctr, q1_s3, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    size_t *idx = new size_t[ctr.rows];
    std::vector<ColumnDesc> sort_ctr_columns;
    sort_ctr_columns.push_back(ColumnDesc(q1_s3_ctr_customer_sk, DType::Int64));
    sort_ctr_columns.push_back(ColumnDesc(q1_s3_ctr_store_sk, DType::Int64));
    ctr.sort_multi_col_to_idx(sort_ctr_columns, idx);

    ShTable *ctr_group = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ctr_group->initialize(q1_s3);
    GroupByFunc byfunc(q1_s3_ctr_total_return, DType::Float32, AggType::SUM);
    ctr.groupby_sorted(idx, sort_ctr_columns, byfunc, *ctr_group);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName1, interDir1 + "q1_s4", ctr_group, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    delete[] idx;
    ctr.deleteTable();
    ctr_group->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q1_stage5(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ctr_all(q1_s3);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName1, interDir1 + "q1_s4", &ctr_all, q1_s3, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    size_t *idx = new size_t[ctr_all.rows];
    std::vector<ColumnDesc> sort_ctr_columns;
    sort_ctr_columns.push_back(ColumnDesc(q1_s3_ctr_customer_sk, DType::Int64));
    sort_ctr_columns.push_back(ColumnDesc(q1_s3_ctr_store_sk, DType::Int64));
    ctr_all.sort_multi_col_to_idx(sort_ctr_columns, idx);

    ShTable *ctr_group = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ctr_group->initialize(q1_s3);
    GroupByFunc byfunc(q1_s3_ctr_total_return, DType::Float32, AggType::SUM);
    ctr_all.groupby_sorted(idx, sort_ctr_columns, byfunc, *ctr_group);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Broadcast), 
        interBucketName1, interDir1 + "q1_s5", ctr_group, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    delete[] idx;
    ctr_all.deleteTable();
    ctr_group->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q1_stage6(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ctm(customer_table);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName1, rawDir + "customer", &ctm, customer_table, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    ShTable *ct = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ct->initialize(q1_s6);
    std::vector<ColumnDesc> ct_columns;
    ct_columns.push_back(ColumnDesc(c_customer_sk, DType::Int64));
    ct_columns.push_back(ColumnDesc(c_customer_id, DType::String));
    ctm.project(ct_columns, *ct);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName1, interDir1 + "q1_s6", ct, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ctm.deleteTable();
    ct->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q1_stage7(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);

    start = get_time();
    Table ct(q1_s6);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[1], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName1, interDir1 + "q1_s6", &ct, q1_s6, ring);
    end = get_time();
    profile.pre_time.tread += get_duration(start, end);
    
    start = get_time();
    Table ctr(q1_s3);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Broadcast), 
        interBucketName1, interDir1 + "q1_s5", &ctr, q1_s3, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    ShTable *ct_ctr = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ct_ctr->initialize(q1_s7);
    // left is customer, right is ctr
    std::vector<ColumnDesc> left_columns;
    left_columns.push_back(ColumnDesc(c_customer_id, DType::String));
    std::vector<ColumnDesc> right_columns;
    right_columns.push_back(ColumnDesc(q1_s3_ctr_customer_sk, DType::Int64));
    right_columns.push_back(ColumnDesc(q1_s3_ctr_store_sk, DType::Int64));
    right_columns.push_back(ColumnDesc(q1_s3_ctr_total_return, DType::Float32));
    Joiner joiner(c_customer_sk, DType::Int64, q1_s3_ctr_customer_sk, DType::Int64);
    ct.join_binary_concat(ctr, joiner, left_columns, right_columns, *ct_ctr);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Gather), 
        interBucketName1, interDir1 + "q1_s7", ct_ctr, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ct.deleteTable();
    ctr.deleteTable();
    ct_ctr->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q1_stage8(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table s(store_table);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName1, rawDir + "store", &s, store_table, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    size_t *idx = new size_t[s.rows];
    size_t rows_selected = 0;
    std::vector<Selector> selectors;
    selectors.push_back(Selector(s_state, DType::String, EQUAL, "MI"));
    s.select_to_idx(selectors, idx, rows_selected);

    ShTable *st = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    st->initialize(int_list);
    std::vector<ColumnDesc> st_columns;
    st_columns.push_back(ColumnDesc(s_store_sk, DType::Int64));
    s.select_with_project(idx, rows_selected, st_columns, *st);
    delete[] idx;
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName1, interDir1 + "q1_s8", st, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    s.deleteTable();
    st->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q1_stage9(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table st(int_list);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[1], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName1, interDir1 + "q1_s8", &st, int_list, ring);
    end = get_time();
    profile.pre_time.tread += get_duration(start, end);

    start = get_time();
    Table st_sorted(int_list);
    ColumnDesc sort_col(0, DType::Int64);
    st.sort(sort_col, st_sorted);
    st.deleteTable();
    end = get_time();
    profile.pre_time.tcomp += get_duration(start, end);

    start = get_time();
    Table ctr(q1_s7);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        interBucketName1, interDir1 + "q1_s7", &ctr, q1_s7, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    ShTable *st_ctr = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    st_ctr->initialize(q1_s7);
    Joiner joiner(q1_s7_ctr_store_sk, DType::Int64, 0, DType::Int64);
    ctr.join_exists_binary(st_sorted, joiner, *st_ctr);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);
    
    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName1, interDir1 + "q1_s9", st_ctr, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ctr.deleteTable();
    st_sorted.deleteTable();
    st_ctr->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q1_stage10(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ctr(q1_s7);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName1, interDir1 + "q1_s9", &ctr, q1_s7, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    size_t *idx = new size_t[ctr.rows];
    ColumnDesc sort_col(q1_s7_ctr_store_sk, DType::Int64);
    ctr.sort_to_idx(sort_col, idx);

    Table r_avg(float_list);
    std::vector<ColumnDesc> groupby_columns;
    groupby_columns.push_back(ColumnDesc(q1_s7_ctr_store_sk, DType::Int64));
    GroupByFunc byfunc(q1_s7_ctr_total_return, DType::Float32, AggType::AVG);
    ctr.groupby_sorted_append(idx, groupby_columns, byfunc, r_avg);
    delete[] idx;
    r_avg.multiply(1.2);
    
    size_t *idx2 = new size_t[r_avg.rows];
    size_t rows_selected = 0;
    ColumnDesc cmp_col(q1_s7_ctr_total_return, DType::Float32);
    ctr.compare_to_idx(cmp_col, r_avg, GREATER, idx2, rows_selected);
    r_avg.deleteTable();
    
    Table cus_id(q1_s10);
    std::vector<ColumnDesc> cus_id_columns;
    cus_id_columns.push_back(ColumnDesc(q1_s7_c_customer_id, DType::String));
    ctr.select_with_project(idx2, rows_selected, cus_id_columns, cus_id);
    delete[] idx2;
    ctr.deleteTable();

    size_t *idx3 = new size_t[cus_id.rows];
    ColumnDesc sort_col2(q1_s10_c_customer_id, DType::String);
    cus_id.sort_to_idx(sort_col2, idx3);

    ShTable *final_res = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    final_res->initialize(q1_s10);
    ColumnDesc drop_col(q1_s10_c_customer_id, DType::String);
    cus_id.drop_dup_sorted(idx3, drop_col, *final_res);
    delete[] idx3;
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName1, interDir1 + "q1_s10", final_res, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    cus_id.deleteTable();
    final_res->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

int execute_q1(TasksToExecute tasks, std::vector<pid_t> &pids) {
    void (*stage)(Task, NimbleProfile &) = nullptr;
    switch (tasks.curr_stage.stage_id) {
        case 1:
            stage = q1_stage1;
            break;
        case 2:
            stage = q1_stage2;
            break;
        case 3:
            stage = q1_stage3;
            break;
        case 4:
            stage = q1_stage4;
            break;
        case 5:
            stage = q1_stage5;
            break;
        case 6:
            stage = q1_stage6;
            break;
        case 7:
            stage = q1_stage7;
            break;
        case 8:
            stage = q1_stage8;
            break;
        case 9:
            stage = q1_stage9;
            break;
        case 10:
            stage = q1_stage10;
            break;
        default:
            LOG(ERROR) << "Invalid stage id " << tasks.curr_stage.stage_id;
            return -1;
    }
    for (int i = 0; i < tasks.curr_stage.num_local_tasks; i++) {
        int task_id = tasks.curr_stage.task_id_start + i;
        int ring_id = tasks.curr_stage.ring_id_start + i;
        pid_t pid = fork();
        if (pid == 0) {
            set_cpu_affinity(ring_id);
            get_cpu_affinity();

            NimbleProfile profile;
            (*stage)(Task(task_id, ring_id, tasks), profile);
            
            profile.print_to_file("../logs/q1_stage" + 
                std::to_string(tasks.curr_stage.stage_id) + "_parall" + 
                std::to_string(tasks.curr_stage.num_tasks) + "_task" + 
                std::to_string(task_id) + ".log");
            
            exit(0);
        }
        pids.push_back(pid);
    }
    return 0;
}

/*===========================================================================*/


/*==============================Query 16=====================================*/

/*
select
  count(distinct cs_order_number) as "order count",
  sum(cs_ext_ship_cost) as "total shipping cost",
  sum(cs_net_profit) as "total net profit"
from
  catalog_sales cs1,
  date_dim,
  customer_address,
  call_center
where
  d_date between '2002-2-01'
  and (cast('2002-2-01' as date) + 60 days)
  and cs1.cs_ship_date_sk = d_date_sk
  and cs1.cs_ship_addr_sk = ca_address_sk
  and ca_state = 'NY'
  and cs1.cs_call_center_sk = cc_call_center_sk
  and cc_county in (
    'Ziebach County',
    'Levy County',
    'Huron County',
    'Franklin Parish',
    'Daviess County'
  )
  and exists (
    select
      *
    from
      catalog_sales cs2
    where
      cs1.cs_order_number = cs2.cs_order_number
      and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk
  )
  and not exists(
    select
      *
    from
      catalog_returns cr1
    where
      cs1.cs_order_number = cr1.cr_order_number
  )
order by
  count(distinct cs_order_number)
limit
  100;
 */

void q16_stage1(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table cs(catalog_sales);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName16, rawDir + "catalog_sales", &cs, catalog_sales, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);
    
    start = get_time();
    ShTable *cs_1 = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    cs_1->initialize(q16_s1);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(cs_order_number, DType::Int64));
    columns.push_back(ColumnDesc(cs_warehouse_sk, DType::Int64));
    cs.project(columns, *cs_1);
    cs.deleteTable();
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Gather), 
        interBucketName16, interDir16 + "q16_s1", cs_1, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    cs_1->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q16_stage2(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table cs_1(q16_s1);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        interBucketName16, interDir16 + "q16_s1", &cs_1, q16_s1, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    size_t *idx = new size_t[cs_1.rows];
    ColumnDesc col(q16_s1_cs_order_number, DType::Int64);
    cs_1.sort_to_idx(col, idx);

    ShTable *cs_wh = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    cs_wh->initialize(q16_s2);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(q16_s1_cs_order_number, DType::Int64));
    GroupByFunc byfunc(q16_s1_cs_warehouse_sk, DType::Int64, AggType::COUNT_DISTINCT_MAP);
    cs_1.groupby_sorted(idx, columns, byfunc, *cs_wh);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);
    
    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName16, interDir16 + "q16_s2", cs_wh, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    delete idx;
    cs_1.deleteTable();
    cs_wh->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q16_stage3(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table cs_wh(q16_s2);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName16, interDir16 + "q16_s2", &cs_wh, q16_s2, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    size_t *idx = new size_t[cs_wh.rows];
    ColumnDesc col(q16_s2_cs_order_number, DType::Int64);
    cs_wh.sort_to_idx(col, idx);

    Table cs_wh_r(q16_s3);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(q16_s2_cs_order_number, DType::Int64));
    GroupByFunc byfunc(q16_s2_cs_warehouse_sk_count, DType::Int64, AggType::COUNT_DISTINCT_REDUCE);
    cs_wh.groupby_sorted(idx, columns, byfunc, cs_wh_r);
    delete[] idx;
    cs_wh.deleteTable();

    std::vector<Selector> selectors;
	selectors.push_back(Selector(q16_s3_cs_warehouse_sk_count, DType::Int64, GREATER, 1));
    size_t *idx_selected = new size_t[cs_wh_r.rows];
    size_t rows_selected = 0;
    cs_wh_r.select_to_idx(selectors, idx_selected, rows_selected);

    ShTable *cs_wh_order_num = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    cs_wh_order_num->initialize(int_list);
    std::vector<ColumnDesc> target_cols;
    target_cols.push_back(ColumnDesc(q16_s3_cs_order_number, DType::Int64));
    cs_wh_r.select_with_project(idx_selected, rows_selected, target_cols, *cs_wh_order_num);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName16, interDir16 + "q16_s3", cs_wh_order_num, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    delete[] idx_selected;
    cs_wh_r.deleteTable();
    cs_wh_order_num->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q16_stage4(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table cs(catalog_sales);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName16, rawDir + "catalog_sales", &cs, web_sales, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);
    
    start = get_time();
    ShTable *cs_j = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    cs_j->initialize(q16_s4);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(cs_order_number, DType::Int64));
    columns.push_back(ColumnDesc(cs_ext_ship_cost, DType::Float32));
    columns.push_back(ColumnDesc(cs_net_profit, DType::Float32));
    columns.push_back(ColumnDesc(cs_ship_date_sk, DType::Int64));
    columns.push_back(ColumnDesc(cs_ship_addr_sk, DType::Int64));
    columns.push_back(ColumnDesc(cs_call_center_sk, DType::Int64));
    columns.push_back(ColumnDesc(cs_warehouse_sk, DType::Int64));
    cs.project(columns, *cs_j);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Gather), 
        interBucketName16, interDir16 + "q16_s4", cs_j, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    cs.deleteTable();
    cs_j->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q16_stage5(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table cr(catalog_returns);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName16, rawDir + "catalog_returns", &cr, catalog_returns, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    ShTable *cr_order_num = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    cr_order_num->initialize(int_list);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(cr_order_number, DType::Int64));
    cr.project(columns, *cr_order_num);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName16, interDir16 + "q16_s5", cr_order_num, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    cr.deleteTable();
    cr_order_num->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q16_stage6(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);

    // Read date_dim
    Table dd(date_dim);
    GetExternalTable(rawBucketName16, "date_dim.dat", dd);
    size_t *idx = new size_t[dd.rows];
    size_t rows_selected = 0;
    std::vector<Selector> selectors;
    selectors.push_back(Selector(d_date, DType::Date, GREATER, date_d(2002, 2, 1)));
    selectors.push_back(Selector(d_date, DType::Date, LESS, date_d(2002, 4, 1)));
    dd.select_to_idx(selectors, idx, rows_selected);
    Table dd_filtered(int_list);
    std::vector<ColumnDesc> dd_columns;
    dd_columns.push_back(ColumnDesc(d_date_sk, DType::Int64));
    dd.select_with_project(idx, rows_selected, dd_columns, dd_filtered);
    delete[] idx;
    dd.deleteTable();
    // Sort date_sk
    Table dd_filter_sorted(int_list);
    ColumnDesc dd_filter_sorted_desc(0, DType::Int64);
    dd_filtered.sort(dd_filter_sorted_desc, dd_filter_sorted);
    dd_filtered.deleteTable();
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);

    // Read s5 intermediate
    start = get_time();
    Table cr_order_num(int_list);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[2], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName16, interDir16 + "q16_s5", &cr_order_num, int_list, ring);
    end = get_time();
    profile.pre_time.tread += get_duration(start, end);
    // Sort wr_order_num
    start = get_time();
    Table cr_order_num_sorted(int_list);
    ColumnDesc cr_order_num_sorted_desc(0, DType::Int64);
    cr_order_num.sort(cr_order_num_sorted_desc, cr_order_num_sorted);
    cr_order_num.deleteTable();
    end = get_time();
    profile.pre_time.tcomp += get_duration(start, end);

    // Read s3 intermediate
    start = get_time();
    Table cs_wh_order_num(int_list);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName16, interDir16 + "q16_s3", &cs_wh_order_num, int_list, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);
    
    // Read s4 intermediate
    start = get_time();
    Table cs_j(q16_s4);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[1], task.meta.curr_stage, TransferOp::Gather), 
        interBucketName16, interDir16 + "q16_s4", &cs_j, q16_s4, ring);
    end = get_time();
    profile.effect_time.tread2 += get_duration(start, end);

    start = get_time();
    Table target_order_num(int_list);
    cs_wh_order_num.difference_sorted(cr_order_num_sorted, target_order_num);
    cs_wh_order_num.deleteTable();
    cr_order_num_sorted.deleteTable();

    ColumnDesc col_desc2(q16_s4_cs_order_number, DType::Int64);
    size_t *idx2 = new size_t[cs_j.rows];
    size_t rows_selected2 = 0;
    cs_j.exists_binary_search_idx(col_desc2, target_order_num, idx2, rows_selected2);
    target_order_num.deleteTable();

    Joiner joiner(q16_s4_cs_ship_date_sk, DType::Int64, 0, DType::Int64);
    ShTable *merged = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    merged->initialize(q16_s4);
    cs_j.join_idx_exists_binary(dd_filter_sorted, joiner, idx2, rows_selected2, *merged);
    delete[] idx2;
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Gather), 
        interBucketName16, interDir16 + "q16_s6", merged, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    dd_filter_sorted.deleteTable();
    cs_j.deleteTable();
    merged->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q16_stage7(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ca(customer_address);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName16, rawDir + "customer_address", &ca, customer_address, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    Table ca_p(q16_s7);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(ca_address_sk, DType::Int64));
	columns.push_back(ColumnDesc(ca_state, DType::String));
	ca.project(columns, ca_p);
    ca.deleteTable();

	Table ca_s(q16_s7);
	std::vector<Selector> selectors;
	selectors.push_back(Selector(q16_s7_ca_state, DType::String, EQUAL, "NY"));
	ca_p.select(selectors, ca_s);
    ca_p.deleteTable();

    ShTable *ca_sk = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ca_sk->initialize(int_list);
    std::vector<ColumnDesc> columns2;
    columns2.push_back(ColumnDesc(q16_s7_ca_address_sk, DType::Int64));
    ca_s.project(columns2, *ca_sk);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName16, interDir16 + "q16_s7", ca_sk, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ca_s.deleteTable();
    ca_sk->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q16_stage8(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);

    Table callctr(call_center);
    GetExternalTable(rawBucketName16, "call_center.dat", callctr);
    Table cc(q16_s8_cc);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(cc_call_center_sk, DType::Int64));
    columns.push_back(ColumnDesc(cc_county, DType::String));
    callctr.project(columns, cc);
    callctr.deleteTable();
    Table cc_select(q16_s8_cc);
    std::vector<Selector> selectors;
    selectors.push_back(Selector(q16_s8_cc_cc_county, DType::String, IN, "Ziebach County"));
    selectors.push_back(Selector(q16_s8_cc_cc_county, DType::String, IN, "Levy County"));
    selectors.push_back(Selector(q16_s8_cc_cc_county, DType::String, IN, "Huron County"));
    selectors.push_back(Selector(q16_s8_cc_cc_county, DType::String, IN, "Franklin Parish"));
    selectors.push_back(Selector(q16_s8_cc_cc_county, DType::String, IN, "Daviess County"));
    cc.select_in(selectors, cc_select);
    cc.deleteTable();
    Table cc_sk(int_list);
    std::vector<ColumnDesc> columns2;
    columns2.push_back(ColumnDesc(q16_s8_cc_cc_call_center_sk, DType::Int64));
    cc_select.project(columns2, cc_sk);
    cc_select.deleteTable();
    Table cc_sk_sorted(int_list);
    ColumnDesc cc_sk_sorted_col(0, DType::Int64);
    cc_sk.sort(cc_sk_sorted_col, cc_sk_sorted);
    cc_sk.deleteTable();
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ca_sk(int_list);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[1], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName16, interDir16 + "q16_s7", &ca_sk, int_list, ring);
    end = get_time();
    profile.pre_time.tread += get_duration(start, end);
    start = get_time();
    Table ca_sk_sorted(int_list);
    ColumnDesc ca_sorted_col(0, DType::Int64);
    ca_sk.sort(ca_sorted_col, ca_sk_sorted);
    ca_sk.deleteTable();
    end = get_time();
    profile.pre_time.tcomp += get_duration(start, end);

    start = get_time();
    Table ws(q16_s4);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        interBucketName16, interDir16 + "q16_s6", &ws, q16_s4, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);
    
    start = get_time();
    Table merged(q16_s4);
    Joiner joiner(q16_s4_cs_ship_addr_sk, DType::Int64, 0, DType::Int64);
    ws.join_exists_binary(ca_sk_sorted, joiner, merged);
    ws.deleteTable();
    ca_sk_sorted.deleteTable();

    Table merged2(q16_s4);
    Joiner joiner2(q16_s4_cs_call_center_sk, DType::Int64, 0, DType::Int64);
    merged.join_exists_binary(cc_sk_sorted, joiner2, merged2);
    merged.deleteTable();
    cc_sk_sorted.deleteTable();

    ShTable *r = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0); 
    r->initialize(q16_s8);
    std::vector<ColumnDesc> columns3;
    columns3.push_back(ColumnDesc(q16_s4_cs_order_number, DType::Int64));
    columns3.push_back(ColumnDesc(q16_s4_cs_ext_ship_cost, DType::Float32));
    columns3.push_back(ColumnDesc(q16_s4_cs_net_profit, DType::Float32));
    merged2.project(columns3, *r);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName16, interDir16 + "q16_s8", r, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    merged2.deleteTable();
    r->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q16_stage9(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ws(q16_s8);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName16, interDir16 + "q16_s8", &ws, q16_s8, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    ShTable *final_res = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    final_res->initialize(q16_s8);
    ColumnDesc col1(q16_s8_cs_order_number, DType::Int64);
    size_t distinct_number = ws.count_distinct(col1);
    ColumnDesc col2(q16_s8_cs_ext_ship_cost, DType::Float32);
    double cost_sum = ws.sum(col2);
    ColumnDesc col3(q16_s8_cs_net_profit, DType::Float32);
    double profit_sum = ws.sum(col3);
    final_res->rows = 1;
    final_res->int_data[0].push_back(distinct_number);
    final_res->float_data[0].push_back(cost_sum);
    final_res->float_data[1].push_back(profit_sum);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName16, interDir16 + "q16_s9", final_res, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ws.deleteTable();
    final_res->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

int execute_q16(TasksToExecute tasks, std::vector<pid_t> &pids) {
    void (*stage)(Task, NimbleProfile &) = nullptr;
    switch (tasks.curr_stage.stage_id) {
        case 1:
            stage = q16_stage1;
            break;
        case 2:
            stage = q16_stage2;
            break;
        case 3:
            stage = q16_stage3;
            break;
        case 4:
            stage = q16_stage4;
            break;
        case 5:
            stage = q16_stage5;
            break;
        case 6:
            stage = q16_stage6;
            break;
        case 7:
            stage = q16_stage7;
            break;
        case 8:
            stage = q16_stage8;
            break;
        case 9:
            stage = q16_stage9;
            break;
        default:
            LOG(ERROR) << "Invalid stage id " << tasks.curr_stage.stage_id;
            return -1;
    }
    for (int i = 0; i < tasks.curr_stage.num_local_tasks; i++) {
        int task_id = tasks.curr_stage.task_id_start + i;
        int ring_id = tasks.curr_stage.ring_id_start + i;
        pid_t pid = fork();
        if (pid == 0) {
            set_cpu_affinity(ring_id);
            get_cpu_affinity();

            NimbleProfile profile;
            (*stage)(Task(task_id, ring_id, tasks), profile);
            
            profile.print_to_file("../logs/q16_stage" + 
                std::to_string(tasks.curr_stage.stage_id) + "_parall" + 
                std::to_string(tasks.curr_stage.num_tasks) + "_task" + 
                std::to_string(task_id) + ".log");
            
            exit(0);
        }
        pids.push_back(pid);
    }
    return 0;
}

/*===========================================================================*/


/*==============================Query 94=====================================*/

/*
select
  count(distinct ws_order_number) as "order count",
  sum(ws_ext_ship_cost) as "total shipping cost",
  sum(ws_net_profit) as "total net profit"
from
  web_sales ws1,
  date_dim,
  customer_address,
  web_site
where
  d_date between '1999-2-01'
  and (cast('1999-2-01' as date) + 60 days)
  and ws1.ws_ship_date_sk = d_date_sk
  and ws1.ws_ship_addr_sk = ca_address_sk
  and ca_state = 'MD'
  and ws1.ws_web_site_sk = web_site_sk
  and web_company_name = 'pri'
  and exists (
    select
      *
    from
      web_sales ws2
    where
      ws1.ws_order_number = ws2.ws_order_number
      and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
  )
  and not exists(
    select
      *
    from
      web_returns wr1
    where
      ws1.ws_order_number = wr1.wr_order_number
  )
order by
  count(distinct ws_order_number)
limit
  100;
 */

void q94_stage1(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ws(web_sales);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName94, rawDir + "web_sales", &ws, web_sales, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);
    
    start = get_time();
    ShTable *ws_1 = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ws_1->initialize(q94_s1);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(ws_order_number, DType::Int64));
    columns.push_back(ColumnDesc(ws_warehouse_sk, DType::Int64));
    ws.project(columns, *ws_1);
    ws.deleteTable();
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Gather), 
        interBucketName94, interDir94 + "q94_s1", ws_1, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ws_1->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q94_stage2(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ws_1(q94_s1);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        interBucketName94, interDir94 + "q94_s1", &ws_1, q94_s1, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    size_t *idx = new size_t[ws_1.rows];
    ColumnDesc col(q94_s1_ws_order_number, DType::Int64);
    ws_1.sort_to_idx(col, idx);

    ShTable *ws_wh = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ws_wh->initialize(q94_s2);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(q94_s1_ws_order_number, DType::Int64));
    GroupByFunc byfunc(q94_s1_ws_warehouse_sk, DType::Int64, AggType::COUNT_DISTINCT_MAP);
    ws_1.groupby_sorted(idx, columns, byfunc, *ws_wh);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);
    
    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName94, interDir94 + "q94_s2", ws_wh, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    delete idx;
    ws_1.deleteTable();
    ws_wh->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q94_stage3(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ws_wh(q94_s2);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName94, interDir94 + "q94_s2", &ws_wh, q94_s2, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    size_t *idx = new size_t[ws_wh.rows];
    ColumnDesc col(q94_s2_ws_order_number, DType::Int64);
    ws_wh.sort_to_idx(col, idx);

    Table ws_wh_r(q94_s3);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(q94_s2_ws_order_number, DType::Int64));
    GroupByFunc byfunc(q94_s2_ws_warehouse_sk_count, DType::Int64, AggType::COUNT_DISTINCT_REDUCE);
    ws_wh.groupby_sorted(idx, columns, byfunc, ws_wh_r);
    delete[] idx;
    ws_wh.deleteTable();

    std::vector<Selector> selectors;
	selectors.push_back(Selector(q94_s3_ws_warehouse_sk_count, DType::Int64, GREATER, 1));
    size_t *idx_selected = new size_t[ws_wh_r.rows];
    size_t rows_selected = 0;
    ws_wh_r.select_to_idx(selectors, idx_selected, rows_selected);

    ShTable *ws_wh_order_num = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ws_wh_order_num->initialize(int_list);
    std::vector<ColumnDesc> target_cols;
    target_cols.push_back(ColumnDesc(q94_s3_ws_order_number, DType::Int64));
    ws_wh_r.select_with_project(idx_selected, rows_selected, target_cols, *ws_wh_order_num);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName94, interDir94 + "q94_s3", ws_wh_order_num, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    delete[] idx_selected;
    ws_wh_r.deleteTable();
    ws_wh_order_num->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q94_stage4(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ws(web_sales);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName94, rawDir + "web_sales", &ws, web_sales, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);
    
    start = get_time();
    ShTable *ws_j = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ws_j->initialize(q94_s4);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(ws_order_number, DType::Int64));
    columns.push_back(ColumnDesc(ws_ext_ship_cost, DType::Float32));
    columns.push_back(ColumnDesc(ws_net_profit, DType::Float32));
    columns.push_back(ColumnDesc(ws_ship_date_sk, DType::Int64));
    columns.push_back(ColumnDesc(ws_ship_addr_sk, DType::Int64));
    columns.push_back(ColumnDesc(ws_web_site_sk, DType::Int64));
    columns.push_back(ColumnDesc(ws_warehouse_sk, DType::Int64));
    ws.project(columns, *ws_j);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Gather), 
        interBucketName94, interDir94 + "q94_s4", ws_j, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ws.deleteTable();
    ws_j->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q94_stage5(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table wr(web_returns);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName94, rawDir + "web_returns", &wr, web_returns, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    ShTable *wr_order_num = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    wr_order_num->initialize(int_list);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(wr_order_number, DType::Int64));
    wr.project(columns, *wr_order_num);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName94, interDir94 + "q94_s5", wr_order_num, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    wr.deleteTable();
    wr_order_num->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q94_stage6(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);

    // Read date_dim
    Table dd(date_dim);
    GetExternalTable(rawBucketName94, "date_dim.dat", dd);
    size_t *idx = new size_t[dd.rows];
    size_t rows_selected = 0;
    std::vector<Selector> selectors;
    selectors.push_back(Selector(d_date, DType::Date, GREATER, date_d(1999, 2, 1)));
    selectors.push_back(Selector(d_date, DType::Date, LESS, date_d(1999, 4, 1)));
    dd.select_to_idx(selectors, idx, rows_selected);
    Table dd_filtered(int_list);
    std::vector<ColumnDesc> dd_columns;
    dd_columns.push_back(ColumnDesc(d_date_sk, DType::Int64));
    dd.select_with_project(idx, rows_selected, dd_columns, dd_filtered);
    delete[] idx;
    dd.deleteTable();
    // Sort date_sk
    Table dd_filter_sorted(int_list);
    ColumnDesc dd_filter_sorted_desc(0, DType::Int64);
    dd_filtered.sort(dd_filter_sorted_desc, dd_filter_sorted);
    dd_filtered.deleteTable();
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);

    // Read s5 intermediate
    start = get_time();
    Table wr_order_num(int_list);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[2], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName94, interDir94 + "q94_s5", &wr_order_num, int_list, ring);
    end = get_time();
    profile.pre_time.tread += get_duration(start, end);
    // Sort wr_order_num
    start = get_time();
    Table wr_order_num_sorted(int_list);
    ColumnDesc wr_order_num_sorted_desc(0, DType::Int64);
    wr_order_num.sort(wr_order_num_sorted_desc, wr_order_num_sorted);
    wr_order_num.deleteTable();
    end = get_time();
    profile.pre_time.tcomp += get_duration(start, end);

    // Read s3 intermediate
    start = get_time();
    Table ws_wh_order_num(int_list);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName94, interDir94 + "q94_s3", &ws_wh_order_num, int_list, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);
    // Read s4 intermediate
    start = get_time();
    Table ws_j(q94_s4);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[1], task.meta.curr_stage, TransferOp::Gather), 
        interBucketName94, interDir94 + "q94_s4", &ws_j, q94_s4, ring);
    end = get_time();
    profile.effect_time.tread2 += get_duration(start, end);

    start = get_time();
    Table target_order_num(int_list);
    ws_wh_order_num.difference_sorted(wr_order_num_sorted, target_order_num);
    ws_wh_order_num.deleteTable();
    wr_order_num_sorted.deleteTable();

    ColumnDesc col_desc2(q94_s4_ws_order_number, DType::Int64);
    size_t *idx2 = new size_t[ws_j.rows];
    size_t rows_selected2 = 0;
    ws_j.exists_binary_search_idx(col_desc2, target_order_num, idx2, rows_selected2);
    target_order_num.deleteTable();

    Joiner joiner(q94_s4_ws_ship_date_sk, DType::Int64, 0, DType::Int64);
    ShTable *merged = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    merged->initialize(q94_s4);
    ws_j.join_idx_exists_binary(dd_filter_sorted, joiner, idx2, rows_selected2, *merged);
    delete[] idx2;
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Gather), 
        interBucketName94, interDir94 + "q94_s6", merged, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    dd_filter_sorted.deleteTable();
    ws_j.deleteTable();
    merged->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q94_stage7(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ca(customer_address);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName94, rawDir + "customer_address", &ca, customer_address, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    Table ca_p(q94_s7);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(ca_address_sk, DType::Int64));
	columns.push_back(ColumnDesc(ca_state, DType::String));
	ca.project(columns, ca_p);
    ca.deleteTable();

	Table ca_s(q94_s7);
	std::vector<Selector> selectors;
	selectors.push_back(Selector(q94_s7_ca_state, DType::String, EQUAL, "MD"));
	ca_p.select(selectors, ca_s);
    ca_p.deleteTable();

    ShTable *ca_sk = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ca_sk->initialize(int_list);
    std::vector<ColumnDesc> columns2;
    columns2.push_back(ColumnDesc(q94_s7_ca_address_sk, DType::Int64));
    ca_s.project(columns2, *ca_sk);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName94, interDir94 + "q94_s7", ca_sk, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ca_s.deleteTable();
    ca_sk->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q94_stage8(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);

    Table wbst(web_site);
    GetExternalTable(rawBucketName94, "web_site.dat", wbst);
    Table web(q94_s8_web);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(web_site_sk, DType::Int64));
    columns.push_back(ColumnDesc(web_company_name, DType::String));
    wbst.project(columns, web);
    wbst.deleteTable();
    Table web_select(q94_s8_web);
    std::vector<Selector> selectors;
    selectors.push_back(Selector(q94_s8_web_web_company_name, DType::String, EQUAL, "pri"));
    web.select(selectors, web_select);
    web.deleteTable();
    Table web_sk(int_list);
    std::vector<ColumnDesc> columns2;
    columns2.push_back(ColumnDesc(q94_s8_web_web_site_sk, DType::Int64));
    web_select.project(columns2, web_sk);
    web_select.deleteTable();
    Table web_sk_sorted(int_list);
    ColumnDesc web_sk_sorted_col(0, DType::Int64);
    web_sk.sort(web_sk_sorted_col, web_sk_sorted);
    web_sk.deleteTable();
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ca_sk(int_list);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[1], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName94, interDir94 + "q94_s7", &ca_sk, int_list, ring);
    end = get_time();
    profile.pre_time.tread += get_duration(start, end);
    start = get_time();
    Table ca_sk_sorted(int_list);
    ColumnDesc ca_sorted_col(0, DType::Int64);
    ca_sk.sort(ca_sorted_col, ca_sk_sorted);
    ca_sk.deleteTable();
    end = get_time();
    profile.pre_time.tcomp += get_duration(start, end);

    start = get_time();
    Table ws(q94_s4);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        interBucketName94, interDir94 + "q94_s6", &ws, q94_s4, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();    
    Table merged(q94_s4);
    Joiner joiner(q94_s4_ws_ship_addr_sk, DType::Int64, 0, DType::Int64);
    ws.join_exists_binary(ca_sk_sorted, joiner, merged);
    ws.deleteTable();
    ca_sk_sorted.deleteTable();

    Table merged2(q94_s4);
    Joiner joiner2(q94_s4_ws_web_site_sk, DType::Int64, 0, DType::Int64);
    merged.join_exists_binary(web_sk_sorted, joiner2, merged2);
    merged.deleteTable();
    web_sk_sorted.deleteTable();

    ShTable *r = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0); 
    r->initialize(q94_s8);
    std::vector<ColumnDesc> columns3;
    columns3.push_back(ColumnDesc(q94_s4_ws_order_number, DType::Int64));
    columns3.push_back(ColumnDesc(q94_s4_ws_ext_ship_cost, DType::Float32));
    columns3.push_back(ColumnDesc(q94_s4_ws_net_profit, DType::Float32));
    merged2.project(columns3, *r);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName94, interDir94 + "q94_s8", r, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    merged2.deleteTable();
    r->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q94_stage9(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ws(q94_s8);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName94, interDir94 + "q94_s8", &ws, q94_s8, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    ShTable *final_res = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    final_res->initialize(q94_s8);
    ColumnDesc col1(q94_s8_ws_order_number, DType::Int64);
    size_t distinct_number = ws.count_distinct(col1);
    ColumnDesc col2(q94_s8_ws_ext_ship_cost, DType::Float32);
    double cost_sum = ws.sum(col2);
    ColumnDesc col3(q94_s8_ws_net_profit, DType::Float32);
    double profit_sum = ws.sum(col3);
    final_res->rows = 1;
    final_res->int_data[0].push_back(distinct_number);
    final_res->float_data[0].push_back(cost_sum);
    final_res->float_data[1].push_back(profit_sum);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName94, interDir94 + "q94_s9", final_res, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ws.deleteTable();
    final_res->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

int execute_q94(TasksToExecute tasks, std::vector<pid_t> &pids) {
    void (*stage)(Task, NimbleProfile &) = nullptr;
    switch (tasks.curr_stage.stage_id) {
        case 1:
            stage = q94_stage1;
            break;
        case 2:
            stage = q94_stage2;
            break;
        case 3:
            stage = q94_stage3;
            break;
        case 4:
            stage = q94_stage4;
            break;
        case 5:
            stage = q94_stage5;
            break;
        case 6:
            stage = q94_stage6;
            break;
        case 7:
            stage = q94_stage7;
            break;
        case 8:
            stage = q94_stage8;
            break;
        case 9:
            stage = q94_stage9;
            break;
        default:
            LOG(ERROR) << "Invalid stage id " << tasks.curr_stage.stage_id;
            return -1;
    }
    for (int i = 0; i < tasks.curr_stage.num_local_tasks; i++) {
        int task_id = tasks.curr_stage.task_id_start + i;
        int ring_id = tasks.curr_stage.ring_id_start + i;
        pid_t pid = fork();
        if (pid == 0) {
            set_cpu_affinity(ring_id);
            get_cpu_affinity();

            NimbleProfile profile;
            (*stage)(Task(task_id, ring_id, tasks), profile);
            
            profile.print_to_file("../logs/q94_stage" + 
                std::to_string(tasks.curr_stage.stage_id) + "_parall" + 
                std::to_string(tasks.curr_stage.num_tasks) + "_task" + 
                std::to_string(task_id) + ".log");
            
            exit(0);
        }
        pids.push_back(pid);
    }
    return 0;
}

/*===========================================================================*/


/*==============================Query 95=====================================*/

/*
with ws_wh as (
  select
    ws1.ws_order_number,
    ws1.ws_warehouse_sk wh1,
    ws2.ws_warehouse_sk wh2
  from
    web_sales ws1,
    web_sales ws2
  where
    ws1.ws_order_number = ws2.ws_order_number
    and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
)
select
  count(distinct ws_order_number) as "order count",
  sum(ws_ext_ship_cost) as "total shipping cost",
  sum(ws_net_profit) as "total net profit"
from
  web_sales ws1,
  date_dim,
  customer_address,
  web_site
where
  d_date between '1999-4-01'
  and (cast('1999-4-01' as date) + 60 days)
  and ws1.ws_ship_date_sk = d_date_sk
  and ws1.ws_ship_addr_sk = ca_address_sk
  and ca_state = 'IA'
  and ws1.ws_web_site_sk = web_site_sk
  and web_company_name = 'pri'
  and ws1.ws_order_number in (
    select
      ws_order_number
    from
      ws_wh
  )
  and ws1.ws_order_number in (
    select
      wr_order_number
    from
      web_returns,
      ws_wh
    where
      wr_order_number = ws_wh.ws_order_number
  )
order by
  count(distinct ws_order_number)
limit
  100;
 */

void q95_stage1(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ws(web_sales);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName95, rawDir + "web_sales", &ws, web_sales, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);
    
    start = get_time();
    ShTable *ws_1 = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ws_1->initialize(q95_s1);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(ws_order_number, DType::Int64));
    columns.push_back(ColumnDesc(ws_warehouse_sk, DType::Int64));
    ws.project(columns, *ws_1);
    ws.deleteTable();
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Gather), 
        interBucketName95, interDir95 + "q95_s1", ws_1, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ws_1->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q95_stage2(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ws_1(q95_s1);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        interBucketName95, interDir95 + "q95_s1", &ws_1, q95_s1, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    size_t *idx = new size_t[ws_1.rows];
    ColumnDesc col(q95_s1_ws_order_number, DType::Int64);
    ws_1.sort_to_idx(col, idx);

    ShTable *ws_wh = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ws_wh->initialize(q95_s2);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(q95_s1_ws_order_number, DType::Int64));
    GroupByFunc byfunc(q95_s1_ws_warehouse_sk, DType::Int64, AggType::COUNT_DISTINCT_MAP);
    ws_1.groupby_sorted(idx, columns, byfunc, *ws_wh);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);
    
    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName95, interDir95 + "q95_s2", ws_wh, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    delete idx;
    ws_1.deleteTable();
    ws_wh->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q95_stage3(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ws_wh(q95_s2);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName95, interDir95 + "q95_s2", &ws_wh, q95_s2, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    size_t *idx = new size_t[ws_wh.rows];
    ColumnDesc col(q95_s2_ws_order_number, DType::Int64);
    ws_wh.sort_to_idx(col, idx);

    Table ws_wh_r(q95_s3);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(q95_s2_ws_order_number, DType::Int64));
    GroupByFunc byfunc(q95_s2_ws_warehouse_sk_count, DType::Int64, AggType::COUNT_DISTINCT_REDUCE);
    ws_wh.groupby_sorted(idx, columns, byfunc, ws_wh_r);
    delete[] idx;
    ws_wh.deleteTable();

    std::vector<Selector> selectors;
	selectors.push_back(Selector(q95_s3_ws_warehouse_sk_count, DType::Int64, GREATER, 1));
    size_t *idx_selected = new size_t[ws_wh_r.rows];
    size_t rows_selected = 0;
    ws_wh_r.select_to_idx(selectors, idx_selected, rows_selected);

    ShTable *ws_wh_order_num = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ws_wh_order_num->initialize(int_list);
    std::vector<ColumnDesc> target_cols;
    target_cols.push_back(ColumnDesc(q95_s3_ws_order_number, DType::Int64));
    ws_wh_r.select_with_project(idx_selected, rows_selected, target_cols, *ws_wh_order_num);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName95, interDir95 + "q95_s3", ws_wh_order_num, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    delete[] idx_selected;
    ws_wh_r.deleteTable();
    ws_wh_order_num->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q95_stage4(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ws(web_sales);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName95, rawDir + "web_sales", &ws, web_sales, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);
    
    start = get_time();
    // Table ws_j_t(q95_s4);
    ShTable *ws_j = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ws_j->initialize(q95_s4);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(ws_order_number, DType::Int64));
    columns.push_back(ColumnDesc(ws_ext_ship_cost, DType::Float32));
    columns.push_back(ColumnDesc(ws_net_profit, DType::Float32));
    columns.push_back(ColumnDesc(ws_ship_date_sk, DType::Int64));
    columns.push_back(ColumnDesc(ws_ship_addr_sk, DType::Int64));
    columns.push_back(ColumnDesc(ws_web_site_sk, DType::Int64));
    columns.push_back(ColumnDesc(ws_warehouse_sk, DType::Int64));
    ws.project(columns, *ws_j);
    // ws.project(columns, ws_j_t);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Gather), 
        interBucketName95, interDir95 + "q95_s4", ws_j, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ws.deleteTable();
    ws_j->deleteTable();
    // ws_j_t.deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q95_stage5(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table wr(web_returns);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName95, rawDir + "web_returns", &wr, web_returns, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    ShTable *wr_order_num = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    wr_order_num->initialize(int_list);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(wr_order_number, DType::Int64));
    wr.project(columns, *wr_order_num);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName95, interDir95 + "q95_s5", wr_order_num, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    wr.deleteTable();
    wr_order_num->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q95_stage6(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);

    // Read date_dim
    Table dd(date_dim);
    GetExternalTable(rawBucketName95, "date_dim.dat", dd);
    size_t *idx = new size_t[dd.rows];
    size_t rows_selected = 0;
    std::vector<Selector> selectors;
    selectors.push_back(Selector(d_date, DType::Date, GREATER, date_d(1999, 4, 1)));
    selectors.push_back(Selector(d_date, DType::Date, LESS, date_d(1999, 6, 1)));
    dd.select_to_idx(selectors, idx, rows_selected);
    Table dd_filtered(int_list);
    std::vector<ColumnDesc> dd_columns;
    dd_columns.push_back(ColumnDesc(d_date_sk, DType::Int64));
    dd.select_with_project(idx, rows_selected, dd_columns, dd_filtered);
    delete[] idx;
    dd.deleteTable();
    // Sort date_sk
    Table dd_filter_sorted(int_list);
    ColumnDesc dd_filter_sorted_desc(0, DType::Int64);
    dd_filtered.sort(dd_filter_sorted_desc, dd_filter_sorted);
    dd_filtered.deleteTable();
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);

    // Read s5 intermediate
    start = get_time();
    Table wr_order_num(int_list);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[2], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName95, interDir95 + "q95_s5", &wr_order_num, int_list, ring);
    end = get_time();
    profile.pre_time.tread += get_duration(start, end);
    // Sort wr_order_num
    start = get_time();
    Table wr_order_num_sorted(int_list);
    ColumnDesc wr_order_num_sorted_desc(0, DType::Int64);
    wr_order_num.sort(wr_order_num_sorted_desc, wr_order_num_sorted);
    wr_order_num.deleteTable();
    end = get_time();
    profile.pre_time.tcomp += get_duration(start, end);

    // Read s3 intermediate
    start = get_time();
    Table ws_wh_order_num(int_list);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName95, interDir95 + "q95_s3", &ws_wh_order_num, int_list, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);
    // Read s4 intermediate
    start = get_time();
    Table ws_j(q95_s4);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[1], task.meta.curr_stage, TransferOp::Gather), 
        interBucketName95, interDir95 + "q95_s4", &ws_j, q95_s4, ring);
    end = get_time();
    profile.effect_time.tread2 += get_duration(start, end);

    start = get_time();
    Table target_order_num(int_list);
    ws_wh_order_num.intersect_sorted(wr_order_num_sorted, target_order_num);
    ws_wh_order_num.deleteTable();
    wr_order_num_sorted.deleteTable();

    ColumnDesc col_desc2(q95_s4_ws_order_number, DType::Int64);
    size_t *idx2 = new size_t[ws_j.rows];
    size_t rows_selected2 = 0;
    ws_j.exists_binary_search_idx(col_desc2, target_order_num, idx2, rows_selected2);
    target_order_num.deleteTable();

    Joiner joiner(q95_s4_ws_ship_date_sk, DType::Int64, 0, DType::Int64);
    ShTable *merged = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    merged->initialize(q95_s4);
    ws_j.join_idx_exists_binary(dd_filter_sorted, joiner, idx2, rows_selected2, *merged);
    delete[] idx2;
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::Gather), 
        interBucketName95, interDir95 + "q95_s6", merged, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    dd_filter_sorted.deleteTable();
    ws_j.deleteTable();
    merged->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q95_stage7(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ca(customer_address);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        rawBucketName95, rawDir + "customer_address", &ca, customer_address, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    Table ca_p(q95_s7);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(ca_address_sk, DType::Int64));
	columns.push_back(ColumnDesc(ca_state, DType::String));
	ca.project(columns, ca_p);
    ca.deleteTable();

	Table ca_s(q95_s7);
	std::vector<Selector> selectors;
	selectors.push_back(Selector(q95_s7_ca_state, DType::String, EQUAL, "CA"));
	ca_p.select(selectors, ca_s);
    ca_p.deleteTable();

    ShTable *ca_sk = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    ca_sk->initialize(int_list);
    std::vector<ColumnDesc> columns2;
    columns2.push_back(ColumnDesc(q95_s7_ca_address_sk, DType::Int64));
    ca_s.project(columns2, *ca_sk);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName95, interDir95 + "q95_s7", ca_sk, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ca_s.deleteTable();
    ca_sk->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q95_stage8(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);

    Table wbst(web_site);
    GetExternalTable(rawBucketName95, "web_site.dat", wbst);
    Table web(q95_s8_web);
    std::vector<ColumnDesc> columns;
    columns.push_back(ColumnDesc(web_site_sk, DType::Int64));
    columns.push_back(ColumnDesc(web_company_name, DType::String));
    wbst.project(columns, web);
    wbst.deleteTable();
    Table web_select(q95_s8_web);
    std::vector<Selector> selectors;
    selectors.push_back(Selector(q95_s8_web_web_company_name, DType::String, EQUAL, "pri"));
    web.select(selectors, web_select);
    web.deleteTable();
    Table web_sk(int_list);
    std::vector<ColumnDesc> columns2;
    columns2.push_back(ColumnDesc(q95_s8_web_web_site_sk, DType::Int64));
    web_select.project(columns2, web_sk);
    web_select.deleteTable();
    Table web_sk_sorted(int_list);
    ColumnDesc web_sk_sorted_col(0, DType::Int64);
    web_sk.sort(web_sk_sorted_col, web_sk_sorted);
    web_sk.deleteTable();
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ca_sk(int_list);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[1], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName95, interDir95 + "q95_s7", &ca_sk, int_list, ring);
    end = get_time();
    profile.pre_time.tread += get_duration(start, end);
    start = get_time();
    Table ca_sk_sorted(int_list);
    ColumnDesc ca_sorted_col(0, DType::Int64);
    ca_sk.sort(ca_sorted_col, ca_sk_sorted);
    ca_sk.deleteTable();
    end = get_time();
    profile.pre_time.tcomp += get_duration(start, end);

    start = get_time();
    Table ws(q95_s4);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::Gather), 
        interBucketName95, interDir95 + "q95_s6", &ws, q95_s4, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    Table merged(q95_s4);
    Joiner joiner(q95_s4_ws_ship_addr_sk, DType::Int64, 0, DType::Int64);
    ws.join_exists_binary(ca_sk_sorted, joiner, merged);
    ws.deleteTable();
    ca_sk_sorted.deleteTable();

    Table merged2(q95_s4);
    Joiner joiner2(q95_s4_ws_web_site_sk, DType::Int64, 0, DType::Int64);
    merged.join_exists_binary(web_sk_sorted, joiner2, merged2);
    merged.deleteTable();
    web_sk_sorted.deleteTable();

    ShTable *r = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0); 
    r->initialize(q95_s8);
    std::vector<ColumnDesc> columns3;
    columns3.push_back(ColumnDesc(q95_s4_ws_order_number, DType::Int64));
    columns3.push_back(ColumnDesc(q95_s4_ws_ext_ship_cost, DType::Float32));
    columns3.push_back(ColumnDesc(q95_s4_ws_net_profit, DType::Float32));
    merged2.project(columns3, *r);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName95, interDir95 + "q95_s8", r, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    merged2.deleteTable();
    r->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

void q95_stage9(Task task, NimbleProfile &profile) {
    auto start = get_time();
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    io_pre_init(4, argv_secondary);
    struct rte_ring **ring;
    int ret = io_init_secondary(ring);
    auto end = get_time();
    profile.pre_time.tpre += get_duration(start, end);
    
    start = get_time();
    Table ws(q95_s8);
    GetTable(Transfer(task.task_id, task.ring_id, 
        task.meta.from_stage[0], task.meta.curr_stage, TransferOp::AllGather), 
        interBucketName95, interDir95 + "q95_s8", &ws, q95_s8, ring);
    end = get_time();
    profile.effect_time.tread1 += get_duration(start, end);

    start = get_time();
    ShTable *final_res = (ShTable *)rte_malloc(NULL, sizeof(ShTable), 0);
    final_res->initialize(q95_s8);
    ColumnDesc col1(q95_s8_ws_order_number, DType::Int64);
    size_t distinct_number = ws.count_distinct(col1);
    ColumnDesc col2(q95_s8_ws_ext_ship_cost, DType::Float32);
    double cost_sum = ws.sum(col2);
    ColumnDesc col3(q95_s8_ws_net_profit, DType::Float32);
    double profit_sum = ws.sum(col3);
    final_res->rows = 1;
    final_res->int_data[0].push_back(distinct_number);
    final_res->float_data[0].push_back(cost_sum);
    final_res->float_data[1].push_back(profit_sum);
    end = get_time();
    profile.effect_time.tcomp += get_duration(start, end);

    start = get_time();
    PutTable(Transfer(task.task_id, task.ring_id, 
        task.meta.curr_stage, task.meta.to_stage, TransferOp::AllGather), 
        interBucketName95, interDir95 + "q95_s9", final_res, ring);
    end = get_time();
    profile.effect_time.twrite += get_duration(start, end);

    start = get_time();
    ws.deleteTable();
    final_res->deleteTable();

    ret = io_exit_secondary(ring);
    Aws::ShutdownAPI(options);
    end = get_time();
    profile.effect_time.tpost += get_duration(start, end);
}

int execute_q95(TasksToExecute tasks, std::vector<pid_t> &pids) {
    void (*stage)(Task, NimbleProfile &) = nullptr;
    switch (tasks.curr_stage.stage_id) {
        case 1:
            stage = q95_stage1;
            break;
        case 2:
            stage = q95_stage2;
            break;
        case 3:
            stage = q95_stage3;
            break;
        case 4:
            stage = q95_stage4;
            break;
        case 5:
            stage = q95_stage5;
            break;
        case 6:
            stage = q95_stage6;
            break;
        case 7:
            stage = q95_stage7;
            break;
        case 8:
            stage = q95_stage8;
            break;
        case 9:
            stage = q95_stage9;
            break;
        default:
            LOG(ERROR) << "Invalid stage id " << tasks.curr_stage.stage_id;
            return -1;
    }
    for (int i = 0; i < tasks.curr_stage.num_local_tasks; i++) {
        int task_id = tasks.curr_stage.task_id_start + i;
        int ring_id = tasks.curr_stage.ring_id_start + i;
        pid_t pid = fork();
        if (pid == 0) {
            set_cpu_affinity(ring_id);
            get_cpu_affinity();

            NimbleProfile profile;
            (*stage)(Task(task_id, ring_id, tasks), profile);
            
            profile.print_to_file("../logs/q95_stage" + 
                std::to_string(tasks.curr_stage.stage_id) + "_parall" + 
                std::to_string(tasks.curr_stage.num_tasks) + "_task" + 
                std::to_string(task_id) + ".log");
            
            exit(0);
        }
        pids.push_back(pid);
    }
    return 0;
}

/*===========================================================================*/