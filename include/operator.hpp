/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

/*
 * This file defines the data structures used by the SQL-like operators for the table
 */

#ifndef OPERATOR_HPP
#define OPERATOR_HPP

#include <glog/logging.h>
#include "table.hpp"

/* Column description */
struct ColumnDesc {
    DType dtype;
    int idx;  // column index in the table of the specified dtype
    ColumnDesc(int idx, DType dtype) : idx(idx), dtype(dtype) {}
    bool operator==(const ColumnDesc& other) const {
        return idx == other.idx && dtype == other.dtype;
    }
};

enum SelectorType {
    EQUAL,
    LESS,
    GREATER,
    LESS_EQUAL,
    GREATER_EQUAL,
    NOT_EQUAL,
    IN,
    IN_YEAR  // for date_d
};

/* Selector 
 * We assume the selector is a single column selector
 * For example, SELECT * FROM table WHERE col1 = 1
 */
struct Selector {
    ColumnDesc col; // column to be selected
    int op;  // selector type
    struct VAL {  // value to be compared, currently only support int64_t, float and date
        int64_t int_val;
        float float_val;
        date_d date_val;
        r_string str_val;
    } val;
    Selector(int idx, DType dtype, int op, float val) : col(idx, dtype), op(op) {
        if (dtype == DType::Int64) {
            this->val.int_val = (int64_t)val;
        } else if (dtype == DType::Float32) {
            this->val.float_val = val;
        } else {
            std::cerr << "Unsupported data type\n";
            exit(1);
        }
    }
    Selector(int idx, DType dtype, int op, date_d val) : col(idx, dtype), op(op) {
        this->val.date_val = val;
    }
    Selector(int idx, DType dtype, int op, const char *val) : col(idx, dtype), op(op) {
        if (dtype == DType::String) {
            this->val.str_val = val;
        } else {
            std::cerr << "Unsupported data type\n";
            exit(1);
        }
    }
};

struct Group {
    int start;
    int len;
    Group(int _start, int _len) : start(_start), len(_len) {}
};

enum class AggType {  
    // TODO: support more types, only SUM, AVG and COUNT_DISTINCT are supported now
    SUM,
    AVG,
    // COUNT,
    // MIN,
    // MAX,
    COUNT_DISTINCT, /* count the number of distinct values, 
                     * returns 1 if all values are the same 
                     * and 2 if there are two distinct values, etc.
                     */
    COUNT_DISTINCT_MAP,  /* count the number of distinct values (distributed version) */
    COUNT_DISTINCT_REDUCE
};

struct GroupByFunc {
    ColumnDesc col;  // column
    AggType func;  // aggregation function type
    GroupByFunc(int idx, DType dtype, AggType _func) : col(idx, dtype), func(_func) {}
};

enum JoinerType {
    INNER_JOIN,
    LEFT_JOIN,
    RIGHT_JOIN,
    FULL_JOIN
};

/* Joiner */
struct Joiner {
    ColumnDesc left;
    ColumnDesc right;
    Joiner(int left_idx, DType left_dtype, int right_idx, DType right_dtype) : 
        left(left_idx, left_dtype), right(right_idx, right_dtype) {}
};

#endif /* OPERATOR_HPP */