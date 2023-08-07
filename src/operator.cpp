/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#include <glog/logging.h>
#include "r_array.hpp"
#include "schema.hpp"
#include "table.hpp"
#include "utils.hpp"

/* For all operators, we assume the result table has been initialized */

bool Table::partition(int start_id, int num_splits, ShTable &table) {
    int total_cols = int_columns + float_columns + string_columns + date_columns;
    size_t start = rows / num_splits * start_id;
    size_t end = rows / num_splits * (start_id + 1);
    if (start_id == num_splits - 1) {
        end = rows;
    }
    size_t *idx = new size_t[end - start];
    for (size_t i = start; i < end; i++) {
        idx[i - start] = i;
    }
    select_by_index(idx, end - start, table);
    return true;
}

/* Combine the other tables and shared tables and this table to this table */
bool Table::hybrid_multi_combine(std::vector<ShTable> &shtables, std::vector<Table> &tables) {
    size_t new_rows = rows;
    for (auto &other : shtables) {
        // They must have the same schema
        if (int_columns != other.int_columns || float_columns != other.float_columns || 
            string_columns != other.string_columns || date_columns != other.date_columns) {
            std::cerr << "Combine: schema not match" << std::endl;
            return false;
        }
        new_rows += other.rows;
    }
    for (auto &other : tables) {
        // They must have the same schema
        if (int_columns != other.int_columns || float_columns != other.float_columns || 
            string_columns != other.string_columns || date_columns != other.date_columns) {
            std::cerr << "Combine: schema not match" << std::endl;
            return false;
        }
        new_rows += other.rows;
    }
    for (int i = 0; i < int_columns; i++) {
        int_data[i].resize(new_rows);
        size_t offset = rows;
        for (auto &other : shtables) {
            memcpy(int_data[i].data + offset, other.int_data[i].data, other.rows * sizeof(int64_t));
            offset += other.rows;
        }
        for (auto &other : tables) {
            memcpy(int_data[i].data + offset, other.int_data[i].data, other.rows * sizeof(int64_t));
            offset += other.rows;
        }
    }
    for (int i = 0; i < float_columns; i++) {
        float_data[i].resize(new_rows);
        size_t offset = rows;
        for (auto &other : shtables) {
            memcpy(float_data[i].data + offset, other.float_data[i].data, other.rows * sizeof(float));
            offset += other.rows;
        }
        for (auto &other : tables) {
            memcpy(float_data[i].data + offset, other.float_data[i].data, other.rows * sizeof(float));
            offset += other.rows;
        }
    }
    for (int i = 0; i < string_columns; i++) {
        for (auto &other : shtables) {
            for (size_t j = 0; j < other.rows; j++) {
                r_string s(other.string_data[i][j].data);
                string_data[i].push_back(s);
            }
        }
        for (auto &other : tables) {
            for (size_t j = 0; j < other.rows; j++) {
                r_string s(other.string_data[i][j].data);
                string_data[i].push_back(s);
            }
        }
    }
    for (int i = 0; i < date_columns; i++) {
        date_data[i].resize(new_rows);
        size_t offset = rows;
        for (auto &other : shtables) {
            memcpy(date_data[i].data + offset, other.date_data[i].data, other.rows * sizeof(date_d));
            offset += other.rows;
        }
        for (auto &other : tables) {
            memcpy(date_data[i].data + offset, other.date_data[i].data, other.rows * sizeof(date_d));
            offset += other.rows;
        }
    }
    rows = new_rows;
    return true;
}

/* Project to a subset of columns */
bool Table::project(std::vector<ColumnDesc> &columns, Table &result) {
    result.rows = rows;
    int int_col = 0;
    int float_col = 0;
    int string_col = 0;
    int date_col = 0;
    // Copy the data and avoid shallow copy
    for (auto &col : columns) {
        if (col.dtype == DType::Int64) {
            result.int_data[int_col].resize(rows);
            memcpy(result.int_data[int_col].data, 
                int_data[col.idx].data, rows * sizeof(int64_t));
            int_col++;
        } else if (col.dtype == DType::Float32) {
            result.float_data[float_col].resize(rows);
            memcpy(result.float_data[float_col].data, 
                float_data[col.idx].data, rows * sizeof(float));
            float_col++;
        } else if (col.dtype == DType::String) {
            for (size_t j = 0; j < rows; ++j) {
                r_string s(string_data[col.idx][j].data);
                result.string_data[string_col].push_back(s);
            }
            string_col++;
        } else if (col.dtype == DType::Date) {
            result.date_data[date_col].resize(rows);
            memcpy(result.date_data[date_col].data, 
                date_data[col.idx].data, rows * sizeof(date_d));
            date_col++;
        }
    }
    result.int_columns = int_col;
    result.float_columns = float_col;
    result.string_columns = string_col;
    result.date_columns = date_col;
    return true;
}

bool Table::project(std::vector<ColumnDesc> &columns, ShTable &result) {
    result.rows = rows;
    int int_col = 0;
    int float_col = 0;
    int string_col = 0;
    int date_col = 0;
    // Copy the data and avoid shallow copy
    for (auto &col : columns) {
        if (col.dtype == DType::Int64) {
            result.int_data[int_col].resize(rows);
            memcpy(result.int_data[int_col].data, 
                int_data[col.idx].data, rows * sizeof(int64_t));
            int_col++;
        } else if (col.dtype == DType::Float32) {
            result.float_data[float_col].resize(rows);
            memcpy(result.float_data[float_col].data, 
                float_data[col.idx].data, rows * sizeof(float));
            float_col++;
        } else if (col.dtype == DType::String) {
            for (size_t j = 0; j < rows; ++j) {
                sh_string s(string_data[col.idx][j].data);
                result.string_data[string_col].push_back(s);
            }
            string_col++;
        } else if (col.dtype == DType::Date) {
            result.date_data[date_col].resize(rows);
            memcpy(result.date_data[date_col].data, 
                date_data[col.idx].data, rows * sizeof(date_d));
            date_col++;
        }
    }
    result.int_columns = int_col;
    result.float_columns = float_col;
    result.string_columns = string_col;
    result.date_columns = date_col;
    return true;
}

/* Select by given index */
bool Table::select_by_index(size_t *idx, size_t rows, Table &result) {
    // Copy the table data indexed by idx to result
    // Note: the result schema is the same as the original table
    result.rows = rows;
    result.int_columns = int_columns;
    result.float_columns = float_columns;
    result.string_columns = string_columns;
    result.date_columns = date_columns;
    for (int i = 0; i < int_columns; ++i) {
        result.int_data[i].resize(result.rows);
        for (size_t j = 0; j < result.rows; ++j) {
            result.int_data[i][j] = int_data[i][idx[j]];
        }
    }
    for (int i = 0; i < float_columns; ++i) {
        result.float_data[i].resize(result.rows);
        for (size_t j = 0; j < result.rows; ++j) {
            result.float_data[i][j] = float_data[i][idx[j]];
        }
    }
    for (int i = 0; i < string_columns; ++i) {
        for (size_t j = 0; j < result.rows; ++j) {
            r_string s(string_data[i][idx[j]].data);
            result.string_data[i].push_back(s);
        }
    }
    for (int i = 0; i < date_columns; ++i) {
        result.date_data[i].resize(result.rows);
        for (size_t j = 0; j < result.rows; ++j) {
            result.date_data[i][j] = date_data[i][idx[j]];
        }
    }
    return true;
}

bool Table::select_by_index(size_t *idx, size_t rows, ShTable &result) {
    // Copy the table data indexed by idx to result
    // Note: the result schema is the same as the original table
    result.rows = rows;
    result.int_columns = int_columns;
    result.float_columns = float_columns;
    result.string_columns = string_columns;
    result.date_columns = date_columns;
    for (int i = 0; i < int_columns; ++i) {
        result.int_data[i].resize(result.rows);
        for (size_t j = 0; j < result.rows; ++j) {
            result.int_data[i][j] = int_data[i][idx[j]];
        }
    }
    for (int i = 0; i < float_columns; ++i) {
        result.float_data[i].resize(result.rows);
        for (size_t j = 0; j < result.rows; ++j) {
            result.float_data[i][j] = float_data[i][idx[j]];
        }
    }
    for (int i = 0; i < string_columns; ++i) {
        for (size_t j = 0; j < result.rows; ++j) {
            sh_string s(string_data[i][idx[j]].data);
            result.string_data[i].push_back(s);
        }
    }
    for (int i = 0; i < date_columns; ++i) {
        result.date_data[i].resize(result.rows);
        for (size_t j = 0; j < result.rows; ++j) {
            result.date_data[i][j] = date_data[i][idx[j]];
        }
    }
    return true;
}

/* Select rows based on a predicate
 * For now, we assume there are only four kinds of select operations:
    * 1. unary operation on one int64 or float32 column, e.g. select a from table where a > 10
    * 2. equal operation on one string column, e.g. select a from table where a = "hello"
    * 3. in_year operation on one date column, e.g. select a from table where a.year = 2002
    * 4. binary operation on one date column, 
    *   e.g. select a from table where a > '2022-01-01' and a < '2022-05-02'
    *   In this condition, we assume the first operation is > and the second operation is <
 */
bool Table::select(std::vector<Selector> &selectors, Table &result) {
    if (selectors.size() != 1 && selectors.size() != 2) {
        std::cerr << "Currently only support select with one or two selectors" << std::endl;
        return false;
    }
    if (selectors.size() == 1 && selectors[0].col.dtype != DType::String) {
        size_t *idx = new size_t[rows];
        size_t rows_selected = 0;
        if (selectors[0].col.dtype == DType::Int64) {
            r_array<int64_t> &data = int_data[selectors[0].col.idx];
            if (selectors[0].op == GREATER) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] > selectors[0].val.int_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else if (selectors[0].op == LESS) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] < selectors[0].val.int_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else if (selectors[0].op == EQUAL) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] == selectors[0].val.int_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else if (selectors[0].op == NOT_EQUAL) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] != selectors[0].val.int_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else {
                std::cerr << "Currently only support >, <, =, != for unary select" << std::endl;
                return false;
            }
            select_by_index(idx, rows_selected, result);
        } else if (selectors[0].col.dtype == DType::Float32) {
            r_array<float> &data = float_data[selectors[0].col.idx];
            size_t rows_selected = 0;;
            if (selectors[0].op == GREATER) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] > selectors[0].val.float_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else if (selectors[0].op == LESS) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] < selectors[0].val.float_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else if (selectors[0].op == EQUAL) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] == selectors[0].val.float_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else if (selectors[0].op == NOT_EQUAL) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] != selectors[0].val.float_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else {
                std::cerr << "Currently only support >, <, =, != for unary select" << std::endl;
                return false;
            }
            select_by_index(idx, rows_selected, result);
        } else {
            r_array<date_d> &data = date_data[selectors[0].col.idx];
            size_t rows_selected = 0;;
            if (selectors[0].op != IN_YEAR) {
                std::cerr << "Currently only support IN_YEAR for date select" << std::endl;
                return false;
            }
            for (size_t i = 0; i < rows; ++i) {
                if (data[i].year == selectors[0].val.date_val.year) {
                    idx[rows_selected] = i;
                    rows_selected++;
                }
            }
            select_by_index(idx, rows_selected, result);
        }
        delete[] idx;
    } else if (selectors.size() == 1 && selectors[0].col.dtype == DType::String) {
        if (selectors[0].op != EQUAL) {
            std::cerr << "Currently only support = for string select" << std::endl;
            return false;
        }
        size_t *idx = new size_t[rows];
        size_t rows_selected = 0;
        r_array<r_string> &val = string_data[selectors[0].col.idx];
        for (size_t i = 0; i < rows; ++i) {
            if (val[i] == selectors[0].val.str_val) {
                idx[rows_selected] = i;
                rows_selected++;
            }
        }
        select_by_index(idx, rows_selected, result);
        delete[] idx;
    } else {
        if (!(selectors[0].col.dtype == DType::Date && 
            selectors[1].col == selectors[0].col && 
            selectors[0].op == GREATER && selectors[1].op == LESS)) {
            // Assume the first selector is > and the second selector is <
            std::cerr << "Currently only support binary select on one date column" << std::endl;
            return false;
        }
        size_t *idx = new size_t[rows];
        size_t rows_selected = 0;
        r_array<date_d> &data = date_data[selectors[0].col.idx];
        for (size_t i = 0; i < rows; ++i) {
            if (selectors[0].val.date_val < data[i] && 
                data[i] < selectors[1].val.date_val) {
                idx[rows_selected] = i;
                rows_selected++;
            }
        }
        select_by_index(idx, rows_selected, result);
        delete[] idx;
    }
    return true;
}

/* Select rows based on a predicate, whose operator is IN
 * For now, we assume the IN predicate is on a string set
 */
bool Table::select_in(std::vector<Selector> &selectors, Table &result) {
    if (selectors[0].col.dtype != DType::String || 
        selectors[0].op != IN) {  // only check the first column
        std::cerr << "Currently only support IN on string column" << std::endl;
        return false;
    }
    size_t *idx = new size_t[rows];
    size_t rows_selected = 0;
    r_array<r_string> &val = string_data[selectors[0].col.idx];
    auto set_size = selectors.size();
    for (size_t i = 0; i < rows; ++i) {
        for (size_t j = 0; j < set_size; ++j) {
            if (val[i] == selectors[j].val.str_val) {
                idx[rows_selected] = i;
                rows_selected++;
                break;
            }
        }
    }
    select_by_index(idx, rows_selected, result);
    delete[] idx;
    return true;
}

/* This function selects the exactly SAME rows as Table::select does, 
 * but does not copy the entire table to mitigate data copy 
 */
bool Table::select_to_idx(std::vector<Selector> &selectors, size_t *idx, size_t &rows_selected) {
    if (selectors.size() != 1 && selectors.size() != 2) {
        std::cerr << "Currently only support select with one or two selectors" << std::endl;
        return false;
    }
    if (selectors.size() == 1 && selectors[0].col.dtype != DType::String) {
        rows_selected = 0;
        if (selectors[0].col.dtype == DType::Int64) {
            r_array<int64_t> &data = int_data[selectors[0].col.idx];
            if (selectors[0].op == GREATER) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] > selectors[0].val.int_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else if (selectors[0].op == LESS) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] < selectors[0].val.int_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else if (selectors[0].op == EQUAL) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] == selectors[0].val.int_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else if (selectors[0].op == NOT_EQUAL) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] != selectors[0].val.int_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else {
                std::cerr << "Currently only support >, <, =, != for unary select" << std::endl;
                return false;
            }
        } else if (selectors[0].col.dtype == DType::Float32) {
            r_array<float> &data = float_data[selectors[0].col.idx];
            rows_selected = 0;;
            if (selectors[0].op == GREATER) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] > selectors[0].val.float_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else if (selectors[0].op == LESS) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] < selectors[0].val.float_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else if (selectors[0].op == EQUAL) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] == selectors[0].val.float_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else if (selectors[0].op == NOT_EQUAL) {
                for (size_t i = 0; i < rows; ++i) {
                    if (data[i] != selectors[0].val.float_val) {
                        idx[rows_selected] = i;
                        rows_selected++;
                    }
                }
            } else {
                std::cerr << "Currently only support >, <, =, != for unary select" << std::endl;
                return false;
            }
        } else {
            r_array<date_d> &data = date_data[selectors[0].col.idx];
            rows_selected = 0;
            if (selectors[0].op != IN_YEAR) {
                std::cerr << "Currently only support IN_YEAR for date column" << std::endl;
                return false;
            }
            for (size_t i = 0; i < rows; ++i) {
                if (data[i].year == selectors[0].val.date_val.year) {
                    idx[rows_selected] = i;
                    rows_selected++;
                }
            }
        }
    } else if (selectors.size() == 1 && selectors[0].col.dtype == DType::String) {
        if (selectors[0].op != EQUAL) {
            std::cerr << "Currently only support = for string select" << std::endl;
            return false;
        }
        rows_selected = 0;
        r_array<r_string> &val = string_data[selectors[0].col.idx];
        for (size_t i = 0; i < rows; ++i) {
            if (val[i] == selectors[0].val.str_val) {
                idx[rows_selected] = i;
                rows_selected++;
            }
        }
    } else {
        if (!(selectors[0].col.dtype == DType::Date && 
            selectors[1].col == selectors[0].col && 
            selectors[0].op == GREATER && selectors[1].op == LESS)) {
            // Assume the first selector is > and the second selector is <
            std::cerr << "Currently only support binary select on one date column" << std::endl;
            return false;
        }
        rows_selected = 0;
        r_array<date_d> &data = date_data[selectors[0].col.idx];
        for (size_t i = 0; i < rows; ++i) {
            if (selectors[0].val.date_val < data[i] && 
                data[i] < selectors[1].val.date_val) {
                idx[rows_selected] = i;
                rows_selected++;
            }
        }
    }
    return true;
}

/* Select rows by idx and specified columns */
bool Table::select_with_project(size_t *idx, size_t rows_selected, 
    std::vector<ColumnDesc> &columns, Table &result) {

    if (columns.empty()) {
        std::cerr << "No columns to project" << std::endl;
        return false;
    }
    result.rows = rows_selected;
    int int_col = 0;
    int float_col = 0;
    int string_col = 0;
    int date_col = 0;
    for (auto &col : columns) {
        if (col.dtype == DType::Int64) {
            result.int_data[int_col].resize(rows_selected);
            for (size_t i = 0; i < rows_selected; ++i) {
                result.int_data[int_col][i] = int_data[col.idx][idx[i]];
            }
            int_col++;
        } else if (col.dtype == DType::Float32) {
            result.float_data[float_col].resize(rows_selected);
            for (size_t i = 0; i < rows_selected; ++i) {
                result.float_data[float_col][i] = float_data[col.idx][idx[i]];
            }
            float_col++;
        } else if (col.dtype == DType::String) {
            for (size_t j = 0; j < rows_selected; ++j) {
                r_string s(string_data[col.idx][idx[j]].data);
                result.string_data[string_col].push_back(s);
            }
            string_col++;
        } else if (col.dtype == DType::Date) {
            result.date_data[date_col].resize(rows_selected);
            for (size_t i = 0; i < rows_selected; ++i) {
                result.date_data[date_col][i] = date_data[col.idx][idx[i]];
            }
            date_col++;
        } else {
            std::cerr << "Unsupported data type" << std::endl;
            return false;
        }
    }
    result.int_columns = int_col;
    result.float_columns = float_col;
    result.string_columns = string_col;
    result.date_columns = date_col;
    return true;
}

bool Table::select_with_project(size_t *idx, size_t rows_selected, 
    std::vector<ColumnDesc> &columns, ShTable &result) {

    if (columns.empty()) {
        std::cerr << "No columns to project" << std::endl;
        return false;
    }
    result.rows = rows_selected;
    int int_col = 0;
    int float_col = 0;
    int string_col = 0;
    int date_col = 0;
    for (auto &col : columns) {
        if (col.dtype == DType::Int64) {
            result.int_data[int_col].resize(rows_selected);
            for (size_t i = 0; i < rows_selected; ++i) {
                result.int_data[int_col][i] = int_data[col.idx][idx[i]];
            }
            int_col++;
        } else if (col.dtype == DType::Float32) {
            result.float_data[float_col].resize(rows_selected);
            for (size_t i = 0; i < rows_selected; ++i) {
                result.float_data[float_col][i] = float_data[col.idx][idx[i]];
            }
            float_col++;
        } else if (col.dtype == DType::String) {
            for (size_t j = 0; j < rows_selected; ++j) {
                sh_string s(string_data[col.idx][idx[j]].data);
                result.string_data[string_col].push_back(s);
            }
            string_col++;
        } else if (col.dtype == DType::Date) {
            result.date_data[date_col].resize(rows_selected);
            for (size_t i = 0; i < rows_selected; ++i) {
                result.date_data[date_col][i] = date_data[col.idx][idx[i]];
            }
            date_col++;
        } else {
            std::cerr << "Unsupported data type" << std::endl;
            return false;
        }
    }
    result.int_columns = int_col;
    result.float_columns = float_col;
    result.string_columns = string_col;
    result.date_columns = date_col;
    return true;
}

bool Table::compare_to_idx(ColumnDesc &col, Table &other, int op, size_t *idx, size_t &rows_selected) {
    // Assume the other table has only one float column
    if (other.float_columns != 1 || other.int_columns != 0 || 
        other.string_columns != 0 || other.date_columns != 0) {
        std::cerr << "Currently only support one float column" << std::endl;
        return false;
    }
    if (col.dtype != DType::Float32) {
        std::cerr << "Currently only support float column" << std::endl;
        return false;
    }
    for (size_t i = 0; i < rows; ++i) {
        if (op == LESS) {
            if (float_data[col.idx][i] < other.float_data[0][i]) {
                idx[rows_selected] = i;
                rows_selected++;
            }
        } else if (op == GREATER) {
            if (float_data[col.idx][i] > other.float_data[0][i]) {
                idx[rows_selected] = i;
                rows_selected++;
            }
        } else {
            std::cerr << "Unsupported operator" << std::endl;
            return false;
        }
    }
    return true;
}

/* Drop duplicates by a given sorted index */
bool Table::drop_dup_sorted(size_t *idx, ColumnDesc &column, Table &result) {
    // Assume this table is a one-column(int64 or string) table
    int total_cols = int_columns + float_columns + date_columns + string_columns;
    if (total_cols != 1) {
        std::cerr << "Currently only support drop_duplicates on one-column table" << std::endl;
        return false;
    }
    if (column.dtype != DType::Int64 && column.dtype != DType::String) {
        std::cerr << "Currently only support drop_duplicates on int64 or string column" 
            << std::endl;
        return false;
    }
    // drop the duplicated rows in this table and store the result in result
    if (column.dtype == DType::Int64) {
        // Iterate the sorted index to drop duplicates
        r_array<int64_t> &data = int_data[column.idx];
        size_t rows_selected = 0;
        for (size_t i = 0; i < rows; ++i) {
            if (i == 0 || !(data[idx[i]] == data[idx[i - 1]])) {
                idx[rows_selected] = idx[i];
                rows_selected++;
            }
        }
        select_by_index(idx, rows_selected, result);
    } else {
        r_array<r_string> &data = string_data[column.idx];
        size_t rows_selected = 0;
        for (size_t i = 0; i < rows; ++i) {
            if (i == 0 || !(data[idx[i]] == data[idx[i - 1]])) {
                idx[rows_selected] = idx[i];
                rows_selected++;
            }
        }
        select_by_index(idx, rows_selected, result);
    }
    return true;
}

bool Table::drop_dup_sorted(size_t *idx, ColumnDesc &column, ShTable &result) {
    // Assume this table is a one-column(int64 or string) table
    int total_cols = int_columns + float_columns + date_columns + string_columns;
    if (total_cols != 1) {
        std::cerr << "Currently only support drop_duplicates on one-column table" << std::endl;
        return false;
    }
    if (column.dtype != DType::Int64 && column.dtype != DType::String) {
        std::cerr << "Currently only support drop_duplicates on int64 or string column" 
            << std::endl;
        return false;
    }
    // drop the duplicated rows in this table and store the result in result
    if (column.dtype == DType::Int64) {
        // Iterate the sorted index to drop duplicates
        r_array<int64_t> &data = int_data[column.idx];
        size_t rows_selected = 0;
        for (size_t i = 0; i < rows; ++i) {
            if (i == 0 || !(data[idx[i]] == data[idx[i - 1]])) {
                idx[rows_selected] = idx[i];
                rows_selected++;
            }
        }
        select_by_index(idx, rows_selected, result);
    } else {  // TODO: check the implementation when using it in query 1
        r_array<r_string> &data = string_data[column.idx];
        size_t rows_selected = 0;
        for (size_t i = 0; i < rows; ++i) {
            if (i == 0 || !(data[idx[i]] == data[idx[i - 1]])) {
                idx[rows_selected] = idx[i];
                rows_selected++;
            }
        }
        select_by_index(idx, rows_selected, result);
    }
    return true;
}

/* Sort the table according to the given column */
bool Table::sort(ColumnDesc &column, Table &result) {
    // Delete this table after this function returns if needed
    result.rows = rows;
    result.int_columns = int_columns;
    result.float_columns = float_columns;
    result.string_columns = string_columns;
    result.date_columns = date_columns;
    size_t *idx = new size_t[rows];
    for (size_t i = 0; i < rows; ++i)
        idx[i] = i;
    if (column.dtype == DType::Int64) {
        r_array<int64_t> &data = int_data[column.idx];
        std::sort(idx, idx + rows, [&data](int a, int b) {return data[a] < data[b];});
    } else if (column.dtype == DType::Float32) {
        r_array<float> &data = float_data[column.idx];
        std::sort(idx, idx + rows, [&data](int a, int b) {return data[a] < data[b];});
    } else if (column.dtype == DType::String) {
        r_array<r_string> &data = string_data[column.idx];
        std::sort(idx, idx + rows, [&data](int a, int b) {return data[a] < data[b];});
    } else if (column.dtype == DType::Date) {
        r_array<date_d> &data = date_data[column.idx];
        std::sort(idx, idx + rows, [&data](int a, int b) {return data[a] < data[b];});
    }
    bool res = select_by_index(idx, rows, result);
    delete[] idx;
    return res;
}

/* Sort by specified column and returns sorted_index */
bool Table::sort_to_idx(ColumnDesc &column, size_t *idx) {
    for (size_t i = 0; i < rows; ++i)
        idx[i] = i;
    if (column.dtype == DType::Int64) {
        r_array<int64_t> &data = int_data[column.idx];
        std::sort(idx, idx + rows, [&data](int a, int b) {return data[a] < data[b];});
    } else if (column.dtype == DType::Float32) {
        r_array<float> &data = float_data[column.idx];
        std::sort(idx, idx + rows, [&data](int a, int b) {return data[a] < data[b];});
    } else if (column.dtype == DType::String) {
        r_array<r_string> &data = string_data[column.idx];
        std::sort(idx, idx + rows, [&data](int a, int b) {return data[a] < data[b];});
    } else if (column.dtype == DType::Date) {
        r_array<date_d> &data = date_data[column.idx];
        std::sort(idx, idx + rows, [&data](int a, int b) {return data[a] < data[b];});
    }
    return true;
}

/* Sort by multiple columns (with priority) and returns sorted_index */
bool Table::sort_multi_col_to_idx(std::vector<ColumnDesc> &columns, size_t *idx) {
    // Assume two columns are int columns
    if (columns[0].dtype != DType::Int64 || columns[1].dtype != DType::Int64) {
        std::cerr << "Currently only support sort_multi_col_to_idx on two int columns" << std::endl;
        return false;
    }
    for (size_t i = 0; i < rows; ++i)
        idx[i] = i;
    r_array<int64_t> &data1 = int_data[columns[0].idx];
    r_array<int64_t> &data2 = int_data[columns[1].idx];
    std::sort(idx, idx + rows, [&data1, &data2](int a, int b) {
        if (data1[a] == data1[b])
            return data2[a] < data2[b];
        else
            return data1[a] < data1[b];
    });
    return true;
}

/* Apply groupby function for a given sorted index */
bool Table::groupby_sorted(size_t *idx, std::vector<ColumnDesc> &columns, 
    GroupByFunc by_func, Table &result) {
    // Currently only support group by one or two columns
    if (columns.size() != 1 && columns.size() != 2) {
        std::cerr << "Currently only support group by one or two columns" << std::endl;
        return false;
    }
    if (columns[0].dtype != DType::Int64) {
        std::cerr << "Currently only support group by int64 column" << std::endl;
        return false;
    }
    if (by_func.col.dtype != DType::Int64 && by_func.col.dtype != DType::Float32) {
        std::cerr << "Currently only support aggregation on int64 or float32 column" << std::endl;
        return false;
    }
    // Delete this table after this function returns if needed
    // assume no other columns except the group by column and the aggregation column
    /* Note: Number of columns of each kind in the result table are specified in the schema
     * Make sure the schema is correct */
    if (columns.size() == 1) {
        r_array<int64_t> &data = int_data[columns[0].idx];
        // build the group
        std::vector<Group> groups;
        groups.clear();
        int64_t last = data[idx[0]];
        size_t start = 0;
        for (size_t i = 1; i < rows; ++i) {
            if (data[idx[i]] != last) {
                groups.push_back(Group(start, i - start));
                last = data[idx[i]];
                start = i;
            }
        }
        groups.push_back(Group(start, rows - start));
        // apply the function
        result.rows = groups.size();
        result.int_data[0].resize(result.rows);
        for (size_t j = 0; j < result.rows; ++j)
            result.int_data[0][j] = int_data[0][idx[groups[j].start]];
        if (by_func.func == AggType::SUM) {
            result.float_data[0].resize(result.rows);
            for (size_t j = 0; j < result.rows; ++j) {
                float sum = 0;
                for (int k = 0; k < groups[j].len; ++k) {
                    if (by_func.col.dtype == DType::Int64)
                        sum += int_data[by_func.col.idx][idx[groups[j].start + k]];
                    else
                        sum += float_data[by_func.col.idx][idx[groups[j].start + k]];
                }
                result.float_data[0][j] = sum;
            }
        } else if (by_func.func == AggType::AVG) {
            result.float_data[0].resize(result.rows);
            for (size_t j = 0; j < result.rows; ++j) {
                float sum = 0;
                for (int k = 0; k < groups[j].len; ++k) {
                    if (by_func.col.dtype == DType::Int64)
                        sum += int_data[by_func.col.idx][idx[groups[j].start + k]];
                    else
                        sum += float_data[by_func.col.idx][idx[groups[j].start + k]];
                }
                result.float_data[0][j] = sum / groups[j].len;
            }
        } else if (by_func.func == AggType::COUNT_DISTINCT) {
            r_array<int64_t> &agg_data = int_data[by_func.col.idx];
            result.int_data[1].resize(result.rows);
            for (size_t j = 0; j < result.rows; ++j) {
                // returns 1 if all elements are the same and 2 if there are distinct elements
                result.int_data[1][j] = 1;
                int64_t last = agg_data[idx[groups[j].start]];
                for (int k = 1; k < groups[j].len; ++k) {
                    if (agg_data[idx[groups[j].start + k]] != last) {
                        result.int_data[1][j]++;
                        break;
                    }
                }
            }
        } else if (by_func.func == AggType::COUNT_DISTINCT_MAP) {
            r_array<int64_t> &agg_data = int_data[by_func.col.idx];
            result.int_data[1].resize(result.rows);
            result.int_data[2].resize(result.rows);
            for (size_t j = 0; j < result.rows; ++j) {
                result.int_data[1][j] = 1;
                int64_t last = agg_data[idx[groups[j].start]];
                result.int_data[2][j] = last;
                for (int k = 1; k < groups[j].len; ++k) {
                    if (agg_data[idx[groups[j].start + k]] != last) {
                        result.int_data[1][j]++;
                        result.int_data[2][j] = 0;
                        break;
                    }
                }
            }
        } else if (by_func.func == AggType::COUNT_DISTINCT_REDUCE) {
            r_array<int64_t> &agg_data = int_data[by_func.col.idx];
            r_array<int64_t> &agg_val = int_data[by_func.col.idx + 1]; // specified in the schema
            result.int_data[1].resize(result.rows);
            for (size_t j = 0; j < result.rows; ++j) {
                result.int_data[1][j] = 1;
                std::unordered_set<int64_t> s;
                s.clear();
                for (int k = 0; k < groups[j].len; ++k) {
                    if (agg_data[idx[groups[j].start + k]] > 1) {
                        result.int_data[1][j]++;
                        break;
                    } else if (agg_data[idx[groups[j].start + k]] == 1) {
                        s.insert(agg_val[idx[groups[j].start + k]]);
                        if (s.size() > 1) {
                            result.int_data[1][j]++;
                            break;
                        }
                    }
                }
            }
        }
    } else {
        if (columns[0].dtype != DType::Int64 || columns[1].dtype != DType::Int64) {
            std::cerr << "Currently only support groupby on two int64 columns" << std::endl;
            return false;
        }
        r_array<int64_t> &data1 = int_data[columns[0].idx];
        r_array<int64_t> &data2 = int_data[columns[1].idx];
        // build the group
        std::vector<Group> groups;
        groups.clear();
        int64_t last1 = data1[idx[0]];
        int64_t last2 = data2[idx[0]];
        size_t start = 0;
        for (size_t i = 1; i < rows; ++i) {
            if (data1[idx[i]] != last1 || data2[idx[i]] != last2) {
                groups.push_back(Group(start, i - start));
                last1 = data1[idx[i]];
                last2 = data2[idx[i]];
                start = i;
            }
        }
        groups.push_back(Group(start, rows - start));
        result.rows = groups.size();
        result.int_data[0].resize(result.rows);
        result.int_data[1].resize(result.rows);
        for (size_t j = 0; j < result.rows; ++j) {
            result.int_data[0][j] = data1[idx[groups[j].start]];
            result.int_data[1][j] = data2[idx[groups[j].start]];
        }
        if (by_func.func == AggType::SUM) {
            result.float_data[0].resize(result.rows);
            for (size_t j = 0; j < result.rows; ++j) {
                float sum = 0;
                for (int k = 0; k < groups[j].len; ++k) {
                    if (by_func.col.dtype == DType::Int64)
                        sum += int_data[by_func.col.idx][idx[groups[j].start + k]];
                    else
                        sum += float_data[by_func.col.idx][idx[groups[j].start + k]];
                }
                result.float_data[0][j] = sum;
            }
        } else {
            std::cerr << "Currently only support SUM on two int64 columns" << std::endl;
            return false;
        }
    }
    return true;
}

bool Table::groupby_sorted(size_t *idx, std::vector<ColumnDesc> &columns, 
    GroupByFunc by_func, ShTable &result) {
    // Currently only support group by one or two columns
    if (columns.size() != 1 && columns.size() != 2) {
        std::cerr << "Currently only support group by one or two columns" << std::endl;
        return false;
    }
    if (columns[0].dtype != DType::Int64) {
        std::cerr << "Currently only support group by int64 column" << std::endl;
        return false;
    }
    if (by_func.col.dtype != DType::Int64 && by_func.col.dtype != DType::Float32) {
        std::cerr << "Currently only support aggregation on int64 or float32 column" << std::endl;
        return false;
    }
    // Delete this table after this function returns if needed
    // assume no other columns except the group by column and the aggregation column
    /* Note: Number of columns of each kind in the result table are specified in the schema
     * Make sure the schema is correct */
    if (columns.size() == 1) {
        r_array<int64_t> &data = int_data[columns[0].idx];
        // build the group
        std::vector<Group> groups;
        groups.clear();
        int64_t last = data[idx[0]];
        size_t start = 0;
        for (size_t i = 1; i < rows; ++i) {
            if (data[idx[i]] != last) {
                groups.push_back(Group(start, i - start));
                last = data[idx[i]];
                start = i;
            }
        }
        groups.push_back(Group(start, rows - start));
        // apply the function
        result.rows = groups.size();
        result.int_data[0].resize(result.rows);
        for (size_t j = 0; j < result.rows; ++j)
            result.int_data[0][j] = int_data[0][idx[groups[j].start]];
        if (by_func.func == AggType::SUM) {
            result.float_data[0].resize(result.rows);
            for (size_t j = 0; j < result.rows; ++j) {
                float sum = 0;
                for (int k = 0; k < groups[j].len; ++k) {
                    if (by_func.col.dtype == DType::Int64)
                        sum += int_data[by_func.col.idx][idx[groups[j].start + k]];
                    else
                        sum += float_data[by_func.col.idx][idx[groups[j].start + k]];
                }
                result.float_data[0][j] = sum;
            }
        } else if (by_func.func == AggType::AVG) {
            result.float_data[0].resize(result.rows);
            for (size_t j = 0; j < result.rows; ++j) {
                float sum = 0;
                for (int k = 0; k < groups[j].len; ++k) {
                    if (by_func.col.dtype == DType::Int64)
                        sum += int_data[by_func.col.idx][idx[groups[j].start + k]];
                    else
                        sum += float_data[by_func.col.idx][idx[groups[j].start + k]];
                }
                result.float_data[0][j] = sum / groups[j].len;
            }
        } else if (by_func.func == AggType::COUNT_DISTINCT) {
            r_array<int64_t> &agg_data = int_data[by_func.col.idx];
            result.int_data[1].resize(result.rows);
            for (size_t j = 0; j < result.rows; ++j) {
                // returns 1 if all elements are the same and 2 if there are distinct elements
                result.int_data[1][j] = 1;
                int64_t last = agg_data[idx[groups[j].start]];
                for (int k = 1; k < groups[j].len; ++k) {
                    if (agg_data[idx[groups[j].start + k]] != last) {
                        result.int_data[1][j]++;
                        break;
                    }
                }
            }
        } else if (by_func.func == AggType::COUNT_DISTINCT_MAP) {
            r_array<int64_t> &agg_data = int_data[by_func.col.idx];
            result.int_data[1].resize(result.rows);
            result.int_data[2].resize(result.rows);
            for (size_t j = 0; j < result.rows; ++j) {
                result.int_data[1][j] = 1;
                int64_t last = agg_data[idx[groups[j].start]];
                result.int_data[2][j] = last;
                for (int k = 1; k < groups[j].len; ++k) {
                    if (agg_data[idx[groups[j].start + k]] != last) {
                        result.int_data[1][j]++;
                        result.int_data[2][j] = 0;
                        break;
                    }
                }
            }
        } else if (by_func.func == AggType::COUNT_DISTINCT_REDUCE) {
            r_array<int64_t> &agg_data = int_data[by_func.col.idx];
            r_array<int64_t> &agg_val = int_data[by_func.col.idx + 1]; // specified in the schema
            result.int_data[1].resize(result.rows);
            for (size_t j = 0; j < result.rows; ++j) {
                result.int_data[1][j] = 1;
                std::unordered_set<int64_t> s;
                s.clear();
                for (int k = 0; k < groups[j].len; ++k) {
                    if (agg_data[idx[groups[j].start + k]] > 1) {
                        result.int_data[1][j]++;
                        break;
                    } else if (agg_data[idx[groups[j].start + k]] == 1) {
                        s.insert(agg_val[idx[groups[j].start + k]]);
                        if (s.size() > 1) {
                            result.int_data[1][j]++;
                            break;
                        }
                    }
                }
            }
        }
    } else {
        if (columns[0].dtype != DType::Int64 || columns[1].dtype != DType::Int64) {
            std::cerr << "Currently only support groupby on two int64 columns" << std::endl;
            return false;
        }
        r_array<int64_t> &data1 = int_data[columns[0].idx];
        r_array<int64_t> &data2 = int_data[columns[1].idx];
        // build the group
        std::vector<Group> groups;
        groups.clear();
        int64_t last1 = data1[idx[0]];
        int64_t last2 = data2[idx[0]];
        size_t start = 0;
        for (size_t i = 1; i < rows; ++i) {
            if (data1[idx[i]] != last1 || data2[idx[i]] != last2) {
                groups.push_back(Group(start, i - start));
                last1 = data1[idx[i]];
                last2 = data2[idx[i]];
                start = i;
            }
        }
        groups.push_back(Group(start, rows - start));
        result.rows = groups.size();
        result.int_data[0].resize(result.rows);
        result.int_data[1].resize(result.rows);
        for (size_t j = 0; j < result.rows; ++j) {
            result.int_data[0][j] = data1[idx[groups[j].start]];
            result.int_data[1][j] = data2[idx[groups[j].start]];
        }
        if (by_func.func == AggType::SUM) {
            result.float_data[0].resize(result.rows);
            for (size_t j = 0; j < result.rows; ++j) {
                float sum = 0;
                for (int k = 0; k < groups[j].len; ++k) {
                    if (by_func.col.dtype == DType::Int64)
                        sum += int_data[by_func.col.idx][idx[groups[j].start + k]];
                    else
                        sum += float_data[by_func.col.idx][idx[groups[j].start + k]];
                }
                result.float_data[0][j] = sum;
            }
        } else {
            std::cerr << "Currently only support SUM on two int64 columns" << std::endl;
            return false;
        }
    }
    return true;
}

bool Table::groupby_sorted_append(size_t *idx, std::vector<ColumnDesc> &columns, 
    GroupByFunc by_func, Table &result) {
    if (columns.size() != 1 || columns[0].dtype != DType::Int64 || 
        by_func.func != AggType::AVG) {
        std::cerr << "Invalid Groupby Append" << std::endl;
        return false;
    }
    r_array<int64_t> &data = int_data[columns[0].idx];
    // build the group
    std::vector<Group> groups;
    groups.clear();
    int64_t last = data[idx[0]];
    size_t start = 0;
    for (size_t i = 1; i < rows; ++i) {
        if (data[idx[i]] != last) {
            groups.push_back(Group(start, i - start));
            last = data[idx[i]];
            start = i;
        }
    }
    groups.push_back(Group(start, rows - start));
    result.rows = rows;
    result.float_data[0].resize(result.rows);
    for (size_t j = 0; j < groups.size(); ++j) {
        float sum = 0;
        for (int k = 0; k < groups[j].len; ++k) {
            sum += float_data[by_func.col.idx][idx[groups[j].start + k]];
        }
        for (int k = 0; k < groups[j].len; ++k) {
            result.float_data[0][idx[groups[j].start + k]] = sum / groups[j].len;
        }
    }
    return true;
}

/* Fetch the rows that exist in the list table 
 * Assume the table and the list table are both sorted
 */
bool Table::exists_sorted(ColumnDesc &column, Table &list, Table &result) {
    // Assume the list table has one int column
    // int total_cols = int_columns + float_columns + date_columns + string_columns;
    int list_cols = list.int_columns + list.float_columns + 
        list.date_columns + list.string_columns;
    if (list_cols != 1) {
        std::cerr << "Currently only support is_in on one-column table" << std::endl;
        return false;
    }
    if (column.dtype != DType::Int64 && column.dtype != DType::String) {
        std::cerr << "Currently only support int64 and string column" << std::endl;
        return false;
    }
    if (!((column.dtype == DType::Int64 && list.int_columns == 1) || 
        (column.dtype == DType::String && list.string_columns == 1))) {
        std::cerr << "Table column type not match" << std::endl;
        return false;
    }
    // Fetch the rows that exist in the list table
    size_t *idx = new size_t[rows];
    size_t rows_selected = 0;
    if (column.dtype == DType::Int64) {
        // use two pointers to iterate the two tables to get the result
        r_array<int64_t> &data = int_data[column.idx];
        r_array<int64_t> &list_data = list.int_data[0];
        size_t i = 0, j = 0;
        while (i < rows && j < list.rows) {
            if (data[i] == list_data[j]) {
                idx[rows_selected] = i;
                rows_selected++;
                i++;
                j++;
            } else if (data[i] < list_data[j]) {
                i++;
            } else {
                j++;
            }
        }
        select_by_index(idx, rows_selected, result);
    } else {
        // use two pointers to iterate the two tables to get the result
        r_array<r_string> &data = string_data[column.idx];
        r_array<r_string> &list_data = list.string_data[0];
        size_t i = 0, j = 0;
        while (i < rows && j < list.rows) {
            if (data[i] == list_data[j]) {
                idx[rows_selected] = i;
                rows_selected++;
                i++;
                j++;
            } else if (data[i] < list_data[j]) {
                i++;
            } else {
                j++;
            }
        }
        select_by_index(idx, rows_selected, result);
    }
    delete[] idx;
    return true;
}

/* Fetch the rows that exist in the list table 
 * Assume the table and the list table are both sorted
 */
bool Table::exists_binary_search(ColumnDesc &column, Table &list, Table &result) {
    int list_cols = list.int_columns + list.float_columns + 
        list.date_columns + list.string_columns;
    if (!(column.dtype == DType::Int64 && list.int_columns == 1)) {
        std::cerr << "Table column type not match" << std::endl;
        return false;
    }
    // Fetch the rows that exist in the list table
    size_t *idx = new size_t[rows];
    size_t rows_selected = 0;
    r_array<int64_t> &data = int_data[column.idx];
    r_array<int64_t> &list_data = list.int_data[0];
    for (size_t i = 0; i < rows; ++i) {
        if (std::binary_search(list_data.data, list_data.data + list.rows, data[i])) {
            idx[rows_selected] = i;
            rows_selected++;
        }
    }
    select_by_index(idx, rows_selected, result);
    delete[] idx;
    return true;
}

bool Table::exists_binary_search_idx(ColumnDesc &column, Table &list, 
    size_t *idx, size_t &rows_selected) {
    
    int list_cols = list.int_columns + list.float_columns + 
        list.date_columns + list.string_columns;
    if (!(column.dtype == DType::Int64 && list.int_columns == 1)) {
        std::cerr << "Table column type not match" << std::endl;
        return false;
    }
    // Fetch the rows that exist in the list table
    r_array<int64_t> &data = int_data[column.idx];
    r_array<int64_t> &list_data = list.int_data[0];
    for (size_t i = 0; i < rows; ++i) {
        if (std::binary_search(list_data.data, list_data.data + list.rows, data[i])) {
            idx[rows_selected] = i;
            rows_selected++;
        }
    }
    return true;
}

/* Returns intersection of table[0] and other[0] 
 * Assume the table and the other table both are sorted and have only one int column
 * Note: the table has to be non-duplicated
 */
bool Table::intersect_sorted(Table &other, Table &result) {
    int total_cols = int_columns + float_columns + date_columns + string_columns;
    int other_cols = other.int_columns + other.float_columns + 
        other.date_columns + other.string_columns;
    if (total_cols != 1 || other_cols != 1 || int_columns != 1 || other.int_columns != 1) {
        std::cerr << "Currently only support intersection on int-lists" << std::endl;
        return false;
    }
    // Fetch the rows that exist in the list table
    size_t *idx = new size_t[rows];
    size_t rows_selected = 0;
    // use two pointers to iterate the two tables to get the result
    r_array<int64_t> &data = int_data[0];
    r_array<int64_t> &other_data = other.int_data[0];
    size_t i = 0, j = 0;
    while (i < rows && j < other.rows) {
        if (data[i] == other_data[j]) {
            idx[rows_selected] = i;
            rows_selected++;
            i++;
            j++;
        } else if (data[i] < other_data[j]) {
            i++;
        } else {
            j++;
        }
    }
    select_by_index(idx, rows_selected, result);
    delete[] idx;
    return true;
}

/* Returns intersection of table[0] and other[0]
 * Assume the table and the other table both have only one int column,
 * and the table is sorted and non-duplicated
 */
bool Table::intersect_binary_search(Table &other, Table &result) {
    int total_cols = int_columns + float_columns + date_columns + string_columns;
    int other_cols = other.int_columns + other.float_columns + 
        other.date_columns + other.string_columns;
    if (total_cols != 1 || other_cols != 1 || int_columns != 1 || other.int_columns != 1) {
        std::cerr << "Currently only support intersection on int-lists" << std::endl;
        return false;
    }

    size_t *idx = new size_t[other.rows];
    size_t rows_selected = 0;
    r_array<int64_t> &data = int_data[0];
    r_array<int64_t> &other_data = other.int_data[0];
    for (size_t i = 0; i < other.rows; i++) {
        if (std::binary_search(data.data, data.data + rows, other_data[i])) {
            idx[rows_selected] = i;
            rows_selected++;
        }
    }
    other.select_by_index(idx, rows_selected, result);
    // Note: the result is duplicated and not sorted
    delete[] idx;

    /* The set impl has poor performance */
    // r_array<int64_t> &data = int_data[0];
    // r_array<int64_t> &other_data = other.int_data[0];
    // std::set<int64_t> set;
    // for (size_t i = 0; i < rows; i++) {
    //     if (std::binary_search(other_data.data, other_data.data + other.rows, data[i])) {
    //         set.insert(data[i]);
    //     }
    // }
    // r_array<int64_t> &result_data = result.int_data[0];
    // result.rows = set.size();
    // result_data.resize(result.rows);
    // size_t i = 0;
    // for (auto it = set.begin(); it != set.end(); it++) {
    //     result_data[i] = *it;
    //     i++;
    // }
    
    return true;
}

/* Returns difference of table[0] and other[0] (table[0] - other[0])
 * Assume the table and the other table both are sorted and have only one int column
 * Note: the table has to be non-duplicated
 */
bool Table::difference_sorted(Table &other, Table &result) {
    int total_cols = int_columns + float_columns + date_columns + string_columns;
    int other_cols = other.int_columns + other.float_columns + 
        other.date_columns + other.string_columns;
    if (total_cols != 1 || other_cols != 1 || int_columns != 1 || other.int_columns != 1) {
        std::cerr << "Currently only support intersection on int-lists" << std::endl;
        return false;
    }
    // Fetch the rows that exist in the list table
    size_t *idx = new size_t[rows];
    size_t rows_selected = 0;
    // use two pointers to iterate the two tables to get the result
    r_array<int64_t> &data = int_data[0];
    r_array<int64_t> &other_data = other.int_data[0];
    size_t i = 0, j = 0;
    while (i < rows && j < other.rows) {
        if (data[i] == other_data[j]) {
            i++;
            j++;
        } else if (data[i] < other_data[j]) {
            idx[rows_selected] = i;
            rows_selected++;
            i++;
        } else {
            j++;
        }
    }
    while (i < rows) {
        idx[rows_selected] = i;
        rows_selected++;
        i++;
    }
    select_by_index(idx, rows_selected, result);
    delete[] idx;
    return true;
}

/* Assume the other table has only one int column
 * The column is sorted and will not be used later
 * somehow equals to exists_binary_search
 */
bool Table::join_exists_binary(Table &other, Joiner &joiner, Table &result) {
    // Sort the other table
    size_t *idx = new size_t[rows];
    size_t rows_selected = 0;
    if (joiner.left.dtype != DType::Int64) {
        std::cerr << "Currently only support join for int64 column" << std::endl;
        return false;
    }
    r_array<int64_t> &data = int_data[joiner.left.idx];
    r_array<int64_t> &other_data = other.int_data[0];
    for (size_t i = 0; i < rows; i++) {
        if (std::binary_search(other_data.data, other_data.data + other.rows, data[i])) {
            idx[rows_selected] = i;
            rows_selected++;
        }
    }
    select_by_index(idx, rows_selected, result);
    delete[] idx;
    return true;
}

bool Table::join_exists_binary(Table &other, Joiner &joiner, ShTable &result) {
    // Sort the other table
    size_t *idx = new size_t[rows];
    size_t rows_selected = 0;
    if (joiner.left.dtype != DType::Int64) {
        std::cerr << "Currently only support join for int64 column" << std::endl;
        return false;
    }
    r_array<int64_t> &data = int_data[joiner.left.idx];
    r_array<int64_t> &other_data = other.int_data[0];
    for (size_t i = 0; i < rows; i++) {
        if (std::binary_search(other_data.data, other_data.data + other.rows, data[i])) {
            idx[rows_selected] = i;
            rows_selected++;
        }
    }
    select_by_index(idx, rows_selected, result);
    delete[] idx;
    return true;
}

bool Table::join_idx_exists_binary(Table &other, Joiner &joiner, 
    size_t *idx, size_t rows_selected, Table &result) {
    size_t *final_idx = new size_t[rows_selected];
    size_t final_rows_selected = 0;
    if (joiner.left.dtype != DType::Int64) {
        std::cerr << "Currently only support join for int64 column" << std::endl;
        return false;
    }
    r_array<int64_t> &data = int_data[joiner.left.idx];
    r_array<int64_t> &other_data = other.int_data[0];
    for (size_t i = 0; i < rows_selected; i++) {
        if (std::binary_search(other_data.data, other_data.data + other.rows, 
            data[idx[i]])) {
            final_idx[final_rows_selected] = idx[i];
            final_rows_selected++;
        }
    }
    select_by_index(final_idx, final_rows_selected, result);
    delete[] final_idx;
    return true;
}

bool Table::join_idx_exists_binary(Table &other, Joiner &joiner, 
    size_t *idx, size_t rows_selected, ShTable &result) {
    size_t *final_idx = new size_t[rows_selected];
    size_t final_rows_selected = 0;
    if (joiner.left.dtype != DType::Int64) {
        std::cerr << "Currently only support join for int64 column" << std::endl;
        return false;
    }
    r_array<int64_t> &data = int_data[joiner.left.idx];
    r_array<int64_t> &other_data = other.int_data[0];
    for (size_t i = 0; i < rows_selected; i++) {
        if (std::binary_search(other_data.data, other_data.data + other.rows, 
            data[idx[i]])) {
            final_idx[final_rows_selected] = idx[i];
            final_rows_selected++;
        }
    }
    select_by_index(final_idx, final_rows_selected, result);
    delete[] final_idx;
    return true;
}

bool Table::join_binary_concat(Table &other, Joiner &joiner, 
    std::vector<ColumnDesc> left_cols, std::vector<ColumnDesc> right_cols, ShTable &result) {
    // Assume the other table has been sorted
    // Assume the result table has only int64, float32 and string columns
    if (result.date_columns > 0) {
        std::cerr << "Currently only support join for int64, float32 and string columns" << std::endl;
        return false;
    }
    r_array<int64_t> &data = int_data[joiner.left.idx];
    r_array<int64_t> &other_data = other.int_data[joiner.right.idx];
    std::vector<size_t> left_idx;
    std::vector<size_t> right_idx;
    left_idx.clear();
    right_idx.clear();
    for (size_t i = 0; i < rows; i++) {
        if (std::binary_search(other_data.data, other_data.data + other.rows, data[i])) {
            size_t start = std::lower_bound(other_data.data, other_data.data + other.rows, data[i]) - other_data.data;
            size_t end = std::upper_bound(other_data.data, other_data.data + other.rows, data[i]) - other_data.data;
            for (size_t j = start; j < end; j++) {
                left_idx.push_back(i);
                right_idx.push_back(j);
            }
        }
    }

    result.rows = left_idx.size();
    for (int i = 0; i < result.int_columns; i++) {
        result.int_data[i].resize(result.rows);
    }
    for (int i = 0; i < result.float_columns; i++) {
        result.float_data[i].resize(result.rows);
    }
    int int_cols = 0;
    int float_cols = 0;
    int string_cols = 0;
    for (auto &col : left_cols) {
        if (col.dtype == DType::Int64) {
            for (size_t i = 0; i < result.rows; i++) {
                result.int_data[int_cols][i] = int_data[col.idx][left_idx[i]];
            }
            int_cols++;
        } else if (col.dtype == DType::Float32) {
            for (size_t i = 0; i < result.rows; i++) {
                result.float_data[float_cols][i] = float_data[col.idx][left_idx[i]];
            }
            float_cols++;
        } else if (col.dtype == DType::String) {
            for (size_t i = 0; i < result.rows; i++) {
                sh_string s(string_data[col.idx][left_idx[i]].data);
                result.string_data[string_cols].push_back(s);
            }
            string_cols++;
        }
    }
    for (auto &col : right_cols) {
        if (col.dtype == DType::Int64) {
            for (size_t i = 0; i < result.rows; i++) {
                result.int_data[int_cols][i] = other.int_data[col.idx][right_idx[i]];
            }
            int_cols++;
        } else if (col.dtype == DType::Float32) {
            for (size_t i = 0; i < result.rows; i++) {
                result.float_data[float_cols][i] = other.float_data[col.idx][right_idx[i]];
            }
            float_cols++;
        } else if (col.dtype == DType::String) {
            for (size_t i = 0; i < result.rows; i++) {
                sh_string s(other.string_data[col.idx][right_idx[i]].data);
                result.string_data[string_cols].push_back(s);
            }
            string_cols++;
        }
    }
    return true;
}

/* Assume the other table has only one int column
 * The column is sorted and will not be used later
 */
bool Table::join_exists_hash(Table &other, Joiner &joiner, Table &result) {
    if (joiner.left.dtype != DType::Int64) {
        std::cerr << "Currently only support join for int64 column" << std::endl;
        return false;
    }
    r_array<int64_t> &data = int_data[joiner.left.idx];
    r_array<int64_t> &other_data = other.int_data[0];
    // use hash table to store the other table
    std::unordered_map<int64_t, int64_t> hash;
    for (size_t i = 0; i < other.rows; i++) {
        hash[other_data[i]] = 1;
    }
    size_t *idx = new size_t[rows];
    size_t rows_selected = 0;
    for (size_t i = 0; i < rows; i++) {
        if (hash.find(data[i]) != hash.end()) {
            idx[rows_selected] = i;
            rows_selected++;
        }
    }
    select_by_index(idx, rows_selected, result);
    delete[] idx;
    return true;
}

/* Count distinct */
size_t Table::count_distinct(ColumnDesc &column) {
    if (column.dtype != DType::Int64) {
        std::cerr << "Currently only support count distinct for int64 column" << std::endl;
        return 0;
    }
    r_array<int64_t> &data = int_data[column.idx];
    std::set<int64_t> set;
    for (size_t i = 0; i < rows; i++) {
        set.insert(data[i]);
    }
    return set.size();
}

/* Sum */
double Table::sum(ColumnDesc &column) {
    if (column.dtype != DType::Int64 && column.dtype != DType::Float32) {
        std::cerr << "Currently only support sum for int64 and float32 column" << std::endl;
        return -1;
    }
    if (column.dtype == DType::Int64) {
        r_array<int64_t> &data = int_data[column.idx];
        int64_t sum = 0;
        for (size_t i = 0; i < rows; i++) {
            sum += data[i];
        }
        return sum;
    } else {
        r_array<float> &data = float_data[column.idx];
        float sum = 0;
        for (size_t i = 0; i < rows; i++) {
            sum += data[i];
        }
        return sum;
    }
    return 0;
}

bool Table::multiply(float factor) {
    // Assume is float_list
    if (float_columns != 1) {
        std::cerr << "Currently only support multiply for float_list" << std::endl;
        return false;
    }
    r_array<float> &data = float_data[0];
    for (size_t j = 0; j < rows; j++) {
        data[j] *= factor;
    }
    return true;
}