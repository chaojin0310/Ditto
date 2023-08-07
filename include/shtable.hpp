/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#ifndef SHTABLE_HPP
#define SHTABLE_HPP

#include <glog/logging.h>
#include "utils.hpp"
#include "schema.hpp"
#include "r_array.hpp"
#include "sh_array.hpp"
#include "operator.hpp"
#include "aws_inc.hpp"


class ShTable {
public:
    struct Schema schema;
    char delimiter;
    size_t rows;
    int int_columns;
    int float_columns;
    int string_columns;
    int date_columns;
    sh_array<sh_array<int64_t> > int_data;
    sh_array<sh_array<float> > float_data;
    sh_array<sh_array<sh_string> > string_data;
    sh_array<sh_array<date_d> > date_data;

    ShTable() {
        rows = 0;
        int_columns = 0;
        float_columns = 0;
        string_columns = 0;
        date_columns = 0;
    }
    ShTable(int schema_type);  // Specify the schema type
    void initialize(int schema_type);
    ~ShTable() {}
    
    void setDelim(char delim);
    void deleteTable();  // NOTE: MUST delete the table explicitly after using it

    bool writeToStream(Aws::StringStream &stream);
    bool writeToRedis(std::vector<std::string> &strs);
    bool select_by_index(size_t *idx, size_t rows, ShTable &result);
    bool partition(int start_id, int num_splits, ShTable &table);
    // bool writeSplitToStream(Aws::StringStream &stream, int start_id, int num_splits);
    bool writeToDAT(const char *filename);
};

ShTable::ShTable(int schema_type) {
    delimiter = '|';
    rows = 0;
    init_schema(schema, schema_type);
    int_columns = schema.int_columns;
    float_columns = schema.float_columns;
    string_columns = schema.string_columns;
    date_columns = schema.date_columns;
    if (int_columns > 0) {
        int_data = sh_array<sh_array<int64_t> >(int_columns);
        for (int i = 0; i < int_columns; i++) {
            int_data.push_back(sh_array<int64_t>(INITIAL_CAPACITY));
        }
    }
    if (float_columns > 0) {
        float_data = sh_array<sh_array<float> >(float_columns);
        for (int i = 0; i < float_columns; i++) {
            float_data.push_back(sh_array<float>(INITIAL_CAPACITY));
        }
    }
    if (string_columns > 0) {
        string_data = sh_array<sh_array<sh_string> >(string_columns);
        for (int i = 0; i < string_columns; i++) {
            string_data.push_back(sh_array<sh_string>(INITIAL_CAPACITY));
        }
    }
    if (date_columns > 0) {
        date_data = sh_array<sh_array<date_d> >(date_columns);
        for (int i = 0; i < date_columns; i++) {
            date_data.push_back(sh_array<date_d>(INITIAL_CAPACITY));
        }
    }
}

void ShTable::initialize(int schema_type) {
    delimiter = '|';
    rows = 0;
    init_schema(schema, schema_type);
    int_columns = schema.int_columns;
    float_columns = schema.float_columns;
    string_columns = schema.string_columns;
    date_columns = schema.date_columns;
    if (int_columns > 0) {
        int_data = sh_array<sh_array<int64_t> >(int_columns);
        for (int i = 0; i < int_columns; i++) {
            int_data.push_back(sh_array<int64_t>(INITIAL_CAPACITY));
        }
    }
    if (float_columns > 0) {
        float_data = sh_array<sh_array<float> >(float_columns);
        for (int i = 0; i < float_columns; i++) {
            float_data.push_back(sh_array<float>(INITIAL_CAPACITY));
        }
    }
    if (string_columns > 0) {
        string_data = sh_array<sh_array<sh_string> >(string_columns);
        for (int i = 0; i < string_columns; i++) {
            string_data.push_back(sh_array<sh_string>(INITIAL_CAPACITY));
        }
    }
    if (date_columns > 0) {
        date_data = sh_array<sh_array<date_d> >(date_columns);
        for (int i = 0; i < date_columns; i++) {
            date_data.push_back(sh_array<date_d>(INITIAL_CAPACITY));
        }
    }
}

void ShTable::setDelim(char delim) {
    delimiter = delim;
}

void ShTable::deleteTable() {
    if (int_columns > 0) {
        for (int i = 0; i < int_columns; i++) {
            int_data[i].delete_array();
        }
        int_data.delete_array();
    }
    if (float_columns > 0) {
        for (int i = 0; i < float_columns; i++) {
            float_data[i].delete_array();
        }
        float_data.delete_array();
    }
    if (string_columns > 0) {
        for (int i = 0; i < string_columns; i++) {
            for (size_t j = 0; j < string_data[i].get_size(); j++) {
                string_data[i][j].delete_string();
            }
            string_data[i].delete_array();
        }
        string_data.delete_array();
    }
    if (date_columns > 0) {
        for (int i = 0; i < date_columns; i++) {
            date_data[i].delete_array();
        }
        date_data.delete_array();
    }
}

bool ShTable::writeToStream(Aws::StringStream &stream) {
    int total_cols = int_columns + float_columns + string_columns + date_columns;
    for (size_t i = 0; i < rows; ++i) {
        int cols = 0;
        int int_col = 0;
        int float_col = 0;
        int string_col = 0;
        int date_col = 0;
        while (cols < total_cols) {
            if (schema.dtypes[cols] == DType::Int64) {
                stream << int_data[int_col][i];
                int_col++;
            } else if (schema.dtypes[cols] == DType::Float32) {
                stream << float_data[float_col][i];
                float_col++;
            } else if (schema.dtypes[cols] == DType::String) {
                stream << string_data[string_col][i];
                string_col++;
            } else if (schema.dtypes[cols] == DType::Date) {
                stream << date_data[date_col][i];
                date_col++;
            }
            if (cols < total_cols - 1) {
                stream << delimiter;
            }
            cols++;
        }
        stream << std::endl;
    }
    return true;
}

bool ShTable::writeToRedis(std::vector<std::string> &strs)
{
    const int row_split = 625000;
    int loop_cnt= rows / row_split;
    int total_cols = int_columns + float_columns + string_columns + date_columns;
    for (int l = 0; l < loop_cnt; l++) {
        std::stringstream ss;
        std::string cur_split;
        for (int i = 0; i < row_split; i++) {
            int cols = 0;
            int int_col = 0;
            int float_col = 0;
            int string_col = 0;
            int date_col = 0;
            while (cols < total_cols) {
                if (schema.dtypes[cols] == DType::Int64) {
                    ss << int_data[int_col][l*row_split+i];
                    int_col++;
                } else if (schema.dtypes[cols] == DType::Float32) {
                    ss << float_data[float_col][l*row_split+i];
                    float_col++;
                } else if (schema.dtypes[cols] == DType::String) {
                    ss << string_data[string_col][l*row_split+i];
                    string_col++;
                } else if (schema.dtypes[cols] == DType::Date) {
                    ss << date_data[date_col][l*row_split+i];
                    date_col++;
                }
                if (cols < total_cols - 1) {
                    ss << delimiter;
                }
                cols++;
            }
            ss << std::endl;
        }
        cur_split = ss.str();
        strs.push_back(cur_split);
    }
    int rem = loop_cnt * row_split;
    std::stringstream ss;
    std::string fin_split;
    for(int i = rem; i < rows; i++) {
        int cols = 0;
        int int_col = 0;
        int float_col = 0;
        int string_col = 0;
        int date_col = 0;
        while (cols < total_cols) {
            if (schema.dtypes[cols] == DType::Int64) {
                ss << int_data[int_col][i];
                int_col++;
            } else if (schema.dtypes[cols] == DType::Float32) {
                ss << float_data[float_col][i];
                float_col++;
            } else if (schema.dtypes[cols] == DType::String) {
                ss << string_data[string_col][i];
                string_col++;
            } else if (schema.dtypes[cols] == DType::Date) {
                ss << date_data[date_col][i];
                date_col++;
            }
            if (cols < total_cols - 1) {
                ss << delimiter;
            }
            cols++;
        }
        ss << std::endl;
    }
    fin_split = ss.str();
    strs.push_back(fin_split);
    return true;
}

bool ShTable::select_by_index(size_t *idx, size_t rows, ShTable &result) {
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

bool ShTable::partition(int start_id, int num_splits, ShTable &table) {
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

bool ShTable::writeToDAT(const char *filename) {
    std::ofstream file(filename);
    if (!file.is_open()) {
        return false;
    }
    int total_columns = int_columns + float_columns + string_columns + date_columns;
    for (size_t row = 0; row < rows; row++) {
        int int_col = 0;
        int float_col = 0;
        int string_col = 0;
        int date_col = 0;
        for (int col = 0; col < total_columns; col++) {
            DType dtype = schema.dtypes[col];
            if (dtype == DType::Int64) {
                file << int_data[int_col][row];
                int_col++;
            } else if (dtype == DType::Float32) {
                file << float_data[float_col][row];
                float_col++;
            } else if (dtype == DType::String) {
                file << string_data[string_col][row];
                string_col++;
            } else if (dtype == DType::Date) {
                file << date_data[date_col][row];
                date_col++;
            }
            if (col < total_columns - 1) {
                file << delimiter;
            }
        }
        file << std::endl;
    }
    file.close();
    return true;
}

#endif /* SHTABLE_HPP */