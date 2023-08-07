/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#ifndef TABLE_HPP
#define TABLE_HPP

#include <glog/logging.h>
#include "utils.hpp"
#include "schema.hpp"
#include "r_array.hpp"
#include "sh_array.hpp"
#include "shtable.hpp"
#include "operator.hpp"
#include "aws_inc.hpp"

class Table {
public:
    struct Schema schema;
    char delimiter;
    size_t rows;
    int int_columns;
    int float_columns;
    int string_columns;
    int date_columns;
    r_array<r_array<int64_t> > int_data;
    r_array<r_array<float> > float_data;
    r_array<r_array<r_string > > string_data;
    r_array<r_array<date_d> > date_data;

    Table() {
        rows = 0;
        int_columns = 0;
        float_columns = 0;
        string_columns = 0;
        date_columns = 0;
    }
    Table(int schema_type);  // Specify the schema type
    void initialize(int schema_type);
    ~Table() {}

    void setDelim(char delim);
    void deleteTable();  // NOTE: You MUST delete the table explicitly after using it

    /* Read/Write(Deserialize/Serialize) the table */
    bool readFromStream(Aws::IOStream &stream);
    bool writeToStream(Aws::StringStream &stream);
    // bool writeSplitToStream(Aws::StringStream &stream, int start_id, int num_splits);
    bool readFromRedis(std::vector<std::string> &strs);
    bool writeToRedis(std::vector<std::string> &strs);

    /* Split the table */
    bool partition(int start_id, int num_splits, ShTable &table);

    /* Combine multiple small tables to this table */
    bool hybrid_multi_combine(std::vector<ShTable> &shtables, std::vector<Table> &tables);

    /* Operators for the table */
    bool project(std::vector<ColumnDesc> &columns, Table &result);
    bool project(std::vector<ColumnDesc> &columns, ShTable &result);

    bool select_by_index(size_t *idx, size_t rows, Table &result);
    bool select_by_index(size_t *idx, size_t rows, ShTable &result);
    bool select(std::vector<Selector> &selectors, Table &result);
    bool select_to_idx(std::vector<Selector> &selectors, size_t *idx, size_t &rows_selected);
    bool select_in(std::vector<Selector> &selectors, Table &result);
    bool select_with_project(size_t *idx, size_t rows_selected, 
        std::vector<ColumnDesc> &columns, Table &result);
    bool select_with_project(size_t *idx, size_t rows_selected, 
        std::vector<ColumnDesc> &columns, ShTable &result);
    bool compare_to_idx(ColumnDesc &col, Table &other, int op, 
        size_t *idx, size_t &rows_selected);
    
    bool drop_dup_sorted(size_t *idx, ColumnDesc &column, Table &result);
    bool drop_dup_sorted(size_t *idx, ColumnDesc &column, ShTable &result);

    bool sort(ColumnDesc &column, Table &result);
    bool sort_to_idx(ColumnDesc &column, size_t *idx);
    bool sort_multi_col_to_idx(std::vector<ColumnDesc> &columns, size_t *idx);

    bool groupby_sorted(size_t *idx, std::vector<ColumnDesc> &columns, 
        GroupByFunc by_func, Table &result);
    bool groupby_sorted(size_t *idx, std::vector<ColumnDesc> &columns, 
        GroupByFunc by_func, ShTable &result);
    bool groupby_sorted_append(size_t *idx, std::vector<ColumnDesc> &columns, 
        GroupByFunc by_func, Table &result);

    bool exists_sorted(ColumnDesc &column, Table &list, Table &result);
    bool exists_binary_search(ColumnDesc &column, Table &list, Table &result);
    bool exists_binary_search_idx(ColumnDesc &column, Table &list, 
        size_t *idx, size_t &rows_selected);
    
    bool intersect_sorted(Table &other, Table &result);
    bool intersect_binary_search(Table &other, Table &result);

    bool difference_sorted(Table &other, Table &result);

    bool join_exists_binary(Table &other, Joiner &joiner, Table &result);
    bool join_exists_binary(Table &other, Joiner &joiner, ShTable &result);
    bool join_idx_exists_binary(Table &other, Joiner &joiner, 
        size_t *idx, size_t rows_selected, Table &result);
    bool join_idx_exists_binary(Table &other, Joiner &joiner, 
        size_t *idx, size_t rows_selected, ShTable &result);
    bool join_binary_concat(Table &other, Joiner &joiner, 
        std::vector<ColumnDesc> left_cols, std::vector<ColumnDesc> right_cols, ShTable &result);
    bool join_exists_hash(Table &other, Joiner &joiner, Table &result);
    
    size_t count_distinct(ColumnDesc &column);
    double sum(ColumnDesc &column);
    bool multiply(float factor);  // for float list

    /* Deprecated IO functions due to IO bottleneck of local disk */
    bool readFromDAT(const char *filename);
    bool readConcatFromDAT(const char *filename_prefix, int start_id, int num_files);
    bool writeToDAT(const char *filename);
    bool writeToPartitionedDAT(const char *filename_prefix, int num_partitions, int partition_id);
    bool writeToPartitionsDAT(const char *filename_prefix, int num_partitions);
};

Table::Table(int schema_type) {
    delimiter = '|';
    rows = 0;
    init_schema(schema, schema_type);
    int_columns = schema.int_columns;
    float_columns = schema.float_columns;
    string_columns = schema.string_columns;
    date_columns = schema.date_columns;
    if (int_columns > 0) {
        int_data = r_array<r_array<int64_t> >(int_columns);
        for (int i = 0; i < int_columns; i++) {
            int_data.push_back(r_array<int64_t>(INITIAL_CAPACITY));
        }
    }
    if (float_columns > 0) {
        float_data = r_array<r_array<float> >(float_columns);
        for (int i = 0; i < float_columns; i++) {
            float_data.push_back(r_array<float>(INITIAL_CAPACITY));
        }
    }
    if (string_columns > 0) {
        string_data = r_array<r_array<r_string> >(string_columns);
        for (int i = 0; i < string_columns; i++) {
            string_data.push_back(r_array<r_string>(INITIAL_CAPACITY));
        }
    }
    if (date_columns > 0) {
        date_data = r_array<r_array<date_d> >(date_columns);
        for (int i = 0; i < date_columns; i++) {
            date_data.push_back(r_array<date_d>(INITIAL_CAPACITY));
        }
    }
}

void Table::initialize(int schema_type) {
    delimiter = '|';
    rows = 0;
    init_schema(schema, schema_type);
    int_columns = schema.int_columns;
    float_columns = schema.float_columns;
    string_columns = schema.string_columns;
    date_columns = schema.date_columns;
    if (int_columns > 0) {
        int_data = r_array<r_array<int64_t> >(int_columns);
        for (int i = 0; i < int_columns; i++) {
            int_data.push_back(r_array<int64_t>(INITIAL_CAPACITY));
        }
    }
    if (float_columns > 0) {
        float_data = r_array<r_array<float> >(float_columns);
        for (int i = 0; i < float_columns; i++) {
            float_data.push_back(r_array<float>(INITIAL_CAPACITY));
        }
    }
    if (string_columns > 0) {
        string_data = r_array<r_array<r_string> >(string_columns);
        for (int i = 0; i < string_columns; i++) {
            string_data.push_back(r_array<r_string>(INITIAL_CAPACITY));
        }
    }
    if (date_columns > 0) {
        date_data = r_array<r_array<date_d> >(date_columns);
        for (int i = 0; i < date_columns; i++) {
            date_data.push_back(r_array<date_d>(INITIAL_CAPACITY));
        }
    }
}

void Table::setDelim(char delim) {
    delimiter = delim;
}

void Table::deleteTable() {
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

/* Deserializing the stream to the table */
bool Table::readFromStream(Aws::IOStream &stream) {
    std::string line;
    while (std::getline(stream, line)) {
        std::stringstream ss(line);
        std::string value;
        int col = 0;
        int int_col = 0;
        int float_col = 0;
        int string_col = 0;
        int date_col = 0;
        while (std::getline(ss, value, delimiter)) {
            DType dtype = schema.dtypes[col];
            if (dtype == DType::Int64) {
                if (unlikely(value.empty())) {
                    int_data[int_col].push_back(0);
                } else {
                    int_data[int_col].push_back(std::stoll(value));
                }
                int_col++;
            } else if (dtype == DType::Float32) {
                if (unlikely(value.empty())) {
                    float_data[float_col].push_back(0.0);
                } else {
                    float_data[float_col].push_back(std::stof(value));
                }
                float_col++;
            } else if (dtype == DType::String) {
                r_string str;
                if (value.empty()) {
                    str = "n/a";
                } else {
                    str = value.c_str();
                }
                string_data[string_col].push_back(str);
                string_col++;
            } else if (dtype == DType::Date) {
                date_d d;
                if (unlikely(value.empty())) {
                    d.set_date(1000, 1, 1);
                } else {
                    d.extract_from_string(value.c_str());
                }
                date_data[date_col].push_back(d);
                date_col++;
            }
            col++;
        }
        if (col < schema.num_cols) {  // lack of the last column
            DType dtype = schema.dtypes[col];
            if (dtype == DType::Int64) {
                int_data[int_col].push_back(0);
            } else if (dtype == DType::Float32) {
                float_data[float_col].push_back(0.0);
            } else if (dtype == DType::String) {
                r_string str = "n/a";
                string_data[string_col].push_back(str);
            } else if (dtype == DType::Date) {
                date_d d;
                d.set_date(1000, 1, 1);
                date_data[date_col].push_back(d);
            }
        }
        rows++;
    }
    return true;
}

bool Table::readFromRedis(std::vector<std::string> &strs) {
    for (auto it = strs.begin(); it != strs.end(); it++) {
        std::stringstream buf(*it);
        std::string line;
        while (std::getline(buf, line)) {
            std::stringstream ss(line);
            std::string value;
            int col = 0;
            int int_col = 0;
            int float_col = 0;
            int string_col = 0;
            int date_col = 0;
            while (std::getline(ss, value, delimiter)) {
                DType dtype = schema.dtypes[col];
                if (dtype == DType::Int64) {
                    if (unlikely(value.empty())) {
                        int_data[int_col].push_back(0);
                    } else {
                        int_data[int_col].push_back(std::stoll(value));
                    }
                    int_col++;
                } else if (dtype == DType::Float32) {
                    if (unlikely(value.empty())) {
                        float_data[float_col].push_back(0.0);
                    } else {
                        float_data[float_col].push_back(std::stof(value));
                    }
                    float_col++;
                } else if (dtype == DType::String) {
                    r_string str;
                    if (value.empty()) {
                        str = "n/a";
                    } else {
                        str = value.c_str();
                    }
                    string_data[string_col].push_back(str);
                    string_col++;
                } else if (dtype == DType::Date) {
                    date_d d;
                    if (unlikely(value.empty())) {
                        d.set_date(1000, 1, 1);
                    } else {
                        d.extract_from_string(value.c_str());
                    }
                    date_data[date_col].push_back(d);
                    date_col++;
                }
                col++;
            }
            if (col < schema.num_cols) {  // lack of the last column
                DType dtype = schema.dtypes[col];
                if (dtype == DType::Int64) {
                    int_data[int_col].push_back(0);
                } else if (dtype == DType::Float32) {
                    float_data[float_col].push_back(0.0);
                } else if (dtype == DType::String) {
                    r_string str = "n/a";
                    string_data[string_col].push_back(str);
                } else if (dtype == DType::Date) {
                    date_d d;
                    d.set_date(1000, 1, 1);
                    date_data[date_col].push_back(d);
                }
            }
            rows++;
        }
    }
    return true;
}

/* Serializing the table to a stream */
bool Table::writeToStream(Aws::StringStream &stream) {
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

bool Table::writeToRedis(std::vector<std::string> &strs) {
    const int row_split = 625000;
    int loop_cnt = rows / row_split;
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

/*=================Deprecated, Debug====================*/

bool Table::readFromDAT(const char *filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        LOG(ERROR) << "Cannot open file: " << filename;
        return false;
    }
    std::string line;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string value;
        int col = 0;
        int int_col = 0;
        int float_col = 0;
        int string_col = 0;
        int date_col = 0;
        while (std::getline(ss, value, delimiter)) {
            DType dtype = schema.dtypes[col];
            if (dtype == DType::Int64) {
                if (unlikely(value.empty())) {
                    int_data[int_col].push_back(0);
                } else {
                    int_data[int_col].push_back(std::stoll(value));
                }
                int_col++;
            } else if (dtype == DType::Float32) {
                if (unlikely(value.empty())) {
                    float_data[float_col].push_back(0.0);
                } else {
                    float_data[float_col].push_back(std::stof(value));
                }
                float_col++;
            } else if (dtype == DType::String) {
                r_string str;
                if (value.empty()) {
                    str = "n/a";
                } else {
                    str = value.c_str();
                }
                string_data[string_col].push_back(str);
                string_col++;
            } else if (dtype == DType::Date) {
                date_d d;
                if (unlikely(value.empty())) {
                    d.set_date(1000, 1, 1);
                } else {
                    d.extract_from_string(value.c_str());
                }
                date_data[date_col].push_back(d);
                date_col++;
            }
            col++;
        }
        if (col < schema.num_cols) {  // lack of the last column
            DType dtype = schema.dtypes[col];
            if (dtype == DType::Int64) {
                int_data[int_col].push_back(0);
            } else if (dtype == DType::Float32) {
                float_data[float_col].push_back(0.0);
            } else if (dtype == DType::String) {
                r_string str = "n/a";
                string_data[string_col].push_back(str);
            } else if (dtype == DType::Date) {
                date_d d;
                d.set_date(1000, 1, 1);
                date_data[date_col].push_back(d);
            }
        }
        rows++;
    }
    file.close();
    return true;
}

bool Table::readConcatFromDAT(const char *filename_prefix, int start_id, int num_files) {
    for (int i = start_id; i < start_id + num_files; i++) {
        std::string filename = std::string(filename_prefix) + "_" + std::to_string(i) + ".dat";
        if (!readFromDAT(filename.c_str())) {
            return false;
        }
    }
    return true;
}

bool Table::writeToDAT(const char *filename) {
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

bool Table::writeToPartitionedDAT(const char *filename_prefix, int num_partitions, int part_id) {
    std::string filename = std::string(filename_prefix) + "_" + std::to_string(part_id) + ".dat";
    std::ofstream file(filename);
    if (!file.is_open()) {
        return false;
    }
    int total_columns = int_columns + float_columns + string_columns + date_columns;
    size_t start = (rows / num_partitions) * part_id;
    size_t end = (part_id == num_partitions - 1) ? rows :
        (rows / num_partitions) * (part_id + 1);
    for (size_t row = start; row < end; row++) {
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

bool Table::writeToPartitionsDAT(const char *filename, int num_partitions) {
    for (int i = 0; i < num_partitions; i++) {
        if (!writeToPartitionedDAT(filename, num_partitions, i)) {
            return false;
        }
    }
    return true;
}

#endif /* TABLE_HPP */
