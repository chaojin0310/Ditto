/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

/*
 * This file contains the definition of useful data structures,
 * such as r_array, r_string, etc., which behave like STL, 
 * to fit with the requirements of the dpdk hugepage memory management.
 */

#ifndef R_ARRAY_HPP
#define R_ARRAY_HPP

#include <iostream>
#include <string.h>
#include <stdlib.h>
#include "sh_array.hpp"
#include "spright_inc.hpp"

// #define RTE_MODE

/* Common error codes */
#define R_ERROR_SUCCESS  0
#define R_ERROR_ERROR    1
#define R_ERROR_MEMORY   2
#define R_ELEMENT_RETURN_ERROR 3

/* Error codes for r_array */
#define R_ARRAY_NOT_INITIALIZED    101
#define R_ARRAY_INDEX_OUT_OF_BOUND 102
#define R_ARRAY_INSERT_FAILED      103

/* Capacity pre-definition */
#define STRING_INIT_CAPACITY 32
#define SMALL_INITIAL_CAPACITY 64
#define INITIAL_CAPACITY 32*16384

template <typename T>
class r_array {
public:
    T *data;
    size_t size;
    size_t capacity;
    r_array() {  // default constructor, for temporary use
        capacity = 0;
        size = 0;
        data = nullptr;
    }
    r_array(size_t cap) {  // constructor with capacity
        if (cap <= 0) {
            std::cerr << "Error: capacity must be positive" << std::endl;
            exit(1);
        }
        if (cap * 2 > INITIAL_CAPACITY) {
            capacity = 2 * (cap > INITIAL_CAPACITY ? cap : INITIAL_CAPACITY);
        } else if (cap <= SMALL_INITIAL_CAPACITY) {
            capacity = cap;
        } else {
            capacity = INITIAL_CAPACITY;
        }
        size = 0;

        #ifdef RTE_MODE
        data = (T *)rte_malloc("r_array", capacity * sizeof(T), 0);
        #else
        data = (T *)malloc(capacity * sizeof(T));
        #endif

        if (data == nullptr) {
            #ifdef RTE_MODE
            rte_exit(EXIT_FAILURE, "init: Cannot allocate memory for r_array\n");
            #else
            std::cerr << "Error: malloc failed" << std::endl;
            exit(1);
            #endif
        }
    }
    ~r_array() {}

    void delete_array() {  
        /* NOTE: You MUST delete the array explicitly before the program exits!
         * For multi-dimensional arrays, you should delete the array
         * from the innermost dimension to the outermost dimension.
         */
        if (data != nullptr) {
            #ifdef RTE_MODE
            rte_free(data);
            #else
            free(data);
            #endif
        }
        data = nullptr;
    }
    
    bool empty();
    size_t get_size();
    size_t get_capacity();
    int push_back(T element);
    T &operator[](int index);
    bool resize(size_t new_size);
};

template <typename T>
bool r_array<T>::empty() {
    return size == 0;
}

template <typename T>
size_t r_array<T>::get_size() {
    return size;
}

template < typename T>
size_t r_array<T>::get_capacity() {
    return capacity;
}

template <typename T>
int r_array<T>::push_back(T element) {
    if (data == nullptr) {
        std::cerr << "Error: array not initialized" << std::endl;
        return R_ARRAY_NOT_INITIALIZED;
    }
    if (size == capacity) {
        #ifdef RTE_MODE
        T *new_data = (T *)rte_realloc(data, capacity * 2 * sizeof(T), 0);
        #else
        T *new_data = (T *)realloc(data, capacity * 2 * sizeof(T));
        #endif
        if (new_data == nullptr) {
            #ifdef RTE_MODE
            rte_exit(EXIT_FAILURE, "push_back: Cannot reallocate memory for r_array\n");
            #else
            std::cerr << "Error: realloc failed" << std::endl;
            exit(1);
            #endif
        }
        data = new_data;
        capacity *= 2;
    }
    data[size] = element;
    size++;
    return R_ERROR_SUCCESS;
}

template <typename T>
T &r_array<T>::operator[](int index) {
    if (index < 0 || index >= size) {
        std::cerr << "Error: index out of bound" << std::endl;
        #ifdef RTE_MODE
        rte_exit(EXIT_FAILURE, "operator[]: Index out of bound\n");
        #else
        exit(1);
        #endif
    }
    return data[index];
}

template <typename T>
bool r_array<T>::resize(size_t new_size) {  
    // resize the array to new_size, used before memcpy data
    if (data == nullptr) {
        std::cerr << "Error: array not initialized" << std::endl;
        return false;
    }
    if (new_size > capacity) {
        #ifdef RTE_MODE
        T *new_data = (T *)rte_realloc(data, new_size * sizeof(T), 0);
        #else
        T *new_data = (T *)realloc(data, new_size * sizeof(T));
        #endif
        if (new_data == nullptr) {
            #ifdef RTE_MODE
            rte_exit(EXIT_FAILURE, "resize: Cannot reallocate memory for r_array\n");
            #else
            std::cerr << "Error: realloc failed" << std::endl;
            exit(1);
            #endif
        }
        data = new_data;
        capacity = new_size;
    }
    size = new_size;
    return true;
}


class r_string : public r_array<char> {
public:
    r_string() : r_array<char>(STRING_INIT_CAPACITY) {}
    r_string(size_t cap) : r_array<char>(cap) {}
    r_string(const char *str) : r_array<char>(strlen(str) + 1) {
        size = strlen(str);
        memset(data, 0, capacity);
        memcpy(data, str, size);
    }
    r_string(const sh_string &str) : r_array<char>(str.size + 1) {
        size = str.size;
        memset(data, 0, capacity);
        memcpy(data, str.data, size);
    }
    ~r_string() {}

    void delete_string() {
        /* NOTE: Delete the string explicitly as delete_array() does. */
        this->delete_array();
    }

    /* Override the operator= to get data from const char*
     * Recommand to use this function to assign value to r_string for convenience
     */
    r_string& operator=(const char* str) {
        size_t len = strlen(str);
        if (len >= capacity) {
            capacity = len + 1;  // +1 for '\0'  
            /* Generally, a r_string is read-only, so we don't need to double the capacity */
            #ifdef RTE_MODE
            char *new_data = (char *)rte_realloc(data, capacity * sizeof(char), 0);
            #else
            char *new_data = (char *)realloc(data, capacity * sizeof(char));
            #endif
            if (new_data == nullptr) {
                #ifdef RTE_MODE
                rte_exit(EXIT_FAILURE, "Cannot reallocate memory for r_array\n");
                #else
                std::cerr << "Error: realloc failed" << std::endl;
                exit(1);
                #endif
            }
            data = new_data;
        }
        size = len;
        memset(data, 0, capacity);
        memcpy(data, str, size);
        return *this;
    }
    
    bool operator==(const r_string &other) const {
        if (size != other.size) {
            return false;
        }
        for (size_t i = 0; i < size; i++) {
            if (data[i] != other.data[i]) {
                return false;
            }
        }
        return true;
    }

    bool operator<(const r_string &other) const {
        if (size < other.size) {
            return true;
        } else if (size > other.size) {
            return false;
        }
        for (size_t i = 0; i < size; i++) {
            if (data[i] < other.data[i]) {
                return true;
            } else if (data[i] > other.data[i]) {
                return false;
            }
        }
        return false;
    }

    // override the operator<< to output the string
    friend std::ostream& operator<<(std::ostream& os, const r_string& str) {
        os << str.data;
        return os;
    }
};


struct date_d {
    int year;
    int month;
    int day;

    date_d() : year(0), month(0), day(0) {}
    date_d(int y, int m, int d) : year(y), month(m), day(d) {}
    ~date_d() {}

    void set_date(int y, int m, int d) {
        year = y;
        month = m;
        day = d;
    }

    void extract_from_string(const char *str) {
        char *token = strtok((char *)str, "-");
        year = atoi(token);
        token = strtok(NULL, "-");
        month = atoi(token);
        token = strtok(NULL, "-");
        day = atoi(token);
    }

    std::string to_string() {
        std::string str = std::to_string(year) + "-" + 
            std::to_string(month) + "-" + std::to_string(day);
        return str;
    }

    bool operator<(const date_d &other) const {
        if (year < other.year) {
            return true;
        } else if (year > other.year) {
            return false;
        }
        if (month < other.month) {
            return true;
        } else if (month > other.month) {
            return false;
        }
        if (day < other.day) {
            return true;
        } else if (day > other.day) {
            return false;
        }
        return false;
    }

    int compare(const date_d &other) const {  // 0: equal, 1: greater, -1: less
        if (year > other.year) {
            return 1;
        } else if (year < other.year) {
            return -1;
        }
        if (month > other.month) {
            return 1;
        } else if (month < other.month) {
            return -1;
        }
        if (day > other.day) {
            return 1;
        } else if (day < other.day) {
            return -1;
        }
        return 0;
    }

    // override the operator<< to output the date
    friend std::ostream& operator<<(std::ostream& os, const date_d& date) {
        os << date.year << "-" << date.month << "-" << date.day;
        return os;
    }
};

#endif /* R_ARRAY_HPP */