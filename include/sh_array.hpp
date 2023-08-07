/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

/*
 * This file contains the definition of useful data structures,
 * such as sh_array, sh_string, etc., which behave like STL, 
 * to fit with the requirements of the dpdk hugepage memory management.
 */

#ifndef SH_ARRAY_HPP
#define SH_ARRAY_HPP

#include <iostream>
#include <string.h>
#include <stdlib.h>
#include "r_array.hpp"
#include "spright_inc.hpp"

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
class sh_array {
public:
    T *data;
    size_t size;
    size_t capacity;
    sh_array() {  // default constructor, for temporary use
        capacity = 0;
        size = 0;
        data = nullptr;
    }
    sh_array(size_t cap) {  // constructor with capacity
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

        data = (T *)rte_malloc("sh_array", capacity * sizeof(T), 0);

        if (data == nullptr) {
            rte_exit(EXIT_FAILURE, "init: Cannot allocate memory for sh_array\n");
        }
    }
    ~sh_array() {}

    void delete_array() {  
        /* NOTE: You MUST delete the array explicitly before the program exits!
         * For multi-dimensional arrays, you should delete the array
         * from the innermost dimension to the outermost dimension.
         */
        if (data != nullptr) {
            rte_free(data);
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
bool sh_array<T>::empty() {
    return size == 0;
}

template <typename T>
size_t sh_array<T>::get_size() {
    return size;
}

template < typename T>
size_t sh_array<T>::get_capacity() {
    return capacity;
}

template <typename T>
int sh_array<T>::push_back(T element) {
    if (data == nullptr) {
        std::cerr << "Error: array not initialized" << std::endl;
        return R_ARRAY_NOT_INITIALIZED;
    }
    if (size == capacity) {
        T *new_data = (T *)rte_realloc(data, capacity * 2 * sizeof(T), 0);
        if (new_data == nullptr) {
            rte_exit(EXIT_FAILURE, "push_back: Cannot reallocate memory for sh_array\n");
        }
        data = new_data;
        capacity *= 2;
    }
    data[size] = element;
    size++;
    return R_ERROR_SUCCESS;
}

template <typename T>
T &sh_array<T>::operator[](int index) {
    if (index < 0 || index >= size) {
        std::cerr << "Error: index out of bound" << std::endl;
        rte_exit(EXIT_FAILURE, "operator[]: Index out of bound\n");
    }
    return data[index];
}

template <typename T>
bool sh_array<T>::resize(size_t new_size) {  
    // resize the array to new_size, used before memcpy data
    if (data == nullptr) {
        std::cerr << "Error: array not initialized" << std::endl;
        return false;
    }
    if (new_size > capacity) {
        T *new_data = (T *)rte_realloc(data, new_size * sizeof(T), 0);
        if (new_data == nullptr) {
            rte_exit(EXIT_FAILURE, "resize: Cannot reallocate memory for sh_array\n");
        }
        data = new_data;
        capacity = new_size;
    }
    size = new_size;
    return true;
}


class sh_string : public sh_array<char> {
public:
    sh_string() : sh_array<char>(STRING_INIT_CAPACITY) {}
    sh_string(size_t cap) : sh_array<char>(cap) {}
    sh_string(const char *str) : sh_array<char>(strlen(str) + 1) {
        size = strlen(str);
        memset(data, 0, capacity);
        memcpy(data, str, size);
    }
    ~sh_string() {}

    void delete_string() {
        /* NOTE: Delete the string explicitly as delete_array() does. */
        this->delete_array();
    }

    /* Override the operator= to get data from const char*
     * Recommand to use this function to assign value to sh_string for convenience
     */
    sh_string& operator=(const char* str) {
        size_t len = strlen(str);
        if (len >= capacity) {
            capacity = len + 1; // +1 for '\0'  
            /* Generally, a sh_string is read-only, so we don't need to double the capacity */
            char *new_data = (char *)rte_realloc(data, capacity * sizeof(char), 0);
            if (new_data == nullptr) {
                rte_exit(EXIT_FAILURE, "Cannot reallocate memory for sh_array\n");
            }
            data = new_data;
        }
        size = len;
        memset(data, 0, capacity);
        memcpy(data, str, size);
        return *this;
    }
    
    bool operator==(const sh_string &other) const {
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

    bool operator<(const sh_string &other) const {
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
    friend std::ostream& operator<<(std::ostream& os, const sh_string& str) {
        os << str.data;
        return os;
    }
};

#endif /* SH_ARRAY_HPP */