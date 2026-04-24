from libc.stdint cimport int32_t, int64_t
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector

cdef extern from "log_analysis.hxx" namespace "logtt" nogil:
    pair[vector[string], vector[int64_t]] get_level_distribution(const string& structured_table_name)
    pair[vector[int64_t], vector[int64_t]] get_log_frequency_distribution(const string& structured_table_name, int32_t months, int32_t days, int64_t micros)
    pair[vector[int64_t], vector[int64_t]] get_template_frequency_distribution(const string& structured_table_name, int32_t months, int32_t days, int64_t micros)
    unordered_map[string, pair[vector[int64_t], vector[int64_t]]] get_log_level_frequency_distribution(const string& structured_table_name, int32_t months, int32_t days, int64_t micros)
    pair[int64_t, vector[vector[int64_t]]] get_template_transition_matrix(const string& structured_table_name, const string& template_table_name)
    pair[int64_t, vector[vector[int64_t]]] get_template_cooccurrence_matrix(const string& structured_table_name, const string& template_table_name, int32_t months, int32_t days, int64_t micros)
