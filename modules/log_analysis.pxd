from libc.stdint cimport int32_t, int64_t, uint32_t
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

cdef extern from "log_analysis.hxx" namespace "logtt" nogil:
    bint has_column(const string& structured_table_name, const string& column_name)
    vector[pair[string, uint32_t]] get_level_distribution(const string& structured_table_name)
    vector[pair[int64_t, uint32_t]] get_log_frequency_distribution(const string& structured_table_name, int32_t months, int32_t days, int64_t micros)
