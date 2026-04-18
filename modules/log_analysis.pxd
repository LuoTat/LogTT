from libc.stdint cimport uint32_t
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

cdef extern from "log_analysis.hxx" namespace "logtt" nogil:
    bint has_column(const string& structured_table_name, const string& column_name)
    vector[pair[string, uint32_t]] get_level_distribution(const string& structured_table_name)
