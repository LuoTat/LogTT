from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

cdef class BaseLogParser:
    cdef string _log_format
    cdef vector[pair[string, string]] _maskings
    cdef vector[string] _delimiters
