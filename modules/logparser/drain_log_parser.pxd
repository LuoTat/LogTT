from libcpp.vector cimport vector
from libcpp.string cimport string
from libc.stdint cimport uint16_t

cdef extern from "drain_log_parser.cxx":
    pass

cdef extern from "drain_log_parser.hxx" namespace "logparser":
    cdef cppclass DrainLogParser:
        DrainLogParser()
        DrainLogParser(uint16_t depth, uint16_t children, float sim_thr)
        void parse(const string& logs,const string& logs,const vector[string]& logs)