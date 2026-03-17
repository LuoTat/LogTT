from libc.stdint cimport uint16_t
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

cdef extern from "utils.cxx":
    pass

cdef extern from "base_log_parser.cxx":
    pass

cdef extern from "drain_log_parser.cxx":
    pass

cdef extern from "precomp.hxx" namespace "logparser":
    ctypedef pair[string, string] Mask

cdef extern from "drain_log_parser.hxx" namespace "logparser":
    cdef cppclass DrainLogParser:
        DrainLogParser()
        DrainLogParser(
            string         log_format,
            vector[string] named_fields,
            vector[Mask]   maskings,
            vector[char]   delimiters,
            uint16_t       depth,
            uint16_t       children,
            float          sim_thr,
        )

        size_t parse(const string& log_file, const string& structured_table_name, const string& templates_table_name, bint keep_para)
