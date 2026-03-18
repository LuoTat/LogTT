from libc.stddef cimport size_t
from libc.stdint cimport uint16_t
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

cdef extern from "utils.cxx":
    pass

cdef extern from "base_log_parser.cxx":
    pass

cdef extern from "ael_log_parser.cxx":
    pass

cdef extern from "drain_log_parser.cxx":
    pass

cdef extern from "jaccard_drain_log_parser.cxx":
    pass

cdef extern from "spell_log_parser.cxx":
    pass

cdef extern from "precomp.hxx" namespace "logparser" nogil:
    ctypedef pair[string, string] Mask

cdef extern from "ael_log_parser.hxx" namespace "logparser" nogil:
    cdef cppclass AELLogParser:
        AELLogParser()
        AELLogParser(
            string         log_format,
            vector[string] named_fields,
            vector[Mask]   maskings,
            vector[char]   delimiters,
            size_t         cluster_thr,
            float          merge_thr,
        )

        size_t parse(const string& log_file, const string& structured_table_name, const string& templates_table_name, bint keep_para)

cdef extern from "drain_log_parser.hxx" namespace "logparser" nogil:
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

cdef extern from "jaccard_drain_log_parser.hxx" namespace "logparser" nogil:
    cdef cppclass JaccardDrainLogParser:
        JaccardDrainLogParser()
        JaccardDrainLogParser(
            string         log_format,
            vector[string] named_fields,
            vector[Mask]   maskings,
            vector[char]   delimiters,
            uint16_t       depth,
            uint16_t       children,
            float          sim_thr,
        )

        size_t parse(const string& log_file, const string& structured_table_name, const string& templates_table_name, bint keep_para)

cdef extern from "spell_log_parser.hxx" namespace "logparser" nogil:
    cdef cppclass SpellLogParser:
        SpellLogParser()
        SpellLogParser(
            string         log_format,
            vector[string] named_fields,
            vector[Mask]   maskings,
            vector[char]   delimiters,
            float          sim_thr,
        )

        size_t parse(const string& log_file, const string& structured_table_name, const string& templates_table_name, bint keep_para)
