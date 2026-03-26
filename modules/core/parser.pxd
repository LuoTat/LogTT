from libc.stdint cimport uint16_t, uint32_t
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

cdef extern from "src/precomp.hxx" namespace "logtt" nogil:
    ctypedef pair[string, string] Mask

cdef extern from "src/ael_log_parser.hxx" namespace "logtt" nogil:
    cdef cppclass AELLogParser:
        AELLogParser()
        AELLogParser(
            string         log_format,
            vector[string] named_fields,
            vector[Mask]   maskings,
            vector[char]   delimiters,
            uint32_t       cluster_thr,
            float          merge_thr,
        )
        uint32_t parse(
            const string& log_file,
            const string& structured_table_name,
            const string& templates_table_name,
            bint          keep_para,
        )

cdef extern from "src/drain_log_parser.hxx" namespace "logtt" nogil:
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
        uint32_t parse(
            const string& log_file,
            const string& structured_table_name,
            const string& templates_table_name,
            bint          keep_para,
        )

cdef extern from "src/jaccard_drain_log_parser.hxx" namespace "logtt" nogil:
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
        uint32_t parse(
            const string& log_file,
            const string& structured_table_name,
            const string& templates_table_name,
            bint          keep_para,
        )

cdef extern from "src/spell_log_parser.hxx" namespace "logtt" nogil:
    cdef cppclass SpellLogParser:
        SpellLogParser()
        SpellLogParser(
            string         log_format,
            vector[string] named_fields,
            vector[Mask]   maskings,
            vector[char]   delimiters,
            float          sim_thr,
        )
        uint32_t parse(
            const string& log_file,
            const string& structured_table_name,
            const string& templates_table_name,
            bint          keep_para,
        )
