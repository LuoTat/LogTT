cdef object formatparse
cdef object ParseResult
cdef object parser_register

import formatparse
from libc.stdint cimport uint16_t
from libcpp.string cimport string
from libcpp.vector cimport vector

from .cxx.jaccard_drain_log_parser cimport JaccardDrainLogParser as CXXDrainLogParser, Mask
from .parse_result import ParseResult
from .parser_factory import parser_register

cdef class JaccardDrainLogParser:
    """Drain算法"""

    cdef CXXDrainLogParser log_parser

    def __init__(
        self,
        string log_format,
        object maskings=None,
        object delimiters=None,
        uint16_t depth=4,
        uint16_t children=100,
        float sim_thr=0.4,
    ):
        """
        Args:
            depth: Depth of prefix tree (minimum 2).
            children: Max children per tree node.
            sim_thr: Similarity threshold (0-1).
        """

        if depth < 2:
            raise ValueError("depth argument must be at least 2")

        cdef object parser = formatparse.compile(log_format)
        cdef vector[Mask] maskings_cxx
        cdef vector[char] delimiters_cxx

        if maskings is None:
            maskings_cxx = vector[Mask]()
        else:
            maskings_cxx = maskings

        if delimiters is  None:
            delimiters_cxx = vector[char]()
        else:
            delimiters_cxx = delimiters

        self.log_parser = CXXDrainLogParser(
            "^" + parser._expression + "$",
            parser.named_fields,
            maskings_cxx,
            delimiters_cxx,
            depth,
            children,
            sim_thr,
        )

    def parse(
        self,
        string log_file,
        string structured_table_name,
        string templates_table_name,
        bint keep_para = False,
    ) -> ParseResult:
        cdef size_t log_length
        with nogil:
            log_length = self.log_parser.parse(
                log_file,
                structured_table_name,
                templates_table_name,
                keep_para,
            )

        return ParseResult(
            log_file,
            log_length,
            structured_table_name,
            templates_table_name,
        )

    @staticmethod
    def name() -> str:
        return "JaccardDrain"

    @staticmethod
    def description() -> str:
        return "JaccardDrain 是一种基于 Drain 和 Jaccard 相似度的高效日志模板提取算法。"

parser_register(JaccardDrainLogParser)
