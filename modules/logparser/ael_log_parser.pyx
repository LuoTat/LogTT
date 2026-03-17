cdef object formatparse
cdef object ParseResult
cdef object parser_register

import formatparse
from libc.stddef cimport size_t
from libcpp.string cimport string
from libcpp.vector cimport vector

from .cxx.ael_log_parser cimport AELLogParser as CXXAELLogParser, Mask
from .parse_result import ParseResult
from .parser_factory import parser_register

cdef class AELLogParser:
    """AEL算法"""

    cdef CXXAELLogParser log_parser

    def __init__(
        self,
        string log_format,
        object maskings=None,
        object delimiters=None,
        size_t cluster_thr=2,
        float merge_thr=1,
    ):
        """
        Args:
            cluster_thr: Minimum number of log clusters to trigger reconciliation.
            merge_thr: Maximum percentage of difference to merge two log clusters.
        """

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

        self.log_parser = CXXAELLogParser(
            "^" + parser._expression + "$",
            parser.named_fields,
            maskings_cxx,
            delimiters_cxx,
            cluster_thr,
            merge_thr,
        )

    def parse(
        self,
        string log_file,
        string structured_table_name,
        string templates_table_name,
        bint keep_para=False,
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
        return "AEL"

    @staticmethod
    def description() -> str:
        return "AEL 是一种通过分桶与相似度合并来自动抽取日志模板的解析方法。"

parser_register(AELLogParser)
