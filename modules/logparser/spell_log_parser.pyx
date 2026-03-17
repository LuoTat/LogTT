cdef object formatparse
cdef object ParseResult
cdef object parser_register

import formatparse
from libcpp.string cimport string
from libcpp.vector cimport vector

from .cxx.spell_log_parser cimport Mask, SpellLogParser as CXXSpellLogParser
from .parse_result import ParseResult
from .parser_factory import parser_register

cdef class SpellLogParser:
    """Spell算法"""

    cdef CXXSpellLogParser log_parser

    def __init__(
        self,
        string log_format,
        object maskings=None,
        object delimiters=None,
        float sim_thr=0.5,
    ):
        """
        Args:
            sim_thr: Similarity threshold (0-1).
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

        self.log_parser = CXXSpellLogParser(
            "^" + parser._expression + "$",
            parser.named_fields,
            maskings_cxx,
            delimiters_cxx,
            sim_thr,
        )

    def parse(
        self,
        string log_file,
        string structured_table_name,
        string templates_table_name,
        bint keep_para=False,
    ) -> ParseResult:
        cdef size_t log_length = self.log_parser.parse(
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
        return "Spell"

    @staticmethod
    def description() -> str:
        return "Spell 是一种基于前缀树和LCS算法的日志解析方法。"

parser_register(SpellLogParser)
