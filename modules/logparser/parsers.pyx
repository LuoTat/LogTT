cdef object formatparse
cdef object ParseResult
cdef object parser_register

import formatparse
from libc.stdint cimport uint16_t, uint32_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from modules.core.parser cimport(
    AELLogParser as CXX_AELLogParser,
    DrainLogParser as CXX_DrainLogParser,
    JaccardDrainLogParser as CXX_JaccardDrainLogParser,
    Mask,
    SpellLogParser as CXX_SpellLogParser,
)

from .parse_result import ParseResult
from .parser_factory import parser_register

# ========================== AEL ==========================

cdef class AELLogParser:
    """AEL算法"""

    cdef CXX_AELLogParser log_parser

    def __init__(
        self,
        string log_format,
        object maskings=None,
        object delimiters=None,
        uint32_t cluster_thr=2,
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

        if delimiters is None:
            delimiters_cxx = vector[char]()
        else:
            delimiters_cxx = delimiters

        self.log_parser = CXX_AELLogParser(
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
        cdef uint32_t log_length

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

# ========================== Drain ==========================

cdef class DrainLogParser:
    """Drain算法"""

    cdef CXX_DrainLogParser log_parser

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
            depth: Depth of prefix tree (minimum 3).
            children: Max children per tree node.
            sim_thr: Similarity threshold (0-1).
        """

        if depth < 3:
            raise ValueError("depth argument must be at least 3")

        cdef object parser = formatparse.compile(log_format)
        cdef vector[Mask] maskings_cxx
        cdef vector[char] delimiters_cxx

        if maskings is None:
            maskings_cxx = vector[Mask]()
        else:
            maskings_cxx = maskings

        if delimiters is None:
            delimiters_cxx = vector[char]()
        else:
            delimiters_cxx = delimiters

        self.log_parser = CXX_DrainLogParser(
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
        cdef uint32_t log_length

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
        return "Drain"

    @staticmethod
    def description() -> str:
        return "Drain 是一种基于树结构的高效日志模板提取算法。"

# ========================== JaccardDrain ==========================

cdef class JaccardDrainLogParser:
    """Drain算法"""

    cdef CXX_JaccardDrainLogParser log_parser

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

        if delimiters is None:
            delimiters_cxx = vector[char]()
        else:
            delimiters_cxx = delimiters

        self.log_parser = CXX_JaccardDrainLogParser(
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
        cdef uint32_t log_length

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

# ========================== Spell ==========================

cdef class SpellLogParser:
    """Spell算法"""

    cdef CXX_SpellLogParser log_parser

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

        if delimiters is None:
            delimiters_cxx = vector[char]()
        else:
            delimiters_cxx = delimiters

        self.log_parser = CXX_SpellLogParser(
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
        cdef uint32_t log_length

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
        return "Spell"

    @staticmethod
    def description() -> str:
        return "Spell 是一种基于前缀树和LCS算法的日志解析方法。"

parser_register(AELLogParser)
parser_register(DrainLogParser)
parser_register(JaccardDrainLogParser)
parser_register(SpellLogParser)
