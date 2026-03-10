# distutils: language=c++

cdef object ParseResult
cdef object parser_register

from libcpp.vector cimport vector
from libcpp.string cimport string
from libc.stdint cimport uint16_t

from datetime import datetime
from .base_log_parser cimport BaseLogParser
from .drain_log_parser cimport DrainLogParser as CXXDrainLogParser
from .parse_result import ParseResult
from .parser_factory import parser_register
from .utils cimport (
load_data,
mask_log_df,
split_log_df,
to_table,
)

cdef class DrainLogParser(BaseLogParser):
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
            depth: Depth of prefix tree (minimum 3).
            children: Max children per tree node.
            sim_thr: Similarity threshold (0-1).
        """
        super().__init__(log_format, maskings, delimiters)

        if depth < 3:
            raise ValueError("depth argument must be at least 3")

        self.log_parser = CXXDrainLogParser(depth, children, sim_thr)

    def parse(
            self,
            string log_file,
            string structured_table_name,
            string templates_table_name,
            bint keep_para = False,
    ) -> ParseResult:
        cdef object log_df = load_data(log_file, self._log_format)
        # 预处理日志内容：掩码处理 + 分词
        log_df = mask_log_df(log_df, self._maskings)
        log_df = split_log_df(log_df, self._delimiters)
        log_df = log_df.drop("raw").collect()

        cdef vector[string] log_templates
        cdef vector[vector[string]] logs = log_df["Tokens"]
        log_templates = self.log_parser.parse(logs)
        to_table(
            log_df,
            log_templates,
            structured_table_name,
            templates_table_name,
            keep_para,
        )

        return ParseResult(
            log_file,
            log_templates.size(),
            structured_table_name,
            templates_table_name,
        )

    @staticmethod
    def name() -> str:
        return "Drain"

    @staticmethod
    def description() -> str:
        return "Drain 是一种基于树结构的高效日志模板提取算法。"

parser_register(DrainLogParser)
