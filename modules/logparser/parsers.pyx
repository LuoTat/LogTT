cdef object formatparse
cdef object parser_register
cdef object ParamDescriptor
cdef object ParamWidgetType
cdef object ParseResult
cdef object QCoreApplication

from libc.stdint cimport uint16_t, uint32_t
from libcpp.string cimport string
from libcpp.vector cimport vector

import formatparse
from modules.logparser.parser cimport (
    AELLogParser as CXX_AELLogParser,
    BrainLogParser as CXX_BrainLogParser,
    DrainLogParser as CXX_DrainLogParser,
    JaccardDrainLogParser as CXX_JaccardDrainLogParser,
    Mask,
    SpellLogParser as CXX_SpellLogParser,
)
from PySide6.QtCore import QCoreApplication

from .param_descriptor import ParamDescriptor, ParamWidgetType
from .parse_result import ParseResult
from .parser_factory import parser_register

# ========================== AEL ==========================

cdef class AELLogParser:
    """AEL算法"""

    cdef CXX_AELLogParser log_parser

    def __init__(
        self,
        string log_format,
        vector[string] timestamp_fields,
        string timestamp_format,
        vector[Mask] maskings,
        object delimiters,
        uint32_t cluster_thr=2,
        float merge_thr=1,
    ):
        """
        Args:
            cluster_thr: Minimum number of log clusters to trigger reconciliation.
            merge_thr: Maximum percentage of difference to merge two log clusters.
        """

        cdef object parser = formatparse.compile(log_format)

        self.log_parser = CXX_AELLogParser(
            "^" + parser._expression + "$",
            parser.named_fields,
            timestamp_fields,
            timestamp_format,
            maskings,
            delimiters.encode("ascii"),
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
        return QCoreApplication.translate("AELLogParser", "AEL 是一种通过分桶与相似度合并来自动抽取日志模板的解析方法。")

    @staticmethod
    def get_param_descriptors() -> list[ParamDescriptor]:
        return [
            ParamDescriptor(
                "cluster_thr",
                "Cluster Threshold",
                QCoreApplication.translate("AELLogParser", "日志簇数量触发日志簇的合并的最小值"),
                ParamWidgetType.SpinBox,
                2,
                1,
                1000,
            ),
            ParamDescriptor(
                "merge_thr",
                "Merge Threshold",
                QCoreApplication.translate("AELLogParser", "合并两个日志簇的最大差异百分比"),
                ParamWidgetType.DoubleSpinBox,
                1.0,
                0.0,
                1.0,
            ),
        ]

# ========================== Brain ==========================

cdef class BrainLogParser:
    """Brain算法"""

    cdef CXX_BrainLogParser log_parser

    def __init__(
        self,
        string log_format,
        vector[string] timestamp_fields,
        string timestamp_format,
        vector[Mask] maskings,
        object delimiters,
        uint16_t var_thr=2,
    ):
        """
        Args:
            var_thr: Threshold for determining variable columns.
        """

        cdef object parser = formatparse.compile(log_format)

        self.log_parser = CXX_BrainLogParser(
            "^" + parser._expression + "$",
            parser.named_fields,
            timestamp_fields,
            timestamp_format,
            maskings,
            delimiters.encode("ascii"),
            var_thr,
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
        return "Brain"

    @staticmethod
    def description() -> str:
        return QCoreApplication.translate("BrainLogParser", "Brain 是一种基于双向树结构的高效日志模板提取算法。")

    @staticmethod
    def get_param_descriptors() -> list[ParamDescriptor]:
        return [
            ParamDescriptor(
                "var_thr",
                "Variable Threshold",
                QCoreApplication.translate("BrainLogParser", "判定变量列的最小不同词数量"),
                ParamWidgetType.SpinBox,
                2,
                1,
                100,
            ),
        ]

# ========================== Drain ==========================

cdef class DrainLogParser:
    """Drain算法"""

    cdef CXX_DrainLogParser log_parser

    def __init__(
        self,
        string log_format,
        vector[string] timestamp_fields,
        string timestamp_format,
        vector[Mask] maskings,
        object delimiters,
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

        self.log_parser = CXX_DrainLogParser(
            "^" + parser._expression + "$",
            parser.named_fields,
            timestamp_fields,
            timestamp_format,
            maskings,
            delimiters.encode("ascii"),
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
        return QCoreApplication.translate("DrainLogParser", "Drain 是一种基于树结构的高效日志模板提取算法。")

    @staticmethod
    def get_param_descriptors() -> list[ParamDescriptor]:
        return [
            ParamDescriptor(
                "depth",
                "Depth",
                QCoreApplication.translate("DrainLogParser", "前缀树的最大深度"),
                ParamWidgetType.SpinBox,
                4,
                3,
                100,
            ),
            ParamDescriptor(
                "children",
                "Max Children",
                QCoreApplication.translate("DrainLogParser", "每个树节点的最大子节点数"),
                ParamWidgetType.SpinBox,
                100,
                1,
                1000,
            ),
            ParamDescriptor(
                "sim_thr",
                "Similarity Threshold",
                QCoreApplication.translate("DrainLogParser", "相似度阈值"),
                ParamWidgetType.DoubleSpinBox,
                0.4,
                0.0,
                1.0,
            ),
        ]

# ========================== JaccardDrain ==========================

cdef class JaccardDrainLogParser:
    """Drain算法"""

    cdef CXX_JaccardDrainLogParser log_parser

    def __init__(
        self,
        string log_format,
        vector[string] timestamp_fields,
        string timestamp_format,
        vector[Mask] maskings,
        object delimiters,
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

        self.log_parser = CXX_JaccardDrainLogParser(
            "^" + parser._expression + "$",
            parser.named_fields,
            timestamp_fields,
            timestamp_format,
            maskings,
            delimiters.encode("ascii"),
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
        return QCoreApplication.translate("JaccardDrainLogParser", "JaccardDrain 是一种基于 Drain 和 Jaccard 相似度的高效日志模板提取算法。")

    @staticmethod
    def get_param_descriptors() -> list[ParamDescriptor]:
        return [
            ParamDescriptor(
                "depth",
                "Depth",
                QCoreApplication.translate("JaccardDrainLogParser", "前缀树的最大深度"),
                ParamWidgetType.SpinBox,
                4,
                2,
                100,
            ),
            ParamDescriptor(
                "children",
                "Max Children",
                QCoreApplication.translate("JaccardDrainLogParser", "每个树节点的最大子节点数"),
                ParamWidgetType.SpinBox,
                100,
                1,
                1000,
            ),
            ParamDescriptor(
                "sim_thr",
                "Similarity Threshold",
                QCoreApplication.translate("JaccardDrainLogParser", "相似度阈值"),
                ParamWidgetType.DoubleSpinBox,
                0.4,
                0.0,
                1.0,
            ),
        ]

# ========================== Spell ==========================

cdef class SpellLogParser:
    """Spell算法"""

    cdef CXX_SpellLogParser log_parser

    def __init__(
        self,
        string log_format,
        vector[string] timestamp_fields,
        string timestamp_format,
        vector[Mask] maskings,
        object delimiters,
        float sim_thr=0.5,
    ):
        """
        Args:
            sim_thr: Similarity threshold (0-1).
        """

        cdef object parser = formatparse.compile(log_format)

        self.log_parser = CXX_SpellLogParser(
            "^" + parser._expression + "$",
            parser.named_fields,
            timestamp_fields,
            timestamp_format,
            maskings,
            delimiters.encode("ascii"),
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
        return QCoreApplication.translate("SpellLogParser", "Spell 是一种基于前缀树和LCS算法的日志解析方法。")

    @staticmethod
    def get_param_descriptors() -> list[ParamDescriptor]:
        return [
            ParamDescriptor(
                "sim_thr",
                "Similarity Threshold",
                QCoreApplication.translate("SpellLogParser", "相似度阈值"),
                ParamWidgetType.DoubleSpinBox,
                0.5,
                0.0,
                1.0,
            ),
        ]

parser_register(AELLogParser)
parser_register(BrainLogParser)
parser_register(DrainLogParser)
parser_register(JaccardDrainLogParser)
parser_register(SpellLogParser)
