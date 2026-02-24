# distutils: language=c++

cdef object ParseResult
from .parse_result import ParseResult

cdef class BaseLogParser:
    """日志模板解析器基类"""

    def __init__(
        self,
        string log_format,
        object maskings=None,
        object delimiters=None,
    ):
        """
        Args:
            log_format : log format string.
            maskings : list of (regex, replacement) tuples for parameters masking.
            delimiters : delimiters for tokenization.
        """
        self._log_format = log_format
        if maskings is not None:
            self._maskings = maskings

        if delimiters is not None:
            self._delimiters = delimiters

    def parse(
        self,
        object log_file,
        string structured_table_name,
        string templates_table_name,
        bint keep_para = False,
    ) -> ParseResult:
        """
        parse the log file into SQL tables.

        Args:
            log_file : path of the input log file.
            structured_table_name : table name for structured log.
            templates_table_name : table name for templates log.
            keep_para : whether to keep parameter list in structured log file.
        """

        raise NotImplementedError()

    @staticmethod
    def name() -> str:
        raise NotImplementedError()

    @staticmethod
    def description() -> str:
        raise NotImplementedError()
