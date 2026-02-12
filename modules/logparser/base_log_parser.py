from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path

from .parse_result import ParseResult


class BaseLogParser(ABC):
    """日志模板解析器基类"""

    def __init__(self, log_format: str, regex: list[str]):
        """
        Initialize Base log parser.

        Args:
            log_format : log format string
            regex : regular expressions used in preprocessing
        """
        self._log_format = log_format
        self._regex = regex

    @abstractmethod
    def parse(
        self,
        log_file: Path,
        structured_table_name: str,
        templates_table_name: str,
        should_stop: Callable[[], bool],
        keep_para: bool = False,
        progress_callback: Callable[[int], None] | None = None,
    ) -> ParseResult:
        """
        parse the log file into SQL tables.

        Args:
            log_file : path of the input log file
            structured_table_name : table name for structured log
            templates_table_name : table name for templates log
            should_stop : callback function to check if the process should stop
            keep_para : whether to keep parameter list in structured log file
            progress_callback : callback function to report progress (0-100)
        """

    ...

    @staticmethod
    @abstractmethod
    def name() -> str: ...

    @staticmethod
    @abstractmethod
    def description() -> str: ...
