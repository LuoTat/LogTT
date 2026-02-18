from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import TypeAlias

import polars as pl
import regex as re

from .parse_result import ParseResult

Token: TypeAlias = str
Content: TypeAlias = list[Token]


class BaseLogParser(ABC):
    """日志模板解析器基类"""

    def __init__(
        self,
        log_format: str,
        masking: list[tuple[str, str]] | None = None,
        delimiters: list[str] | None = None,
    ):
        """
        Args:
            log_format : log format string.
            masking : list of (regex, replacement) tuples for parameters masking.
            delimiters : delimiters for tokenization.
        """
        self._log_format = log_format
        self._masking = masking or []
        self._delimiters = delimiters or []

    def _mask_log_df(self, log_df: pl.DataFrame) -> pl.DataFrame:
        """Mask the DataFrame"""
        content_col = pl.col("Content")
        for regex, replacement in self._masking:
            content_col = content_col.str.replace_all(regex, replacement)
        return log_df.with_columns(content_col.alias("MaskedContent"))

    def _mask_log(self, log: str) -> str:
        """Mask the log"""
        for regex, replacement in self._masking:
            log = re.sub(regex, replacement, log)
        return log

    def _split_log_df(self, log_df: pl.DataFrame) -> pl.DataFrame:
        """Split the DataFrame"""
        content_col = pl.col("MaskedContent")
        for delim in self._delimiters:
            content_col = content_col.str.replace_all(delim, f"{delim} ", literal=True)
        # 分割后过滤掉空字符串
        content_col = content_col.str.split(" ").list.filter(pl.element() != "")
        return log_df.with_columns(content_col.alias("Tokens"))

    def _split_log(self, log: str) -> Content:
        """Split the log"""
        for delim in self._delimiters:
            log = log.replace(delim, f"{delim} ")
        return log.split()

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
            log_file : path of the input log file.
            structured_table_name : table name for structured log.
            templates_table_name : table name for templates log.
            should_stop : callback function to check if the process should stop.
            keep_para : whether to keep parameter list in structured log file.
            progress_callback : callback function to report progress (0-100).
        """

    ...

    @staticmethod
    @abstractmethod
    def name() -> str: ...

    @staticmethod
    @abstractmethod
    def description() -> str: ...
