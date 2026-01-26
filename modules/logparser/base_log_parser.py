from pathlib import Path
from typing import Callable
from abc import ABC, abstractmethod

from .parse_result import ParseResult


class BaseLogParser(ABC):
    """日志模板解析器基类"""

    def __init__(self, log_id: int, log_file: Path, log_format: str, regex: list[str], should_stop: Callable[[], bool], progress_callback: Callable[[int], None] | None = None):
        """
        Attributes
        ----------
            log_id : unique identifier for this log
            log_file : path of the input log file
            log_format : log format string
            regex : regular expressions used in preprocessing (step1)
            should_stop : callback function to check if the process should stop
            progress_callback : callback function to report progress (0-100)
        """
        self.log_id = log_id
        self.log_file = log_file
        self.log_format = log_format
        self.regex = regex
        self.should_stop = should_stop
        self.progress_callback = progress_callback
        self.output_dir = Path(__file__).resolve().parent.parent.parent / "tmp"
        self.log_structured_file = self.output_dir / f"{self.log_id}_{self.log_file.stem}_structured.csv"
        self.log_templates_file = self.output_dir / f"{self.log_id}_{self.log_file.stem}_templates.csv"

    @abstractmethod
    def parse(self) -> ParseResult:
        pass

    @staticmethod
    @abstractmethod
    def name() -> str:
        pass

    @staticmethod
    @abstractmethod
    def description() -> str:
        pass