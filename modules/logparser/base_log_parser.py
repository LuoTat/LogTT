from pathlib import Path
from abc import ABC, abstractmethod

from .parse_result import ParseResult


class BaseLogParser(ABC):
    """日志模板解析器基类"""

    input_file: Path
    output_dir: Path = Path(__file__).resolve().parent.parent.parent / "tmp"
    log_format: str
    regex: list[str]

    def __init__(self, input_file: Path, log_format: str, regex: list[str]):
        self.input_file = input_file
        self.log_format = log_format
        self.regex = regex

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