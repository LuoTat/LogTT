from pathlib import Path
from dataclasses import dataclass
from abc import ABC, abstractmethod


@dataclass
class ParseResult:
    """日志模板提取结果"""
    log_file: Path  # 原始日志文件路径
    log_structured_file: Path  # 结构化CSV文件路径
    log_templates_file: Path  # 模板CSV文件路径


class BaseLogParser(ABC):
    """日志模板解析器基类"""
    input_file: Path
    output_dir: Path = Path(__file__).resolve().parent.parent / "tmp"
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