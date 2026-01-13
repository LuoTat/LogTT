from pathlib import Path

from .base_log_parser import BaseLogParser, ParseResult
from .parser_factory import parser_register


@parser_register
class DrainParser(BaseLogParser):
    """Drain算法模板解析器"""

    def __init__(self, input_file: Path, log_format: str, regex: list):
        super().__init__(input_file, log_format, regex)

    def parse(self) -> ParseResult:
        from logparser.Drain import LogParser

        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)

        parser = LogParser(
            log_format=self.log_format,
            indir=str(self.input_file.parent),
            outdir=str(self.output_dir),
            rex=self.regex
        )

        # 提取日志模板
        parser.parse(self.input_file.name)

        # 生成的文件名
        base_name = self.input_file.stem
        structured_file = self.output_dir / f"{base_name}.log_structured.csv"
        templates_file = self.output_dir / f"{base_name}.log_templates.csv"

        return ParseResult(
            log_file=self.input_file,
            log_structured_file=structured_file,
            log_templates_file=templates_file,
        )

    @staticmethod
    def name() -> str:
        return "Drain"

    @staticmethod
    def description() -> str:
        return "Drain 是一种基于树结构的高效日志模板提取算法"