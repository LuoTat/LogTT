from pathlib import Path
from dataclasses import dataclass


@dataclass
class ParseResult:
    """日志模板提取结果"""
    log_file: Path  # 原始日志文件路径
    log_structured_file: Path  # 结构化CSV文件路径
    log_templates_file: Path  # 模板CSV文件路径