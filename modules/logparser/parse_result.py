from dataclasses import dataclass
from pathlib import Path


@dataclass
class ParseResult:
    """日志模板提取结果"""

    log_file: Path  # 原始日志文件路径
    line_count: int  # 日志行数
    log_structured_file: Path  # 结构日志CSV文件路径
    log_templates_file: Path  # 日志模板CSV文件路径
