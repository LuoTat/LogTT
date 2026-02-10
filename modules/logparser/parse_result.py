from dataclasses import dataclass
from pathlib import Path


@dataclass
class ParseResult:
    """日志模板提取结果"""

    log_file: Path  # 原始日志文件路径
    line_count: int  # 日志行数
    structured_table_name: str  # 结构化日志表名
    templates_table_name: str  # 模板日志表名
