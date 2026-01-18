from datetime import datetime
from dataclasses import dataclass


@dataclass
class LogRecord:
    id: int  # 日志ID
    log_type: str  # 日志类型
    format_type: str | None  # 日志格式类型
    log_uri: str  # 日志URI
    create_time: datetime  # 创建时间
    is_extracted: bool  # 是否已提取
    extract_method: str | None  # 提取方法
    line_count: int | None  # 日志行数