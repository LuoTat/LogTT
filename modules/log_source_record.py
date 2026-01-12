from datetime import datetime
from dataclasses import dataclass


@dataclass
class LogSourceRecord:
    id: int  # 日志源ID
    source_type: str  # 日志源类型
    source_uri: str  # 日志源URI
    create_time: datetime  # 创建时间
    is_extracted: bool  # 是否已提取
    extract_method: str | None  # 提取方法
    line_count: int | None  # 日志行数