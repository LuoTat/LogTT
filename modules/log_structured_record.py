from datetime import datetime
from dataclasses import dataclass


@dataclass
class LogStructuredRecord:
    log_source_id: int  # 关联的日志源ID
    line_id: int  # 日志行号
    content: str  # 原始日志内容
    create_time: datetime  # 创建时间
    event_id: str  # 模板ID
    event_template: str  # 事件模板
    parameter_list: str | None  # 参数列表（JSON格式）