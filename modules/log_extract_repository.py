from typing import List
from datetime import datetime

from .db import get_connection
from .log_extract_record import LogExtractRecord


class LogExtractRepository:
    """日志提取记录数据库操作类"""

    def __init__(self):
        self._init_table()

    def _init_table(self):
        with get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS log_extracts
                (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    log_source_id  INTEGER NOT NULL,
                    line_num       INTEGER NOT NULL,
                    content        TEXT NOT NULL,
                    event_id       TEXT NOT NULL,
                    event_template TEXT NOT NULL,
                    parameter_list TEXT,
                    create_time    TEXT NOT NULL,
                    FOREIGN KEY (log_source_id) REFERENCES log_sources(id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_log_extracts_source_id ON log_extracts(log_source_id)
                """
            )
            conn.commit()

    def add(self, record: LogExtractRecord) -> int:
        """添加提取记录，返回新记录ID"""
        # TODO: 实现添加逻辑
        pass

    def add_batch(self, records: List[LogExtractRecord]):
        """批量添加提取记录"""
        # TODO: 实现批量添加逻辑
        pass

    def delete_by_source_id(self, log_source_id: int):
        """根据日志源ID删除所有相关提取记录"""
        # TODO: 实现删除逻辑
        pass

    def get(self, id: int) -> LogExtractRecord | None:
        """根据ID获取提取记录"""
        # TODO: 实现获取逻辑
        pass

    def get_by_source_id(self, log_source_id: int) -> List[LogExtractRecord]:
        """根据日志源ID获取所有提取记录"""
        # TODO: 实现获取逻辑
        pass

    def get_all(self) -> List[LogExtractRecord]:
        """获取所有提取记录"""
        # TODO: 实现获取逻辑
        pass
