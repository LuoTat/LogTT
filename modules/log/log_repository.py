from typing import List
from datetime import datetime

from .log_record import LogRecord
from modules.db import get_connection


class LogRepository:
    """Log数据库操作类"""

    def __init__(self):
        self._init_table()

    @staticmethod
    def _init_table():
        with get_connection() as conn:
            conn.execute(
                """
                create table if not exists log
                (
                    id             INTEGER primary key autoincrement,
                    log_type       TEXT not null,
                    format_type    TEXT,
                    log_uri        TEXT not null unique,
                    create_time    TEXT not null,
                    is_extracted   INT  not null,
                    extract_method TEXT,
                    line_count     INT
                )
                """
            )
            conn.commit()

    @staticmethod
    def add(log: LogRecord):
        with get_connection() as conn:
            conn.execute(
                """
                insert into log (log_type, format_type, log_uri, create_time, is_extracted, extract_method, line_count)
                values (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    log.log_type,
                    log.format_type,
                    log.log_uri,
                    log.create_time.isoformat(timespec="seconds"),
                    log.is_extracted,
                    log.extract_method,
                    log.line_count
                ]
            )
            conn.commit()

    @staticmethod
    def delete(log_id: int):
        with get_connection() as conn:
            conn.execute(
                """
                delete
                from log
                where id = ?
                """,
                [log_id]
            )
            conn.commit()

    @staticmethod
    def get(log_id: int) -> LogRecord | None:
        with get_connection() as conn:
            row = conn.execute(
                """
                select id,
                       log_type,
                       format_type,
                       log_uri,
                       create_time,
                       is_extracted,
                       extract_method,
                       line_count
                from log
                where id = ?
                """,
                [log_id]
            ).fetchone()

        if row:
            return LogRecord(
                id=row[0],
                log_type=row[1],
                format_type=row[2],
                log_uri=row[3],
                create_time=datetime.fromisoformat(row[4]),
                is_extracted=bool(row[5]),
                extract_method=row[6],
                line_count=row[7])
        return None

    @staticmethod
    def get_all() -> List[LogRecord]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                select id,
                       log_type,
                       format_type,
                       log_uri,
                       create_time,
                       is_extracted,
                       extract_method,
                       line_count
                from log
                """
            ).fetchall()

        return [
            LogRecord(
                id=row[0],
                log_type=row[1],
                format_type=row[2],
                log_uri=row[3],
                create_time=datetime.fromisoformat(row[4]),
                is_extracted=bool(row[5]),
                extract_method=row[6],
                line_count=row[7])
            for row in rows
        ]

    @staticmethod
    def update_format_type(log_id: int, format_type: str):
        with get_connection() as conn:
            conn.execute(
                """
                update log
                set format_type = ?
                where id = ?
                """,
                [format_type, log_id]
            )
            conn.commit()

    @staticmethod
    def update_extract_method(log_id: int, extract_method: str):
        with get_connection() as conn:
            conn.execute(
                """
                update log
                set extract_method = ?
                where id = ?
                """,
                [extract_method, log_id]
            )
            conn.commit()

    @staticmethod
    def update_is_extracted(log_id: int, is_extracted: bool):
        with get_connection() as conn:
            conn.execute(
                """
                update log
                set is_extracted = ?
                where id = ?
                """,
                [is_extracted, log_id]
            )
            conn.commit()

    @staticmethod
    def update_line_count(log_id: int, line_count: int):
        with get_connection() as conn:
            conn.execute(
                """
                update log
                set line_count = ?
                where id = ?
                """,
                [line_count, log_id]
            )
            conn.commit()

    @staticmethod
    def search_by_uri(keyword: str) -> List[LogRecord]:
        """根据 log_uri 模糊搜索日志"""
        with get_connection() as conn:
            rows = conn.execute(
                """
                select id,
                       log_type,
                       format_type,
                       log_uri,
                       create_time,
                       is_extracted,
                       extract_method,
                       line_count
                from log
                where log_uri like ?
                """,
                [f"%{keyword}%"]
            ).fetchall()

        return [
            LogRecord(
                id=row[0],
                log_type=row[1],
                format_type=row[2],
                log_uri=row[3],
                create_time=datetime.fromisoformat(row[4]),
                is_extracted=bool(row[5]),
                extract_method=row[6],
                line_count=row[7])
            for row in rows
        ]