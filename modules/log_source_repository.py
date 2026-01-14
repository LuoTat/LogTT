from typing import List
from datetime import datetime

from .db import get_connection
from .log_source_record import LogSourceRecord


class LogSourceRepository:
    """Log源数据库操作类"""

    def __init__(self):
        self._init_table()

    @staticmethod
    def _init_table():
        with get_connection() as conn:
            conn.execute(
                """
                create table if not exists log_sources
                (
                    id             INTEGER primary key autoincrement,
                    source_type    TEXT not null,
                    format_type    TEXT,
                    source_uri     TEXT not null unique,
                    create_time    TEXT not null,
                    is_extracted   INT  not null,
                    extract_method TEXT,
                    line_count     INT
                )
                """
            )
            conn.commit()

    @staticmethod
    def add(log_source: LogSourceRecord):
        with get_connection() as conn:
            conn.execute(
                """
                insert into log_sources (source_type, format_type, source_uri, create_time, is_extracted, extract_method, line_count)
                values (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    log_source.source_type,
                    log_source.format_type,
                    log_source.source_uri,
                    log_source.create_time.isoformat(timespec="seconds"),
                    log_source.is_extracted,
                    log_source.extract_method,
                    log_source.line_count
                ]
            )
            conn.commit()

    @staticmethod
    def delete(log_source_id: int):
        with get_connection() as conn:
            conn.execute(
                """
                delete
                from log_sources
                where id = ?
                """,
                [log_source_id]
            )
            conn.commit()

    @staticmethod
    def get(log_source_id: int) -> LogSourceRecord | None:
        with get_connection() as conn:
            row = conn.execute(
                """
                select id,
                       source_type,
                       format_type,
                       source_uri,
                       create_time,
                       is_extracted,
                       extract_method,
                       line_count
                from log_sources
                where id = ?
                """,
                [log_source_id]
            ).fetchone()

        if row:
            return LogSourceRecord(
                id=row[0],
                source_type=row[1],
                format_type=row[2],
                source_uri=row[3],
                create_time=datetime.fromisoformat(row[4]),
                is_extracted=bool(row[5]),
                extract_method=row[6],
                line_count=row[7])
        return None

    @staticmethod
    def get_all() -> List[LogSourceRecord]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                select id,
                       source_type,
                       format_type,
                       source_uri,
                       create_time,
                       is_extracted,
                       extract_method,
                       line_count
                from log_sources
                """
            ).fetchall()

        return [
            LogSourceRecord(
                id=row[0],
                source_type=row[1],
                format_type=row[2],
                source_uri=row[3],
                create_time=datetime.fromisoformat(row[4]),
                is_extracted=bool(row[5]),
                extract_method=row[6],
                line_count=row[7])
            for row in rows
        ]