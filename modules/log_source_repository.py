from typing import List
from datetime import datetime

from .db import get_connection
from .log_source_record import LogSourceRecord


class LogSourceRepository:
    """Log源数据库操作类"""

    def __init__(self):
        self._init_table()

    def _init_table(self):
        with get_connection() as conn:
            conn.execute(
                """
                create table if not exists log_sources
                (
                    id             INTEGER primary key autoincrement,
                    source_type    TEXT not null,
                    source_uri     TEXT not null unique,
                    create_time    TEXT not null,
                    is_extracted   INT  not null,
                    extract_method TEXT,
                    line_count     INT
                )
                """
            )
            conn.commit()

    def add(self, record: LogSourceRecord):
        with get_connection() as conn:
            conn.execute(
                """
                insert into log_sources (source_type, source_uri, create_time, is_extracted, extract_method, line_count)
                values (?, ?, ?, ?, ?, ?)
                """,
                [
                    record.source_type,
                    record.source_uri,
                    record.create_time.isoformat(timespec="seconds"),
                    record.is_extracted,
                    record.extract_method,
                    record.line_count
                ]
            )
            conn.commit()

    def delete(self, id: int):
        with get_connection() as conn:
            conn.execute(
                """
                delete
                from log_sources
                where id = ?
                """,
                [id]
            )
            conn.commit()

    def get(self, id: int) -> LogSourceRecord | None:
        with get_connection() as conn:
            row = conn.execute(
                """
                select id, source_type, source_uri, create_time, is_extracted, extract_method, line_count
                from log_sources
                where id = ?
                """,
                [id]
            ).fetchone()

        if row:
            return LogSourceRecord(
                id=row[0],
                source_type=row[1],
                source_uri=row[2],
                create_time=datetime.fromisoformat(row[3]),
                is_extracted=bool(row[4]),
                extract_method=row[5],
                line_count=row[6],
            )
        return None

    def get_all(self) -> List[LogSourceRecord]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                select id, source_type, source_uri, create_time, is_extracted, extract_method, line_count
                from log_sources
                """
            ).fetchall()

        return [
            LogSourceRecord(
                id=row[0],
                source_type=row[1],
                source_uri=row[2],
                create_time=datetime.fromisoformat(row[3]),
                is_extracted=bool(row[4]),
                extract_method=row[5],
                line_count=row[6],
            )
            for row in rows
        ]