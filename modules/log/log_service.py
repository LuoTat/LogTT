from pathlib import Path
from datetime import datetime

from .log_record import LogRecord
from .log_repository import LogRepository


class LogService:
    """Log数据库的业务接口"""

    def __init__(self):
        self.repo = LogRepository()

    def add_local_log(self, file_path: str):
        log = LogRecord(
            id=-1,
            log_type="本地文件",
            log_uri=Path(file_path).resolve().as_posix(),
            create_time=datetime.now(),
            is_extracted=False,
            extract_method=None,
            line_count=None,
            format_type=None
        )
        self.repo.add(log)

    def add_network_log(self, url: str):
        log = LogRecord(
            id=-1,
            log_type="网络地址",
            log_uri=url,
            create_time=datetime.now(),
            is_extracted=False,
            extract_method="Drain3",
            line_count=None,
            format_type=None
        )
        self.repo.add(log)

    def delete_log(self, log_id: int):
        self.repo.delete(log_id)

    def get_log(self, log_id: int) -> LogRecord | None:
        return self.repo.get(log_id)

    def get_all_logs(self) -> list[LogRecord]:
        return self.repo.get_all()

    def update_format_type(self, log_id: int, format_type: str):
        self.repo.update_format_type(log_id, format_type)

    def update_extract_method(self, log_id: int, extract_method: str):
        self.repo.update_extract_method(log_id, extract_method)

    def update_is_extracted(self, log_id: int, is_extracted: bool):
        self.repo.update_is_extracted(log_id, is_extracted)

    def update_line_count(self, log_id: int, line_count: int):
        self.repo.update_line_count(log_id, line_count)

    def search_by_uri(self, keyword: str) -> list[LogRecord]:
        return self.repo.search_by_uri(keyword)