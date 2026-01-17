from pathlib import Path
from datetime import datetime

from .log_source_record import LogSourceRecord
from .log_source_repository import LogSourceRepository


class LogSourceService:
    """Log源数据库的业务接口"""

    def __init__(self):
        self.repo = LogSourceRepository()

    def add_local_log(self, file_path: str):
        log_source = LogSourceRecord(
            id=-1,
            source_type="本地文件",
            source_uri=Path(file_path).resolve().as_posix(),
            create_time=datetime.now(),
            is_extracted=False,
            extract_method=None,
            line_count=None,
            format_type=None
        )
        self.repo.add(log_source)

    def add_network_log(self, url: str):
        log_source = LogSourceRecord(
            id=-1,
            source_type="网络地址",
            source_uri=url,
            create_time=datetime.now(),
            is_extracted=False,
            extract_method="Drain3",
            line_count=None,
            format_type=None
        )
        self.repo.add(log_source)

    def delete_log(self, log_source_id: int):
        self.repo.delete(log_source_id)

    def get_log(self, log_source_id: int) -> LogSourceRecord | None:
        return self.repo.get(log_source_id)

    def get_all_logs(self) -> list[LogSourceRecord]:
        return self.repo.get_all()

    def update_format_type(self, log_source_id: int, format_type: str):
        self.repo.update_format_type(log_source_id, format_type)

    def update_extract_method(self, log_source_id: int, extract_method: str):
        self.repo.update_extract_method(log_source_id, extract_method)

    def update_is_extracted(self, log_source_id: int, is_extracted: bool):
        self.repo.update_is_extracted(log_source_id, is_extracted)

    def update_line_count(self, log_source_id: int, line_count: int):
        self.repo.update_line_count(log_source_id, line_count)

    def search_by_uri(self, keyword: str) -> list[LogSourceRecord]:
        return self.repo.search_by_uri(keyword)