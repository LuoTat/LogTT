from pathlib import Path
from datetime import datetime

from .log_source_record import LogSourceRecord
from .log_source_repository import LogSourceRepository


class LogSourceService:
    """Log源数据库的业务接口"""

    def __init__(self):
        self.repo = LogSourceRepository()

    def add_local_log(self, file_path: str):
        record = LogSourceRecord(
            id=-1,
            source_type="本地文件",
            source_uri=Path(file_path).resolve().as_uri(),
            create_time=datetime.now(),
            is_extracted=False,
            extract_method=None,
            line_count=None
        )
        self.repo.add(record)

    def add_network_log(self, url: str):
        record = LogSourceRecord(
            id=-1,
            source_type="网络地址",
            source_uri=url,
            create_time=datetime.now(),
            is_extracted=False,
            extract_method="Drain3",
            line_count=None
        )
        self.repo.add(record)

    def delete_log(self, id: int):
        self.repo.delete(id)

    def get_log(self, id: int):
        return self.repo.get(id)

    def get_all_logs(self):
        return self.repo.get_all()