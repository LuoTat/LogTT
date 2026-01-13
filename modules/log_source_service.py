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
        """更新日志源的格式类型"""
        # TODO: 实现更新逻辑
        pass

    def update_extract_status(self, log_source_id: int, extract_method: str, line_count: int, format_type: str):
        """更新日志源的提取状态"""
        # TODO: 实现更新逻辑
        pass