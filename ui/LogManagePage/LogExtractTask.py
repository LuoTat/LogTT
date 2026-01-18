from pathlib import Path

from PySide6.QtCore import (
    Slot,
    Signal,
    QObject,
    QThread
)

from modules.log import LogService
from modules.logparser import ParserFactory


class LogExtractTask(QObject):
    """日志提取工作线程"""

    finished = Signal(int, int)  # (log_id, line_count)
    interrupted = Signal(int)  # (log_id)
    error = Signal(int, str)  # (log_id, error_message)
    progress = Signal(int, int)  # (log_id, progress)

    def __init__(self, log_id: int, log_file: Path, algorithm: str, format_type: str, log_format: str, regex: list[str]):
        super().__init__()
        self.log_id = log_id
        self.log_file = log_file
        self.algorithm = algorithm
        self.format_type = format_type
        self.log_format = log_format
        self.regex = regex
        self.log_service = LogService()

    @Slot()
    def run(self):
        try:
            # 保存日志格式到数据库
            self.log_service.update_format_type(self.log_id, self.format_type)
            # 保存提取算法到数据库
            self.log_service.update_extract_method(self.log_id, self.algorithm)

            parser_type = ParserFactory.get_parser_type(self.algorithm)
            result = parser_type(
                self.log_file,
                self.log_format,
                self.regex,
                lambda: QThread.currentThread().isInterruptionRequested(),
                lambda progress: self.progress.emit(self.log_id, progress)
            ).parse()

            # 将日志设置为已提取
            self.log_service.update_is_extracted(self.log_id, True)
            # 保存日志行数
            self.log_service.update_line_count(self.log_id, result.line_count)

            # TODO：保存提取的模板内容

            # 提取完成
            self.finished.emit(self.log_id, result.line_count)
        except InterruptedError:
            self.interrupted.emit(self.log_id)
        except Exception as e:
            self.error.emit(self.log_id, str(e))