from pathlib import Path

from PyQt6.QtCore import (
    QObject,
    QThread,
    pyqtSlot,
    pyqtSignal
)

from modules.logparser import ParserFactory
from modules.log_source_service import LogSourceService


class LogExtractTask(QObject):
    """日志提取工作线程"""

    finished = pyqtSignal(int)  # 提取完成信号，参数为行数
    error = pyqtSignal(str)  # 提取失败信号，参数为错误信息
    progress = pyqtSignal(int)  # 进度信号，参数为进度值 (0-100)

    def __init__(self, log_id: int, log_file: Path, algorithm: str, format_type: str, log_format: str, regex: list[str]):
        super().__init__()
        self.log_id = log_id
        self.log_file = log_file
        self.algorithm = algorithm
        self.format_type = format_type
        self.log_format = log_format
        self.regex = regex
        self.log_source_service = LogSourceService()

    @pyqtSlot()
    def run(self):
        try:
            # 保存日志格式到数据库
            self.log_source_service.update_format_type(self.log_id, self.format_type)
            # 保存提取算法到数据库
            self.log_source_service.update_extract_method(self.log_id, self.algorithm)

            parser_type = ParserFactory.get_parser_type(self.algorithm)
            result = parser_type(
                self.log_file,
                self.log_format,
                self.regex,
                lambda: QThread.currentThread().isInterruptionRequested(),
                self.progress.emit
            ).parse()

            # 将日志设置为已提取
            self.log_source_service.update_is_extracted(self.log_id, True)
            # 保存日志行数
            self.log_source_service.update_line_count(self.log_id, result.line_count)

            # TODO：保存提取的模板内容

            # 提取完成
            self.finished.emit(result.line_count)
        except InterruptedError as e:
            print(f"Log extraction interrupted: {self.log_file}")
        except Exception as e:
            self.error.emit(str(e))