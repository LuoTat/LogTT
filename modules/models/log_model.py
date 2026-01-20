from typing import Any
from pathlib import Path
from enum import IntEnum
from datetime import datetime
from dataclasses import dataclass

from PySide6.QtCore import (
    Qt,
    Slot,
    Signal,
    QThread,
    QModelIndex, QObject
)
from PySide6.QtSql import (
    QSqlDatabase,
    QSqlTableModel
)

from modules.logparser import ParserFactory

DB_PATH = Path(__file__).resolve().parent.parent.parent / "logtt.db"


class LogSqlHeader(IntEnum):
    """日志表格头枚举"""

    ID = 0  # id
    LOG_TYPE = 1  # log_type
    FORMAT_TYPE = 2  # format_type
    LOG_URI = 3  # log_uri
    CREATE_TIME = 4  # create_time
    IS_EXTRACTED = 5  # is_extracted
    EXTRACT_METHOD = 6  # extract_method
    LINE_COUNT = 7  # line_count


class LogStatus(IntEnum):
    """日志状态枚举"""

    EXTRACTED = 0  # 已提取
    NOT_EXTRACTED = 1  # 未提取
    EXTRACTING = 2  # 提取中


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

    @Slot()
    def run(self):
        try:
            parser_type = ParserFactory.get_parser_type(self.algorithm)
            result = parser_type(
                self.log_file,
                self.log_format,
                self.regex,
                lambda: QThread.currentThread().isInterruptionRequested(),
                lambda progress: self.progress.emit(self.log_id, progress)
            ).parse()

            # 提取完成
            self.finished.emit(self.log_id, result.line_count)
        except InterruptedError:
            self.interrupted.emit(self.log_id)
        except Exception as e:
            self.error.emit(self.log_id, str(e))


@dataclass
class LogExtractTaskInfo:
    """日志提取任务信息"""

    thread: QThread
    task: LogExtractTask
    progress: int = 0


class LogSqlModel(QSqlTableModel):
    """日志数据模型 - 基于QSqlTableModel"""

    # 自定义角色
    LogIdRole = Qt.ItemDataRole.UserRole + 1
    StatusRole = Qt.ItemDataRole.UserRole + 2
    ProgressRole = Qt.ItemDataRole.UserRole + 3

    # UI 控制信号
    extractFinished = Signal(int, int)  # 提取完成 (log_id, line_count)
    extractInterrupted = Signal(int)  # 提取中断 (log_id)
    extractError = Signal(int, str)  # 提取错误 (log_id, error_message)
    addSuccess = Signal()  # 添加成功
    addDuplicate = Signal()  # 添加重复
    addError = Signal(str)  # 添加失败 (error_message)
    deleteSuccess = Signal()  # 删除成功
    deleteError = Signal(str)  # 删除失败 (error_message)

    def __init__(self, parent=None):
        # 创建数据库连接
        self._init_database()

        super().__init__(parent, self._db)
        self.setTable("log")
        self.setEditStrategy(QSqlTableModel.EditStrategy.OnManualSubmit)
        self.select()

        # 存储正在提取的任务信息: log_id -> LogExtractTaskInfo
        self._extract_tasks: dict[int, LogExtractTaskInfo] = {}

    def _init_database(self):
        """初始化数据库连接，如果表不存在则创建"""
        connection_name = "log_connection"

        if QSqlDatabase.contains(connection_name):
            self._db = QSqlDatabase.database(connection_name)
        else:
            self._db = QSqlDatabase.addDatabase("QSQLITE", connection_name)
            self._db.setDatabaseName(str(DB_PATH))

        if not self._db.isOpen():
            if not self._db.open():
                raise RuntimeError(f"无法打开数据库: {self._db.lastError().text()}")

        # 如果表不存在则创建
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        """创建 log 表（如果不存在）"""
        query = self._db.exec(
            """
            CREATE TABLE IF NOT EXISTS log
            (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                log_type       TEXT          NOT NULL,
                format_type    TEXT,
                log_uri        TEXT          NOT NULL UNIQUE,
                create_time    TEXT          NOT NULL,
                is_extracted   INT default 0 NOT NULL,
                extract_method TEXT,
                line_count     INT
            )
            """
        )
        if query.lastError().isValid():
            raise RuntimeError(f"创建表失败: {query.lastError().text()}")

    # ==================== 重写方法 ====================

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        """返回单元格数据"""
        if not index.isValid():
            return None

        # 显示角色
        if role == Qt.ItemDataRole.DisplayRole:
            return super().data(index, role)
        # 对齐角色
        elif role == Qt.ItemDataRole.TextAlignmentRole:
            return Qt.AlignmentFlag.AlignCenter
        # 自定义角色
        elif role == self.LogIdRole:
            return self._getId(index)
        elif role == self.StatusRole:
            return self._getStatus(index)
        elif role == self.ProgressRole:
            return self._getProgress(index)

        return None

    # ==================== 私有辅助方法 ====================

    def _getId(self, index: QModelIndex) -> int:
        """根据索引获取log_id"""
        return super().data(self.index(index.row(), LogSqlHeader.ID), Qt.ItemDataRole.DisplayRole)

    def _getStatus(self, index: QModelIndex) -> LogStatus:
        """获取日志状态"""
        is_extracted = super().data(self.index(index.row(), LogSqlHeader.IS_EXTRACTED), Qt.ItemDataRole.DisplayRole)
        if is_extracted:
            return LogStatus.EXTRACTED
        elif self._getId(index) in self._extract_tasks:
            return LogStatus.EXTRACTING
        else:
            return LogStatus.NOT_EXTRACTED

    def _getProgress(self, index: QModelIndex) -> int:
        """获取提取进度"""
        is_extracted = super().data(self.index(index.row(), LogSqlHeader.IS_EXTRACTED), Qt.ItemDataRole.DisplayRole)
        if is_extracted:
            return 100
        elif log_id := self._getId(index) in self._extract_tasks:
            return self._extract_tasks[log_id].progress
        return 0

    def _getRow(self, log_id: int) -> int | None:
        """根据log_id获取行号"""
        for row in range(self.rowCount()):
            if self._getId(self.index(row, 0)) == log_id:
                return row
        return None

    def _interruptExtractTask(self, index: QModelIndex):
        """中断提取任务"""
        log_id = self._getId(index)
        if log_id not in self._extract_tasks:
            return

        task_info = self._extract_tasks.pop(log_id)
        task_info.thread.requestInterruption()
        task_info.thread.quit()
        task_info.thread.wait()

    # ==================== 提取回调方法 ====================

    @Slot(int, int)
    def _onExtractFinished(self, log_id: int, line_count: int):
        """处理提取完成"""
        # 清理任务信息
        if log_id in self._extract_tasks:
            task_info = self._extract_tasks.pop(log_id)
            task_info.thread.quit()
            task_info.thread.wait()

        # 更新数据库
        row = self._getRow(log_id)
        self.setData(self.index(row, LogSqlHeader.IS_EXTRACTED), 1)
        self.setData(self.index(row, LogSqlHeader.LINE_COUNT), line_count)
        self.submitAll()

        # 发出完成信号
        self.extractFinished.emit(log_id, line_count)

    @Slot(int)
    def _onExtractInterrupted(self, log_id: int):
        """处理提取中断"""
        # 清理任务信息
        if log_id in self._extract_tasks:
            task_info = self._extract_tasks.pop(log_id)
            task_info.thread.quit()
            task_info.thread.wait()

        # 发出中断信号
        self.extractInterrupted.emit(log_id)

    @Slot(int, str)
    def _onExtractErrored(self, log_id: int, error_msg: str):
        """处理提取错误"""
        # 清理任务信息
        if log_id in self._extract_tasks:
            task_info = self._extract_tasks.pop(log_id)
            task_info.thread.quit()
            task_info.thread.wait()

        # 发出错误信号
        self.extractError.emit(log_id, error_msg)

    @Slot(int, int)
    def _onExtractProgress(self, log_id: int, progress: int):
        """处理提取进度"""
        self._extract_tasks[log_id].progress = progress

    # ==================== 数据操作方法 ====================

    def requestAdd(self, log_type: str, log_uri: str, extract_method: str | None = None):
        """请求添加日志记录"""
        # 在第一行插入新记录
        self.insertRow(0)
        self.setData(self.index(0, LogSqlHeader.LOG_TYPE), log_type)
        self.setData(self.index(0, LogSqlHeader.LOG_URI), log_uri)
        self.setData(self.index(0, LogSqlHeader.CREATE_TIME), datetime.now().isoformat(timespec="seconds"))
        if extract_method:
            self.setData(self.index(0, LogSqlHeader.EXTRACT_METHOD), extract_method)

        if self.submitAll():
            self.select()  # 同步ID
            self.addSuccess.emit()
        else:
            error = self.lastError()
            self.revertAll()
            if error.nativeErrorCode() == 2067:
                self.addDuplicate.emit()
            else:
                self.addError.emit(error.text())

    def requestDelete(self, index: QModelIndex):
        """请求删除日志记录"""

        # 如果有正在提取的任务，先中断
        self._interruptExtractTask(index)

        self.removeRow(index.row())
        if self.submitAll():
            self.deleteSuccess.emit()
        else:
            self.revertAll()
            self.deleteError.emit(self.lastError().text())

    def requestExtract(self, index: QModelIndex, algorithm: str, format_type: str, log_format: str, regex: list[str]):
        """请求提取日志"""
        row = index.row()
        # 更新日志格式和提取方法到数据库
        self.setData(self.index(row, LogSqlHeader.FORMAT_TYPE), format_type)
        self.setData(self.index(row, LogSqlHeader.EXTRACT_METHOD), algorithm)
        self.submitAll()

        log_id = self._getId(index)
        # 创建提取任务
        task = LogExtractTask(
            log_id,
            Path(super().data(self.index(row, LogSqlHeader.LOG_URI), Qt.ItemDataRole.DisplayRole)),
            algorithm,
            format_type,
            log_format,
            regex
        )

        # 创建工作线程
        thread = QThread()

        # 保存任务信息
        task_info = LogExtractTaskInfo(thread, task)
        self._extract_tasks[log_id] = task_info

        # 连接信号
        thread.started.connect(task.run)
        task.finished.connect(self._onExtractFinished)
        task.interrupted.connect(self._onExtractInterrupted)
        task.error.connect(self._onExtractErrored)
        task.progress.connect(self._onExtractProgress)

        # 启动线程
        thread.start()

    def requestInterruptExtract(self, index: QModelIndex):
        """请求中断提取任务"""
        self._interruptExtractTask(index)

    def refresh(self):
        """刷新模型数据"""
        self.select()

    def hasExtractingTasks(self) -> bool:
        """是否有正在提取的任务"""
        return len(self._extract_tasks) > 0

    def interruptAllExtractTasks(self):
        """中断所有正在提取的任务"""
        for row in range(self.rowCount()):
            self._interruptExtractTask(self.index(row, 0))