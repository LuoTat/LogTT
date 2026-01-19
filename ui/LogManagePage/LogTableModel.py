from typing import Any
from pathlib import Path
from enum import IntEnum
from datetime import datetime

from PySide6.QtCore import (
    Qt,
    Signal,
    QModelIndex,
    QAbstractTableModel
)

from modules.log import LogRecord


class LogTableHeader(IntEnum):
    """日志表格头枚举"""

    NAME = 0  # 名称
    LINE_COUNT = 1  # 日志行数
    LOG_TYPE = 2  # 日志类型
    FORMAT_TYPE = 3  # 日志格式
    CREATE_TIME = 4  # 创建时间
    PROGRESS = 5  # 进度条
    STATUS = 6  # 状态
    EXTRACT_METHOD = 7  # 提取方法


class LogStatus(IntEnum):
    """日志状态枚举"""

    EXTRACTED = 0  # 已提取
    NOT_EXTRACTED = 1  # 未提取
    EXTRACTING = 2  # 提取中


class LogItem:
    """日志数据项，包含记录和运行时状态"""

    def __init__(self, log: LogRecord):
        self.log = log
        self.status = LogStatus.EXTRACTED if log.is_extracted else LogStatus.NOT_EXTRACTED
        self.progress: int = 0  # 提取进度 (0-100)

    @property
    def id(self) -> int:
        return self.log.id

    @property
    def log_type(self) -> str:
        return self.log.log_type

    @property
    def format_type(self) -> str | None:
        return self.log.format_type

    @property
    def log_uri(self) -> str:
        return self.log.log_uri

    @property
    def create_time(self) -> datetime:
        return self.log.create_time

    @property
    def extract_method(self) -> str | None:
        return self.log.extract_method

    @property
    def line_count(self) -> int | None:
        return self.log.line_count


class LogTableModel(QAbstractTableModel):
    """日志数据项表格模型"""

    # 自定义角色
    LogIdRole = Qt.ItemDataRole.UserRole + 1
    StatusRole = Qt.ItemDataRole.UserRole + 2
    ProgressRole = Qt.ItemDataRole.UserRole + 3
    LogItemRole = Qt.ItemDataRole.UserRole + 4

    # 操作信号
    extract = Signal(int)  # 请求提取
    viewLog = Signal(int)  # 请求查看日志
    viewTemplate = Signal(int)  # 请求查看模板
    delete = Signal(int)  # 请求删除

    HEADERS = ["名称", "日志行数", "日志类型", "日志格式", "创建时间", "进度", "状态", "提取方法"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.items: list[LogItem] = []
        self.id_to_row: dict[int, int] = {}

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self.items)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self.HEADERS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> str | None:
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            if 0 <= section < len(self.HEADERS):
                return self.HEADERS[section]
        return None

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if row < 0 or row >= len(self.items):
            return None

        item = self.items[row]

        # 显示角色
        if role == Qt.ItemDataRole.DisplayRole:
            if col == LogTableHeader.NAME:
                return Path(item.log_uri).name
            elif col == LogTableHeader.LINE_COUNT:
                return f"{item.line_count:,}" if item.line_count is not None else "—"
            elif col == LogTableHeader.LOG_TYPE:
                return item.log_type
            elif col == LogTableHeader.FORMAT_TYPE:
                return item.format_type if item.format_type else "—"
            elif col == LogTableHeader.CREATE_TIME:
                return item.create_time.isoformat(" ", "seconds")
            elif col == LogTableHeader.PROGRESS:
                return None  # 进度条由 delegate 绘制
            elif col == LogTableHeader.STATUS:
                if item.status == LogStatus.EXTRACTED:
                    return "已提取"
                elif item.status == LogStatus.NOT_EXTRACTED:
                    return "未提取"
                else:
                    return "提取中"
            elif col == LogTableHeader.EXTRACT_METHOD:
                return item.extract_method if item.extract_method else "—"
        # 对齐角色
        elif role == Qt.ItemDataRole.TextAlignmentRole:
            return Qt.AlignmentFlag.AlignCenter
        # 自定义角色
        elif role == self.LogIdRole:
            return item.id
        elif role == self.StatusRole:
            return item.status
        elif role == self.ProgressRole:
            return item.progress
        elif role == self.LogItemRole:
            return item
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:
        if not index.isValid():
            return Qt.ItemFlag.NoItemFlags
        return Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable

    # ==================== 数据操作方法 ====================

    def setLogs(self, logs: list[LogRecord], log_progress: dict[int, int] | None = None):
        """设置日志列表"""
        self.beginResetModel()
        self.items.clear()
        self.id_to_row.clear()

        for idx, log in enumerate(logs):
            item = LogItem(log)

            if item.status == LogStatus.EXTRACTED:
                item.progress = 100
            elif log_progress and log.id in log_progress:
                item.status = LogStatus.EXTRACTING
                item.progress = log_progress[log.id]

            self.items.append(item)
            self.id_to_row[log.id] = idx

        self.endResetModel()

    def getLog(self, log_id: int) -> LogItem | None:
        """根据ID获取数据项"""
        if log_id not in self.id_to_row:
            return None
        return self.items[self.id_to_row[log_id]]

    def getRow(self, log_id: int) -> int | None:
        """根据ID获取行号"""
        return self.id_to_row.get(log_id)

    def addLog(self, log: LogRecord):
        """添加日志记录"""
        self.beginInsertRows(QModelIndex(), len(self.items), len(self.items))
        item = LogItem(log)
        self.items.append(item)
        self.id_to_row[log.id] = len(self.items) - 1
        self.endInsertRows()

    def setLog(self, log_id: int, log: LogRecord):
        """设置日志记录"""
        if log_id not in self.id_to_row:
            return
        row = self.id_to_row[log_id]

        item = self.items[row]
        item.log = log

        # 更新整行数据
        left_index = self.index(row, 0)
        right_index = self.index(row, self.columnCount() - 1)
        self.dataChanged.emit(left_index, right_index)

    def setProgress(self, log_id: int, progress: int):
        """设置提取进度"""
        if log_id not in self.id_to_row:
            return
        row = self.id_to_row[log_id]

        item = self.items[row]
        item.progress = progress

        # 更新进度列
        index = self.index(row, LogTableHeader.PROGRESS)
        self.dataChanged.emit(index, index)

    def remove(self, log_id: int):
        """根据ID移除数据项"""
        if log_id not in self.id_to_row:
            return
        row = self.id_to_row[log_id]

        self.beginRemoveRows(QModelIndex(), row, row)
        del self.items[row]
        # 重建索引映射
        self.id_to_row.clear()
        for idx, item in enumerate(self.items):
            self.id_to_row[item.id] = idx
        self.endRemoveRows()

    def clear(self):
        """清空所有数据"""
        self.beginResetModel()
        self.items.clear()
        self.id_to_row.clear()
        self.endResetModel()

    # ==================== 操作触发方法 ====================

    def requestExtract(self, log_id: int):
        """触发提取请求信号"""
        self.extract.emit(log_id)

    def requestViewLog(self, log_id: int):
        """触发查看日志请求信号"""
        self.viewLog.emit(log_id)

    def requestViewTemplate(self, log_id: int):
        """触发查看模板请求信号"""
        self.viewTemplate.emit(log_id)

    def requestDelete(self, log_id: int):
        """触发删除请求信号"""
        self.delete.emit(log_id)