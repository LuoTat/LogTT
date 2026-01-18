from typing import Any
from enum import IntEnum
from datetime import datetime

from PyQt6.QtCore import Qt, QModelIndex, QAbstractTableModel, pyqtSignal

from modules.log_source_record import LogSourceRecord


class LogSourceColumn(IntEnum):
    """日志源表格头枚举"""

    SOURCE_TYPE = 0  # 类型
    FORMAT_TYPE = 1  # 日志格式
    SOURCE_URI = 2  # URI
    CREATE_TIME = 3  # 创建时间
    EXTRACT_METHOD = 4  # 提取方法
    LINE_COUNT = 5  # 行数
    PROGRESS = 6  # 进度条
    ACTIONS = 7  # 操作


class LogSourceItem:
    """日志源数据项，包含记录和运行时状态"""

    def __init__(self, log: LogSourceRecord):
        self.log = log
        self.progress: int | None = None  # 提取进度 (0-100)，None 表示未在提取中

    @property
    def id(self) -> int:
        return self.log.id

    @property
    def source_type(self) -> str:
        return self.log.source_type

    @property
    def format_type(self) -> str | None:
        return self.log.format_type

    @property
    def source_uri(self) -> str:
        return self.log.source_uri

    @property
    def create_time(self) -> datetime:
        return self.log.create_time

    @property
    def is_extracted(self) -> bool:
        return self.log.is_extracted

    @property
    def extract_method(self) -> str | None:
        return self.log.extract_method

    @property
    def line_count(self) -> int | None:
        return self.log.line_count


class LogSourceTableModel(QAbstractTableModel):
    """日志源数据项表格模型"""

    # 自定义角色
    LogIdRole = Qt.ItemDataRole.UserRole + 1
    IsExtractedRole = Qt.ItemDataRole.UserRole + 2
    ProgressRole = Qt.ItemDataRole.UserRole + 3
    LogSourceItemRole = Qt.ItemDataRole.UserRole + 4

    # 操作信号
    extract = pyqtSignal(int)  # 请求提取
    viewLog = pyqtSignal(int)  # 请求查看日志
    viewTemplate = pyqtSignal(int)  # 请求查看模板
    delete = pyqtSignal(int)  # 请求删除

    HEADERS = ["类型", "日志格式", "URI", "创建时间", "提取方法", "行数", "进度", "操作"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.items: list[LogSourceItem] = []
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
            if col == LogSourceColumn.SOURCE_TYPE:
                return item.source_type
            elif col == LogSourceColumn.FORMAT_TYPE:
                return item.format_type if item.format_type else "—"
            elif col == LogSourceColumn.SOURCE_URI:
                return item.source_uri
            elif col == LogSourceColumn.CREATE_TIME:
                return item.create_time.isoformat(" ", "seconds")
            elif col == LogSourceColumn.EXTRACT_METHOD:
                return item.extract_method if item.extract_method else "—"
            elif col == LogSourceColumn.LINE_COUNT:
                return f"{item.line_count:,}" if item.line_count is not None else "—"
            elif col == LogSourceColumn.PROGRESS:
                return None  # 进度条由 delegate 绘制
            elif col == LogSourceColumn.ACTIONS:
                return None  # 操作按钮由 delegate 绘制
        # 对齐角色
        elif role == Qt.ItemDataRole.TextAlignmentRole:
            return Qt.AlignmentFlag.AlignCenter
        # 自定义角色
        elif role == self.LogIdRole:
            return item.id
        elif role == self.IsExtractedRole:
            return item.is_extracted
        elif role == self.ProgressRole:
            return item.progress
        elif role == self.LogSourceItemRole:
            return item
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:
        if not index.isValid():
            return Qt.ItemFlag.NoItemFlags
        return Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable

    # ==================== 数据操作方法 ====================

    def setLogSources(self, logs: list[LogSourceRecord], log_progress: dict[int, int] | None = None):
        """设置日志源列表"""
        self.beginResetModel()
        self.items.clear()
        self.id_to_row.clear()

        for idx, log in enumerate(logs):
            item = LogSourceItem(log)

            if log_progress and log.id in log_progress:
                item.progress = log_progress[log.id]

            self.items.append(item)
            self.id_to_row[log.id] = idx

        self.endResetModel()

    def getItem(self, log_id: int) -> LogSourceItem | None:
        """根据ID获取数据项"""
        if log_id not in self.id_to_row:
            return None
        return self.items[self.id_to_row[log_id]]

    def getRow(self, log_id: int) -> int | None:
        """根据ID获取行号"""
        return self.id_to_row.get(log_id)

    def setLog(self, log_id: int, log: LogSourceRecord):
        """设置日志源记录"""
        if log_id not in self.id_to_row:
            return
        row = self.id_to_row[log_id]

        item = self.items[row]
        item.log = log

        # 更新整行数据
        left_index = self.index(row, 0)
        right_index = self.index(row, self.columnCount() - 1)
        self.dataChanged.emit(left_index, right_index)

    def setProgress(self, log_id: int, progress: int | None):
        """设置提取进度"""
        if log_id not in self.id_to_row:
            return
        row = self.id_to_row[log_id]

        item = self.items[row]
        item.progress = progress

        # 更新进度列
        index = self.index(row, LogSourceColumn.PROGRESS)
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