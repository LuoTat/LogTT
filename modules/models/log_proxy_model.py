from typing import Any
from pathlib import Path
from enum import IntEnum

from PySide6.QtGui import QColor
from PySide6.QtCore import (
    Qt,
    QModelIndex,
    QSortFilterProxyModel
)

from .log_model import LogSqlModel


class LogProxyModel(QSortFilterProxyModel):
    """日志代理模型 - 用于显示、过滤和排序"""

    # 显示的表头
    HEADERS = ["名称", "日志行数", "日志类型", "日志格式", "创建时间", "进度", "状态", "提取方法"]

    class ProxyColumn(IntEnum):
        """代理模型的列枚举"""

        NAME = 0  # 名称 -> LOG_URI
        LINE_COUNT = 1  # 日志行数 -> LINE_COUNT
        LOG_TYPE = 2  # 日志类型 -> LOG_TYPE
        FORMAT_TYPE = 3  # 日志格式 -> FORMAT_TYPE
        CREATE_TIME = 4  # 创建时间 -> CREATE_TIME
        PROGRESS = 5  # 进度 -> 虚拟列
        STATUS = 6  # 状态 -> 虚拟列
        EXTRACT_METHOD = 7  # 提取方法 -> EXTRACT_METHOD

    # 代理列到源列的映射
    _PROXY_TO_SOURCE = {
        ProxyColumn.NAME: LogSqlModel.SqlColumn.LOG_URI,
        ProxyColumn.LINE_COUNT: LogSqlModel.SqlColumn.LINE_COUNT,
        ProxyColumn.LOG_TYPE: LogSqlModel.SqlColumn.LOG_TYPE,
        ProxyColumn.FORMAT_TYPE: LogSqlModel.SqlColumn.FORMAT_TYPE,
        ProxyColumn.CREATE_TIME: LogSqlModel.SqlColumn.CREATE_TIME,
        ProxyColumn.PROGRESS: 0,  # 虚拟列
        ProxyColumn.STATUS: 0,  # 虚拟列
        ProxyColumn.EXTRACT_METHOD: LogSqlModel.SqlColumn.EXTRACT_METHOD,
    }

    # 源列到代理列的映射
    _SOURCE_TO_PROXY = {
        LogSqlModel.SqlColumn.ID: -1,
        LogSqlModel.SqlColumn.LOG_TYPE: ProxyColumn.LOG_TYPE,
        LogSqlModel.SqlColumn.FORMAT_TYPE: ProxyColumn.FORMAT_TYPE,
        LogSqlModel.SqlColumn.LOG_URI: ProxyColumn.NAME,
        LogSqlModel.SqlColumn.CREATE_TIME: ProxyColumn.CREATE_TIME,
        LogSqlModel.SqlColumn.IS_EXTRACTED: -1,
        LogSqlModel.SqlColumn.EXTRACT_METHOD: ProxyColumn.EXTRACT_METHOD,
        LogSqlModel.SqlColumn.LINE_COUNT: ProxyColumn.LINE_COUNT,
    }

    # 状态显示文本
    _STATUS_TO_TEXT = {
        LogSqlModel.LogStatus.EXTRACTED: "已提取",
        LogSqlModel.LogStatus.NOT_EXTRACTED: "未提取",
        LogSqlModel.LogStatus.EXTRACTING: "提取中",
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self._filter_keyword = ""

    # ==================== 重写方法 ====================

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """返回代理模型的列数"""
        return len(self.HEADERS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        """返回表头数据"""
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            if 0 <= section < len(self.HEADERS):
                return self.HEADERS[section]
        return None

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        """返回单元格数据"""
        if not index.isValid():
            return None

        source_model = self.sourceModel()
        if source_model is None:
            return None

        proxy_col = index.column()
        source_index = self.mapToSource(index)

        if not source_index.isValid():
            return None

        # 处理显示角色
        if role == Qt.ItemDataRole.DisplayRole:
            return self._getDisplayData(source_index, proxy_col)

        # 处理前景色角色
        elif role == Qt.ItemDataRole.ForegroundRole:
            status = source_index.data(LogSqlModel.StatusRole)
            if status == LogSqlModel.LogStatus.EXTRACTING:
                return QColor(Qt.GlobalColor.green)
            return None

        # 传递自定义角色到源模型
        elif role in (LogSqlModel.StatusRole, LogSqlModel.ProgressRole):
            return source_index.data(role)

        return super().data(index, role)

    def mapToSource(self, index: QModelIndex) -> QModelIndex:
        """将代理索引映射到源索引"""
        if not index.isValid():
            return QModelIndex()

        source_model = self.sourceModel()
        if source_model is None:
            return QModelIndex()

        # 获取源模型的行（通过父类处理排序过滤后的行映射）
        source_row = super().mapToSource(index).row()

        # 列映射
        source_col = self._PROXY_TO_SOURCE[self.ProxyColumn(index.column())]
        return source_model.index(source_row, source_col)

    def mapFromSource(self, index: QModelIndex) -> QModelIndex:
        """将源索引映射到代理索引 """
        if not index.isValid():
            return QModelIndex()

        # 获取代理模型的行（通过父类处理排序过滤后的行映射）
        proxy_row = super().mapFromSource(index).row()

        # 反向列映射
        proxy_col = self._SOURCE_TO_PROXY[LogSqlModel.SqlColumn(index.column())]
        return self.index(proxy_row, proxy_col)

    def filterAcceptsRow(self, source_row: int, source_parent: QModelIndex) -> bool:
        """过滤行 - 根据名称关键字过滤"""
        if not self._filter_keyword:
            return True

        source_model = self.sourceModel()
        if source_model is None:
            return True

        # 获取 LOG_URI 列的数据
        source_index = source_model.index(source_row, LogSqlModel.SqlColumn.LOG_URI)
        name = self._getDisplayData(source_index, LogProxyModel.ProxyColumn.NAME)

        # 不区分大小写的关键字搜索
        return self._filter_keyword.lower() in name.lower()

    def lessThan(self, left: QModelIndex, right: QModelIndex) -> bool:
        """比较两个索引的数据用于排序"""
        source_model = self.sourceModel()
        if source_model is None:
            return False

        proxy_col = self.sortColumn()
        # 虚拟列的排序
        if proxy_col == self.ProxyColumn.PROGRESS:
            left_progress = left.data(LogSqlModel.ProgressRole)
            right_progress = right.data(LogSqlModel.ProgressRole)
            return left_progress < right_progress

        elif proxy_col == self.ProxyColumn.STATUS:
            left_status = left.data(LogSqlModel.StatusRole)
            right_status = right.data(LogSqlModel.StatusRole)
            return left_status < right_status

        # 常规列的排序
        source_col = self._PROXY_TO_SOURCE[self.ProxyColumn(proxy_col)]
        left_data = source_model.index(left.row(), source_col).data(Qt.ItemDataRole.DisplayRole)
        right_data = source_model.index(right.row(), source_col).data(Qt.ItemDataRole.DisplayRole)

        # 处理 None 值
        if left_data in (None, "") and right_data in (None, ""):
            return False
        if left_data in (None, ""):
            return True
        if right_data in (None, ""):
            return False

        return left_data < right_data

    # ==================== 私有辅助方法 ====================

    def _getDisplayData(self, source_index: QModelIndex, proxy_col: int) -> Any:
        """获取显示数据"""
        source_model = self.sourceModel()
        if source_model is None:
            return None

        source_row = source_index.row()

        # 名称列
        if proxy_col == self.ProxyColumn.NAME:
            uri = source_model.index(source_row, LogSqlModel.SqlColumn.LOG_URI).data(Qt.ItemDataRole.DisplayRole)
            return Path(uri).name

        # 进度列
        # 使用委托显示进度条，因此这里返回 None
        elif proxy_col == self.ProxyColumn.PROGRESS:
            return None

        # 状态列
        elif proxy_col == self.ProxyColumn.STATUS:
            status = source_model.index(source_row, 0).data(LogSqlModel.StatusRole)
            return self._STATUS_TO_TEXT[status]

        # 常规列
        source_col = source_index.column()
        return source_model.index(source_row, source_col).data(Qt.ItemDataRole.DisplayRole)

    # ==================== 公共方法 ====================

    def searchByName(self, keyword: str):
        """按 URI 关键字搜索"""
        self._filter_keyword = keyword.strip()
        self.invalidateFilter()

    def clearSearch(self):
        """清除搜索"""
        self._filter_keyword = ""
        self.invalidateFilter()