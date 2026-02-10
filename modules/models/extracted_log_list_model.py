from enum import IntEnum
from typing import Any

from PySide6.QtCore import QAbstractListModel, QModelIndex, Qt

from modules.duckdb_service import DuckDBService


class SqlColumn(IntEnum):
    """日志表数据库列枚举"""

    ID = 0  # id
    LOG_URI = 1  # log_uri
    STRUCTURED_TABLE_NAME = 2  # structured_table_name
    TEMPLATES_TABLE_NAME = 3  # templates_table_name


class ExtractedLogListModel(QAbstractListModel):
    """已提取日志的模型类"""

    # 用户自定义角色
    LOG_ID_ROLE = Qt.ItemDataRole.UserRole + 1  # 日志ID
    STRUCTURED_TABLE_NAME_ROLE = Qt.ItemDataRole.UserRole + 2  # 结构化表名
    TEMPLATES_TABLE_NAME_ROLE = Qt.ItemDataRole.UserRole + 3  # 模板表名

    def __init__(self, parent=None):
        super().__init__(parent)

        self._duckdb_service = DuckDBService()
        # 一次性获取整个表的数据到内存中
        self._df: list[tuple] = self._duckdb_service.get_extracted_log_table()

    # ==================== 重写方法 ====================

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._df)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None

        row = index.row()

        # 显示角色
        if role in (Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole):
            return self._df[row][SqlColumn.LOG_URI]

        # 自定义角色：返回各种数据
        elif role == self.LOG_ID_ROLE:
            return int(self._df[row][SqlColumn.ID])

        elif role == self.STRUCTURED_TABLE_NAME_ROLE:
            return self._df[row][SqlColumn.STRUCTURED_TABLE_NAME]

        elif role == self.TEMPLATES_TABLE_NAME_ROLE:
            return self._df[row][SqlColumn.TEMPLATES_TABLE_NAME]

        return None

    # ==================== 公共方法 ====================

    def get_row(self, log_id: int) -> int:
        """根据 log_id 获取行号"""
        for idx, row in enumerate(self._df):
            if row[SqlColumn.ID] == log_id:
                return idx
        return -1

    def refresh(self):
        """刷新数据"""
        self.beginResetModel()
        self._df = self._duckdb_service.get_extracted_log_table()
        self.endResetModel()
