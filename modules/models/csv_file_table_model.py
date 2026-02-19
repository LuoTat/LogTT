from typing import Any

import polars
from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor

from modules.duckdb_service import DuckDBService


class CsvFileTableModel(QAbstractTableModel):
    """专门用来显示DuckDB数据库中保存的csv文件的表格模型，支持分页加载、排序和过滤"""

    # 每次加载的页数
    _WINDOW_PAGES = 5
    # 每页的行数
    _PAGE_SIZE = 200

    def __init__(self, table_name: str, parent=None):
        super().__init__(parent)

        # 表的元信息
        self._table_name = table_name
        self._columns = DuckDBService.get_table_columns(table_name)
        self._total_row_count = DuckDBService.get_table_row_count(table_name)

        # 缓存的DataFrame
        self._cache_df: polars.DataFrame = polars.DataFrame()
        self._cache_offset: int = 0
        self._cache_limit: int = 0

        # 过滤的状态
        self._filters: dict[str, list[str]] = {}
        self._filtered_row_count: int

        # 预加载第一页数据，主要是为了提前获取总行数
        self._cache_row_data(0)

    # ==================== 重写方法 ====================

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return self._filtered_row_count

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._columns)

    def headerData(
        self,
        section: int,
        orientation: Qt.Orientation,
        role: int = Qt.ItemDataRole.DisplayRole,
    ) -> Any:
        if (
            orientation == Qt.Orientation.Horizontal
            and role == Qt.ItemDataRole.DisplayRole
        ):
            return self._columns[section]
        return None

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if not (self._cache_offset <= row < self._cache_offset + self._cache_limit):
            self._cache_row_data(row)

        if role == Qt.ItemDataRole.DisplayRole:
            return self._cache_df.item(row - self._cache_offset, col)

        # 处理前景色角色
        elif role == Qt.ItemDataRole.ForegroundRole:
            column_name = self._columns[col]
            if column_name in self._filters:
                return QColor(Qt.GlobalColor.cyan)
            return None

        return None

    # ==================== 私有方法 ====================

    def _cache_row_data(self, row: int):
        """将目标行左右的数据缓存到本地"""
        self._cache_limit = self._PAGE_SIZE * self._WINDOW_PAGES
        self._cache_offset = max(0, row - self._PAGE_SIZE)

        try:
            self._cache_df, self._filtered_row_count = DuckDBService.fetch_csv_table(
                self._table_name,
                self._cache_offset,
                self._cache_limit,
                self._filters,
            )
        except Exception as e:
            print(f"Error fetching data: {e}")

    # ==================== 公共方法 ====================
    def table_name(self) -> str:
        """获取当前表名"""
        return self._table_name

    def total_row_count(self) -> int:
        """获取总行数"""
        return self._total_row_count

    def filtered_row_count(self) -> int:
        """获取过滤后的行数"""
        return self._filtered_row_count

    def is_column_filtered(self, column_name: str) -> bool:
        """检查列是否有过滤"""
        return column_name in self._filters

    def get_all_filters(self) -> dict[str, list[str]]:
        """获取所有列的过滤值"""
        return self._filters.copy()

    def get_column_filter(self, column_name: str) -> list[str]:
        """获取列的过滤值"""
        return self._filters.get(column_name, [])

    def set_column_filter(self, column_name: str, values: list[str]):
        """设置列的过滤值"""
        self._filters[column_name] = values
        self.refresh()

    def clear_column_filter(self, column_name: str):
        """清除列的过滤"""
        self._filters.pop(column_name)
        self.refresh()

    def clear_all_filters(self):
        """清除所有过滤"""
        self._filters.clear()
        self.refresh()

    def refresh(self):
        """刷新模型数据"""
        self.beginResetModel()
        self._cache_row_data(0)
        self.endResetModel()
