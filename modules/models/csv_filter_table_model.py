from typing import Any

import polars
from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt, Signal

from modules.duckdb_service import DuckDBService


class CsvFilterTableModel(QAbstractTableModel):
    """专门用来显示csv文件的列聚合的模型，支持分页加载、排序和关键字过滤"""

    # 每次加载的页数
    _WINDOW_PAGES = 5
    # 每页的行数
    _PAGE_SIZE = 20

    # 显示的表头：值列同时显示复选框和值
    _TABLE_HEADERS = ["值", "计数"]

    # UI 控制信号
    filterChanged = Signal(object)  # 传递当前选中的值列表

    def __init__(
        self,
        table_name: str,
        column_name: str,
        all_filters: dict[str, list[str]],
        parent=None,
    ):
        super().__init__(parent)

        self._duckdb_service = DuckDBService()

        # 表的元信息
        self._table_name = table_name
        self._column_name = column_name
        self._total_row_count: int

        # 缓存的DataFrame
        self._cache_df: polars.DataFrame = polars.DataFrame()
        self._cache_offset: int = 0
        self._cache_limit: int = 0

        # 关键字过滤状态
        self._keyword: str = str()

        # 当前已选中的值集合
        self._current_filter = all_filters.get(column_name, [])
        # 去除当前列的其他过滤条件
        if column_name in all_filters:
            all_filters.pop(column_name)
        self._other_filters = all_filters

        # 预加载第一页数据，主要是为了提前获取总行数
        self._cacheRowData(0)

    # ==================== 重写方法 ====================

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return self._total_row_count

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._TABLE_HEADERS)

    def headerData(
        self,
        section: int,
        orientation: Qt.Orientation,
        role: int = Qt.ItemDataRole.DisplayRole,
    ) -> Any:
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self._TABLE_HEADERS[section]
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:
        if not index.isValid():
            return Qt.ItemFlag.NoItemFlags

        base_flags = super().flags(index)
        if index.column() == 0:
            return base_flags | Qt.ItemFlag.ItemIsUserCheckable

        return base_flags

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if not (self._cache_offset <= row < self._cache_offset + self._cache_limit):
            self._cacheRowData(row)

        if role == Qt.ItemDataRole.DisplayRole:
            return str(self._cache_df.item(row - self._cache_offset, col))

        if role == Qt.ItemDataRole.CheckStateRole:
            if col != 0:
                return None
            row_value = str(self._cache_df.item(row - self._cache_offset, 0))
            return Qt.CheckState.Checked if row_value in self._current_filter else Qt.CheckState.Unchecked

        return None

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.ItemDataRole.EditRole) -> bool:
        if not index.isValid():
            return False

        if role == Qt.ItemDataRole.CheckStateRole:
            row_value = str(self._cache_df.item(index.row() - self._cache_offset, 0))

            if Qt.CheckState(value) == Qt.CheckState.Checked:
                self._current_filter.append(row_value)
            else:
                self._current_filter.remove(row_value)

            self.filterChanged.emit(self._current_filter)
            self.dataChanged.emit(index, index, [role])

            return True

        return False

    # ==================== 私有方法 ====================

    def _cacheRowData(self, row: int):
        """将目标行左右的数据缓存到本地"""
        self._cache_limit = self._PAGE_SIZE * self._WINDOW_PAGES
        self._cache_offset = max(0, row - self._PAGE_SIZE)

        try:
            self._cache_df, self._total_row_count = self._duckdb_service.fetch_filter_table(
                self._table_name,
                self._column_name,
                self._cache_offset,
                self._cache_limit,
                self._keyword,
                self._other_filters,
            )

        except Exception as e:
            print(f"Error fetching data: {e}")

    # ==================== 公共方法 ====================

    def searchByKeyword(self, keyword: str):
        """按关键字搜索"""
        self._keyword = keyword.strip().lower()
        self.refresh()

    def clearSearch(self):
        """清除搜索"""
        self._keyword = str()
        self.refresh()

    def refresh(self):
        """刷新模型数据"""
        self.beginResetModel()
        self._cacheRowData(0)
        self.endResetModel()

    # def setAllChecked(self, checked: bool):
    #     """设置所有项的选中状态"""
    #     self.beginResetModel()
    #     if checked:
    #         self._checked_values = set(str(v) for v in self._df.iloc[:, 0])
    #     else:
    #         self._checked_values.clear()
    #     self.endResetModel()
    #     self.selectionChanged.emit()
    #
    # def isAllChecked(self) -> bool:
    #     """检查是否全选"""
    #     all_values = set(str(v) for v in self._df.iloc[:, 0])
    #     return self._checked_values == all_values
    #
    # def getSelectedValues(self) -> set[str]:
    #     """获取选中的值"""
    #     return self._checked_values.copy()
    #
    # def getSelectedCount(self) -> int:
    #     """获取选中的数量"""
    #     return len(self._checked_values)
