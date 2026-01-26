import pandas
from typing import Any

from PySide6.QtCore import (
    Qt,
    Signal,
    QModelIndex,
    QAbstractTableModel
)

from .duckdb_service import DuckDBService


class DuckDBTableModel(QAbstractTableModel):
    """用来显示DuckDB数据库的模型类"""

    # 每次加载的页数
    WINDOW_PAGES = 5
    # 每页的行数
    PAGE_SIZE = 200

    # UI 控制信号
    dataLoading = Signal()  # 开始加载数据
    dataLoaded = Signal()  # 数据加载完成
    filterChanged = Signal()  # 过滤条件发生变化

    def __init__(self, table_name: str, parent=None):
        super().__init__(parent)

        self._duckdb_service = DuckDBService()

        # 表的元信息
        self._table_name = table_name
        self._columns = self._duckdb_service.get_table_columns(table_name)
        self._total_row_count = self._duckdb_service.get_table_row_count(table_name)

        # 缓存的DataFrame
        self._cache_df: pandas.DataFrame = pandas.DataFrame()
        self._cache_offset: int = 0
        self._cache_limit: int = 0

        # 排序的状态
        self._order_expr: str = ""

        # 过滤的状态
        self._filter_expr: str = ""
        self._filter_dcit: dict[int, list[str]] = dict()
        self._filtered_row_count = self._total_row_count

    # ==================== 重写方法 ====================

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return self._filtered_row_count

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._columns)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            if 0 <= section < len(self._columns):
                return self._columns[section]
        return None

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if role == Qt.ItemDataRole.DisplayRole:
            if self._cache_offset <= row < self._cache_offset + self._cache_limit:
                return self._cache_df.iat[row - self._cache_offset, col]
            else:
                self._cacheRowData(row)
                return "Loading..."

        elif role == Qt.ItemDataRole.TextAlignmentRole:
            return Qt.AlignmentFlag.AlignLeft

        return None

    def sort(self, column: int, order: Qt.SortOrder = Qt.SortOrder.AscendingOrder):
        # TODO: 构建排序表达式
        # self._order_expr = order

        self.beginResetModel()
        # 重头开始加载数据
        self._cacheRowData(0)
        self.endResetModel()

    # ==================== 私有辅助方法 ====================

    def _cacheRowData(self, row: int):
        """将目标行左右的数据缓存到本地"""
        self._cache_limit = self.PAGE_SIZE * self.WINDOW_PAGES
        self._cache_offset = max(0, row - self._cache_limit // 2)

        try:
            self._cache_df = self._duckdb_service.fetch_page(
                self._table_name,
                self._cache_offset,
                self._cache_limit,
                self._filter_expr,
                self._order_expr
            ).to_df()
        except Exception as e:
            print(f"Error fetching data: {e}")

    # ==================== 公共方法 ====================
    def tableName(self) -> str:
        """获取当前表名 """
        return self._table_name

    def columns(self) -> list[str]:
        """获取列名列表"""
        return self._columns.copy()

    def totalRowCount(self) -> int:
        """获取总行数"""
        return self._total_row_count

    def filteredRowCount(self) -> int:
        """获取过滤后的行数"""
        return self._filtered_row_count

    def isColumnFiltered(self, column: int) -> bool:
        """检查列是否有过滤"""
        return column in self._filter_dcit

    def getColumnFilter(self, column: int) -> list[str]:
        """获取列的过滤值"""
        return self._filter_dcit[column]

    def setColumnFilter(self, column: int, values: list[str]):
        """设置列的过滤值 """
        # TODO: 构建过滤表达式并应用
        self._filter_dcit[column] = values

    def clearColumnFilter(self, column: int):
        """清除列的过滤"""
        # 重构过滤表达式
        self._filter_dcit.pop(column)

    def clearAllFilters(self):
        """清除所有过滤"""
        self._filter_expr = ""  # 重置过滤表达式
        self._filter_dcit.clear()  # 清空过滤列标记