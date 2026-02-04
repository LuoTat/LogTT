from PySide6.QtCore import (
    Slot,
)
from PySide6.QtWidgets import QHeaderView
from qfluentwidgets import (
    MessageBoxBase,
    SearchLineEdit,
    SubtitleLabel,
    TableView,
)

from modules.models import CsvFilterTableModel


class ColumnFilterMessageBox(MessageBoxBase):
    """列过滤对话框"""

    def __init__(
        self,
        table_name: str,
        column_name: str,
        all_filters: dict[str, list[str]],
        parent=None,
    ):
        super().__init__(parent)

        self._table_name = table_name
        self._column_name = column_name
        self._all_filters = all_filters

        # 初始化模型
        self._initModel()

        self._initTitle()
        self._initSearchEdit()
        self._initTableView()
        self.widget.setMinimumWidth(700)
        self.widget.setMinimumHeight(800)

    # ==================== 私有方法 ====================

    def _initModel(self):
        self._csv_filter_table_model = CsvFilterTableModel(self._table_name, self._column_name, self._all_filters, self)
        self._csv_filter_table_model.filterChanged.connect(self._onfilterChanged)

    def _initTitle(self):
        """初始化标题"""
        self._title_label = SubtitleLabel(f"'{self._column_name}' 的本地筛选器", self)
        self.viewLayout.addWidget(self._title_label)

    def _initSearchEdit(self):
        """初始化搜索框"""
        self._search_edit = SearchLineEdit(self)
        self._search_edit.setPlaceholderText("搜索值...")
        self._search_edit.setClearButtonEnabled(True)
        self._search_edit.textChanged.connect(self._onSearchTextChanged)
        self._search_edit.clearSignal.connect(self._onClearSearch)
        self.viewLayout.addWidget(self._search_edit)

    def _initTableView(self):
        """初始化表格视图"""
        self._table_view = TableView(self)
        self._table_view.setBorderVisible(True)
        self._table_view.setBorderRadius(8)
        self._table_view.setModel(self._csv_filter_table_model)

        # 禁用单元格换行
        self._table_view.setWordWrap(False)
        # 隐藏垂直表头
        self._table_view.verticalHeader().hide()
        # 设置每次只选择一行
        self._table_view.setSelectionMode(TableView.SelectionMode.SingleSelection)
        # 设置水平表头拉伸填充
        self._table_view.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        self.viewLayout.addWidget(self._table_view)

    # =================== 槽函数 ====================

    @Slot(object)
    def _onfilterChanged(self, current_filter: list[str]):
        """选择过滤器变化"""
        self._current_filter = current_filter

    @Slot(str)
    def _onSearchTextChanged(self, text: str):
        """搜索文本变化"""
        self._csv_filter_table_model.searchByKeyword(text)

    @Slot()
    def _onClearSearch(self):
        """清除搜索"""
        self._csv_filter_table_model.clearSearch()

    # ==================== 公共方法 ====================

    @property
    def current_filter(self) -> list[str]:
        """当前选择的过滤器"""
        return self._current_filter
