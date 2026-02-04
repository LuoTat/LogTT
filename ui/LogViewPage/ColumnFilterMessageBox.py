from PySide6.QtCore import (
    Slot,
)
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

    # ==================== 私有方法 ====================

    def _initModel(self):
        self._csv_filter_table_model = CsvFilterTableModel(
            self._table_name,
            self._column_name,
            self._all_filters,
            self,
        )
        self._csv_filter_table_model.filterChanged.connect(self._onfilterChanged)

    def _initTitle(self):
        """初始化标题"""
        self._title_label = SubtitleLabel(f"'{self._column_name}' 的本地筛选器", self)
        self.viewLayout.addWidget(self._title_label)

    def _initSearchEdit(self):
        """初始化搜索框"""
        self._search_edit = SearchLineEdit(self)
        # self.searchEdit.setFixedWidth(600)
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
        # 启用排序
        self._table_view.setSortingEnabled(True)
        # 最后一列拉伸填充
        self._table_view.horizontalHeader().setStretchLastSection(True)

        self.viewLayout.addWidget(self._table_view)

    # def _updateCountLabel(self):
    #     """更新计数标签"""
    #     selected_count = self.model.getSelectedCount()
    #     total_count = self.model.rowCount()
    #     self.countLabel.setText(f"已选择 {selected_count}/{total_count}")
    #
    # def _updateSelectAllState(self):
    #     """更新全选复选框状态"""
    #     self.selectAllCheckBox.blockSignals(True)
    #     self.selectAllCheckBox.setChecked(self.model.isAllChecked())
    #     self.selectAllCheckBox.blockSignals(False)
    #
    # @Slot(str)
    # def _onSearchTextChanged(self, text: str):
    #     """搜索文本变化"""
    #     self.model.searchByKeyword(text)
    #     self._updateCountLabel()
    #     self._updateSelectAllState()
    #
    # @Slot(int)
    # def _onSelectAllChanged(self, state):
    #     """全选状态变化"""
    #     checked = state == Qt.CheckState.Checked.value
    #     self.model.setAllChecked(checked)
    #     self._updateCountLabel()

    # =================== 槽函数 ====================

    @Slot(object)
    def _onfilterChanged(self, current_filter: list[str]):
        """选择变化"""
        self._current_filter = current_filter

    # @Slot(QModelIndex)
    # def _onTableClicked(self, index: QModelIndex):
    #     """表格点击 - 切换复选框状态"""
    #     # 点击任意列都触发复选框切换
    #     check_index = self.model.index(index.row(), 0)
    #     current_state = self.model.data(check_index, Qt.ItemDataRole.CheckStateRole)
    #     new_state = Qt.CheckState.Unchecked if current_state == Qt.CheckState.Checked else Qt.CheckState.Checked
    #     self.model.setData(check_index, new_state, Qt.ItemDataRole.CheckStateRole)

    # def getSelectedValues(self) -> set[str]:
    #     """获取选中的值"""
    #     return self.model.getSelectedValues()
    #
    # def isAllSelected(self) -> bool:
    #     """是否全选"""
    #     return self.model.isAllChecked()
