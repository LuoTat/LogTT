from PySide6.QtCore import (
    QPoint,
    Qt,
    Slot,
)
from PySide6.QtGui import QShowEvent
from PySide6.QtWidgets import (
    QHBoxLayout,
    QHeaderView,
    QVBoxLayout,
    QWidget,
)
from qfluentwidgets import (
    Action,
    BodyLabel,
    FluentIcon,
    InfoBar,
    InfoBarPosition,
    RoundMenu,
    TableView,
)
from qfluentwidgets.components import ModelComboBox

from modules.duckdb_service import DuckDBService
from modules.models import CsvFileTableModel, ExtractedLogListModel
from ui.Widgets import ColumnFilterMessageBox


class TemplateViewPage(QWidget):
    """模板查看页面"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("TemplateViewPage")

        # 主布局
        self._main_layout = QVBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        # 初始化日志列表模型
        self._extracted_log_list_model = ExtractedLogListModel(self)
        self._init_toolbar()
        self._init_table_view()

    # ==================== 重写方法 ====================

    def showEvent(self, event: QShowEvent):
        """页面显示时自动刷新日志列表"""
        super().showEvent(event)
        self._extracted_log_list_model.refresh()

    # ==================== 私有方法 ====================

    def _init_toolbar(self):
        """初始化工具栏"""
        tool_bar_layout = QHBoxLayout()
        tool_bar_layout.setSpacing(16)

        # 日志选择标签
        label = BodyLabel(self.tr("选择日志："), self)
        tool_bar_layout.addWidget(label)

        # 日志选择下拉框
        self._log_combo_box = ModelComboBox(self)
        self._log_combo_box.setModel(self._extracted_log_list_model)
        self._log_combo_box.setMinimumWidth(400)
        self._log_combo_box.setPlaceholderText(self.tr("请选择已提取的日志文件"))
        self._log_combo_box.currentIndexChanged.connect(self._on_log_selected)
        tool_bar_layout.addWidget(self._log_combo_box)

        tool_bar_layout.addStretch()

        # 统计信息
        self._info_label = BodyLabel(self)
        tool_bar_layout.addWidget(self._info_label)

        self._main_layout.addLayout(tool_bar_layout)

    def _init_table_view(self):
        """初始化表格视图"""
        # 初始化时不设置模型，等待用户选择日志
        self._table_view = TableView(self)
        self._table_view.setBorderVisible(True)
        self._table_view.setBorderRadius(8)

        # 禁用单元格换行
        self._table_view.setWordWrap(False)
        # 隐藏垂直表头
        self._table_view.verticalHeader().hide()
        # 设置每次只选择一行
        self._table_view.setSelectionMode(TableView.SelectionMode.SingleSelection)
        # 设置水平表头拉伸填充
        self._table_view.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.Stretch
        )
        # 设置表格单元格右键菜单
        self._table_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._table_view.customContextMenuRequested.connect(
            self._on_context_menu_requested
        )
        # 设置表头右键菜单
        header = self._table_view.horizontalHeader()
        header.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        header.customContextMenuRequested.connect(
            self._on_header_context_menu_requested
        )

        self._main_layout.addWidget(self._table_view)

    def _update_info_label(self):
        """更新行数统计标签"""
        if self._csv_file_table_model:
            total = self._csv_file_table_model.total_row_count()
            filtered = self._csv_file_table_model.filtered_row_count()

            if total == filtered:
                self._info_label.setText(self.tr("共 {0} 行").format(f"{total:,}"))
            else:
                self._info_label.setText(
                    self.tr("显示 {0} / {1} 行").format(f"{filtered:,}", f"{total:,}")
                )
        else:
            self._info_label.setText("")

    def set_log(self, log_id: int):
        """设置选择的日志"""
        # 先刷新一次日志列表
        self._extracted_log_list_model.refresh()
        # 查找对应的索引
        if (index := self._extracted_log_list_model.get_row(log_id)) >= 0:
            self._log_combo_box.setCurrentIndex(index)

    # ==================== 槽函数 ====================

    @Slot(int)
    def _on_log_selected(self, index: int):
        # 从模型获取数据
        model_index = self._extracted_log_list_model.index(index)
        templates_table_name = model_index.data(
            ExtractedLogListModel.TEMPLATES_TABLE_NAME_ROLE
        )

        # 检查表是否存在
        duckdb_service = DuckDBService()
        if not duckdb_service.table_exists(templates_table_name):
            InfoBar.error(
                title=self.tr("数据未找到"),
                content=self.tr("未找到模板表: {0}").format(templates_table_name),
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=5000,
                parent=self,
            )
            return

        # 创建新的模型实例
        self._csv_file_table_model = CsvFileTableModel(templates_table_name, self)
        self._table_view.setModel(self._csv_file_table_model)
        self._update_info_label()

    def _show_column_filter_menu(self, column_name: str, global_pos: QPoint):
        """显示列过滤菜单"""
        # 创建菜单
        menu = RoundMenu(parent=self._table_view)

        # 设置本地筛选器
        filter_action = Action(
            FluentIcon.FILTER, self.tr("设置 '{0}' 的筛选器").format(column_name)
        )
        filter_action.triggered.connect(lambda: self._on_set_column_filter(column_name))
        menu.addAction(filter_action)

        # 如果该列有过滤，添加清除选项
        if self._csv_file_table_model.is_column_filtered(column_name):
            clear_action = Action(
                FluentIcon.DELETE, self.tr("清除 '{0}' 的筛选器").format(column_name)
            )
            clear_action.triggered.connect(
                lambda: self._on_clear_column_filter(column_name)
            )
            menu.addAction(clear_action)

        menu.addSeparator()

        # 清除所有筛选器
        clear_all_action = Action(FluentIcon.CLOSE, self.tr("清除所有筛选器"))
        clear_all_action.triggered.connect(self._on_clear_all_filters)
        menu.addAction(clear_all_action)

        # 显示菜单
        menu.exec(global_pos)

    @Slot(QPoint)
    def _on_context_menu_requested(self, pos: QPoint):
        """处理表格单元格右键菜单请求"""
        index = self._table_view.indexAt(pos)
        if not index.isValid():
            return

        column_name = self._csv_file_table_model.headerData(
            index.column(), Qt.Orientation.Horizontal, Qt.ItemDataRole.DisplayRole
        )

        global_pos = self._table_view.viewport().mapToGlobal(pos)
        self._show_column_filter_menu(column_name, global_pos)

    @Slot(QPoint)
    def _on_header_context_menu_requested(self, pos: QPoint):
        """处理表头右键菜单请求"""
        header = self._table_view.horizontalHeader()
        column_index = header.logicalIndexAt(pos.x())

        if column_index < 0:
            return

        column_name = self._csv_file_table_model.headerData(
            column_index, Qt.Orientation.Horizontal, Qt.ItemDataRole.DisplayRole
        )

        global_pos = header.mapToGlobal(pos)
        self._show_column_filter_menu(column_name, global_pos)

    @Slot(str)
    def _on_set_column_filter(self, column_name: str):
        """设置列过滤"""
        all_filters = self._csv_file_table_model.get_all_filters()
        dialog = ColumnFilterMessageBox(
            self._csv_file_table_model.table_name(),
            column_name,
            all_filters,
            self.window(),
        )
        if dialog.exec():
            self._csv_file_table_model.set_column_filter(
                column_name, dialog.current_filter
            )
            self._update_info_label()

    @Slot(str)
    def _on_clear_column_filter(self, column_name: str):
        """清除列过滤"""
        self._csv_file_table_model.clear_column_filter(column_name)
        self._update_info_label()

    @Slot()
    def _on_clear_all_filters(self):
        """清除所有过滤"""
        self._csv_file_table_model.clear_all_filters()
        self._update_info_label()
