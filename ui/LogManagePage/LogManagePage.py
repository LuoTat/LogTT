from PySide6.QtCore import QModelIndex, QPoint, Qt, Signal, Slot
from PySide6.QtWidgets import QHBoxLayout, QVBoxLayout, QWidget
from qfluentwidgets import (
    Action,
    FluentIcon,
    InfoBar,
    InfoBarPosition,
    MessageBox,
    PrimaryPushButton,
    PushButton,
    RoundMenu,
    SearchLineEdit,
    TableView,
)

from modules.app_config import appcfg
from modules.models import LogColumn, LogStatus, LogTableModel

from .AddLogMessageBox import AddLogMessageBox
from .ExtractLogMessageBox import ExtractLogMessageBox
from .ProgressBarDelegate import ProgressBarDelegate


class LogManagePage(QWidget):
    """日志管理页面"""

    # 查看日志请求信号 (log_id)
    viewLogRequested = Signal(int)
    # 查看模板请求信号 (log_id)
    viewTemplateRequested = Signal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("LogManagePage")

        # 主布局
        self._main_layout = QVBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        # 初始化模型
        self._init_model()

        self._init_toolbar()
        self._init_table_view()

    # ==================== 私有方法 ====================

    def _init_model(self):
        """初始化数据模型"""
        self._log_table_model = LogTableModel(self)

        # 连接模型信号 -> UI 反馈
        self._log_table_model.extractFinished.connect(self._on_extract_finished)
        self._log_table_model.extractInterrupted.connect(self._on_extract_interrupted)
        self._log_table_model.extractError.connect(self._on_extract_error)

        self._log_table_model.addSuccess.connect(self._on_add_success)
        self._log_table_model.addDuplicate.connect(self._on_add_duplicate)
        self._log_table_model.addError.connect(self._on_add_error)

        self._log_table_model.deleteSuccess.connect(self._on_delete_success)
        self._log_table_model.deleteError.connect(self._on_delete_error)

    def _init_toolbar(self):
        """初始化工具栏"""
        tool_bar_layout = QHBoxLayout()
        tool_bar_layout.setSpacing(16)

        self._search_edit = SearchLineEdit(self)
        self._search_edit.setPlaceholderText(self.tr("按名称搜索"))
        self._search_edit.setClearButtonEnabled(True)
        self._search_edit.returnPressed.connect(
            lambda: self._log_table_model.search_by_name(self._search_edit.text())
        )
        self._search_edit.searchSignal.connect(
            lambda keyword: self._log_table_model.search_by_name(keyword)
        )
        self._search_edit.clearSignal.connect(self._log_table_model.clear_search)

        self._refresh_button = PushButton(FluentIcon.SYNC, self.tr("刷新"), self)
        self._refresh_button.clicked.connect(self._log_table_model.refresh)

        self._add_button = PrimaryPushButton(FluentIcon.ADD, self.tr("新增日志"), self)
        self._add_button.clicked.connect(self._on_add_log)

        tool_bar_layout.addWidget(self._search_edit, 1)
        tool_bar_layout.addStretch(1)
        tool_bar_layout.addWidget(self._refresh_button)
        tool_bar_layout.addWidget(self._add_button)

        self._main_layout.addLayout(tool_bar_layout)

    def _init_table_view(self):
        """初始化表格视图"""
        self._table_view = TableView(self)
        self._table_view.setBorderVisible(True)
        self._table_view.setBorderRadius(8)

        # 启用排序，为了不默认排序任何列必须在setModel之前设置
        self._table_view.setSortingEnabled(True)
        # 消除显示的排序指示器
        self._table_view.horizontalHeader().setSortIndicator(
            -1, Qt.SortOrder.AscendingOrder
        )

        self._table_view.setModel(self._log_table_model)

        # 禁用单元格换行
        self._table_view.setWordWrap(False)
        # 隐藏垂直表头
        self._table_view.verticalHeader().hide()
        # 设置每次只选择一行
        self._table_view.setSelectionMode(TableView.SelectionMode.SingleSelection)
        # 设置进度条委托
        self._progress_delegate = ProgressBarDelegate(self._table_view)
        self._table_view.setItemDelegateForColumn(
            LogColumn.PROGRESS, self._progress_delegate
        )
        # 设置右键菜单
        self._table_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._table_view.customContextMenuRequested.connect(
            self._on_context_menu_requested
        )
        # 连接列宽变化信号，保存列宽
        self._table_view.horizontalHeader().sectionResized.connect(
            self._on_column_resized
        )

        # 恢复列宽
        self._restore_column_widths()

        self._main_layout.addWidget(self._table_view)

    def _restore_column_widths(self):
        """从配置恢复列宽"""
        if widths := appcfg.get(appcfg.logTableColumnWidths):
            for col, width in enumerate(widths):
                self._table_view.horizontalHeader().resizeSection(col, width)

    # ==================== 槽函数 ====================

    @Slot()
    def _on_column_resized(self):
        """保存列宽到配置"""
        header = self._table_view.horizontalHeader()
        widths = [
            header.sectionSize(col)
            for col in range(self._log_table_model.columnCount())
        ]
        appcfg.set(appcfg.logTableColumnWidths, widths)

    @Slot(QPoint)
    def _on_context_menu_requested(self, pos: QPoint):
        """处理右键菜单请求"""
        index = self._table_view.indexAt(pos)
        if not index.isValid():
            return

        # 右键选中该行
        self._table_view.setCurrentIndex(index)

        # 获取日志项状态
        status = index.data(LogTableModel.LOG_STATUS_ROLE)

        # 创建菜单
        menu = RoundMenu(parent=self._table_view)

        if status == LogStatus.EXTRACTED:
            # 已提取的日志
            view_log_action = Action(FluentIcon.DOCUMENT, self.tr("查看日志"))
            view_log_action.triggered.connect(lambda: self._on_view_log(index))
            menu.addAction(view_log_action)

            view_template_action = Action(FluentIcon.BOOK_SHELF, self.tr("查看模板"))
            view_template_action.triggered.connect(
                lambda: self._on_view_template(index)
            )
            menu.addAction(view_template_action)
        elif status == LogStatus.NOT_EXTRACTED:
            # 未提取的日志
            extract_action = Action(FluentIcon.PLAY, self.tr("开始提取"))
            extract_action.triggered.connect(lambda: self._on_extract_log(index))
            menu.addAction(extract_action)
        else:
            # 正在提取的日志
            stop_action = Action(FluentIcon.CANCEL, self.tr("终止提取"))
            stop_action.triggered.connect(
                lambda: self._log_table_model.request_interrupt_task(index)
            )
            menu.addAction(stop_action)

        menu.addSeparator()

        # 删除操作
        delete_action = Action(FluentIcon.DELETE, self.tr("删除"))
        delete_action.triggered.connect(lambda: self._on_delete_log(index))
        menu.addAction(delete_action)

        # 显示菜单
        menu.exec(self._table_view.viewport().mapToGlobal(pos))

    @Slot()
    def _on_add_log(self):
        """新增日志"""
        dialog = AddLogMessageBox(self.window())
        if dialog.exec():
            log_uri = dialog.log_uri
            if dialog.is_local_file:
                self._log_table_model.request_add("本地文件", log_uri)
                return
            else:
                self._log_table_model.request_add("网络地址", log_uri, "Drain3")

    @Slot(QModelIndex)
    def _on_extract_log(self, index: QModelIndex):
        """处理提取日志请求"""
        dialog = ExtractLogMessageBox(self.window())
        if dialog.exec():
            # 请求模型执行提取
            self._log_table_model.request_extract(
                index,
                dialog.logparser_type,
                dialog.format_type,
                dialog.log_format,
                dialog.log_regex,
            )

    @Slot(QModelIndex)
    def _on_view_log(self, index: QModelIndex):
        """处理查看日志请求，发送信号给主窗口跳转"""
        log_id = index.data(LogTableModel.LOG_ID_ROLE)
        self.viewLogRequested.emit(log_id)

    @Slot(QModelIndex)
    def _on_view_template(self, index: QModelIndex):
        """处理查看模板请求，发送信号给主窗口跳转"""
        log_id = index.data(LogTableModel.LOG_ID_ROLE)
        self.viewTemplateRequested.emit(log_id)

    @Slot(QModelIndex)
    def _on_delete_log(self, index: QModelIndex):
        """处理删除日志请求"""
        confirm = MessageBox(self.tr("确认删除"), self.tr("确定删除该日志吗？"), self)
        if confirm.exec():
            self._log_table_model.request_delete(index)

    @Slot(int, int)
    def _on_extract_finished(self, _: int, line_count: int):
        """处理提取完成"""
        InfoBar.success(
            title=self.tr("提取成功"),
            content=self.tr("日志模板提取完成，共 {0} 行").format(f"{line_count:,}"),
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )

    @Slot(int)
    def _on_extract_interrupted(self, _: int):
        """处理提取中断"""
        InfoBar.info(
            title=self.tr("提取中断"),
            content=self.tr("日志提取已终止"),
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )

    @Slot(int, str)
    def _on_extract_error(self, _: int, msg: str):
        """处理提取错误"""
        InfoBar.error(
            title=self.tr("提取失败"),
            content=msg,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=5000,
            parent=self,
        )

    @Slot()
    def _on_add_success(self):
        """处理添加成功"""
        InfoBar.success(
            title=self.tr("导入成功"),
            content=self.tr("日志已导入"),
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )

    @Slot()
    def _on_add_duplicate(self):
        """处理添加重复"""
        InfoBar.warning(
            title=self.tr("导入失败"),
            content=self.tr("该日志已存在"),
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=4000,
            parent=self,
        )

    @Slot(str)
    def _on_add_error(self, msg: str):
        """处理添加失败"""
        InfoBar.error(
            title=self.tr("导入失败"),
            content=msg,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=5000,
            parent=self,
        )

    @Slot()
    def _on_delete_success(self):
        """处理删除成功"""
        InfoBar.success(
            title=self.tr("删除成功"),
            content=self.tr("日志已删除"),
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )

    @Slot(str)
    def _on_delete_error(self, msg: str):
        """处理删除失败"""
        InfoBar.error(
            title=self.tr("删除失败"),
            content=msg,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=5000,
            parent=self,
        )

    # ==================== 公共方法 ====================

    def has_extracting_tasks(self) -> bool:
        """是否有正在提取的任务"""
        return self._log_table_model.has_extracting_tasks()

    def interrupt_all_extract_tasks(self):
        """中断所有正在提取的任务"""
        self._log_table_model.interrupt_all_tasks()
