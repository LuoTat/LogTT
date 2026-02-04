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
        self._initModel()

        self._initToolbar()
        self._initTableView()

    # ==================== 私有方法 ====================

    def _initModel(self):
        """初始化数据模型"""
        self._log_table_model = LogTableModel(self)

        # 连接模型信号 -> UI 反馈
        self._log_table_model.extractFinished.connect(self._onExtractFinished)
        self._log_table_model.extractInterrupted.connect(self._onExtractInterrupted)
        self._log_table_model.extractError.connect(self._onExtractError)

        self._log_table_model.addSuccess.connect(self._onAddSuccess)
        self._log_table_model.addDuplicate.connect(self._onAddDuplicate)
        self._log_table_model.addError.connect(self._onAddError)

        self._log_table_model.deleteSuccess.connect(self._onDeleteSuccess)
        self._log_table_model.deleteError.connect(self._onDeleteError)

    def _initToolbar(self):
        """初始化工具栏"""
        tool_bar_layout = QHBoxLayout()
        tool_bar_layout.setSpacing(16)

        self._search_edit = SearchLineEdit(self)
        self._search_edit.setPlaceholderText("按名称搜索")
        self._search_edit.setClearButtonEnabled(True)
        self._search_edit.returnPressed.connect(lambda: self._log_table_model.searchByName(self._search_edit.text()))
        self._search_edit.searchSignal.connect(lambda keyword: self._log_table_model.searchByName(keyword))
        self._search_edit.clearSignal.connect(self._log_table_model.clearSearch)

        self._refresh_button = PushButton(FluentIcon.SYNC, "刷新", self)
        self._refresh_button.clicked.connect(self._log_table_model.refresh)

        self._add_button = PrimaryPushButton(FluentIcon.ADD, "新增日志", self)
        self._add_button.clicked.connect(self._onAddLog)

        tool_bar_layout.addWidget(self._search_edit, 1)
        tool_bar_layout.addStretch(1)
        tool_bar_layout.addWidget(self._refresh_button)
        tool_bar_layout.addWidget(self._add_button)

        self._main_layout.addLayout(tool_bar_layout)

    def _initTableView(self):
        """初始化表格视图"""
        self._table_view = TableView(self)
        self._table_view.setBorderVisible(True)
        self._table_view.setBorderRadius(8)
        self._table_view.setModel(self._log_table_model)

        # 禁用单元格换行
        self._table_view.setWordWrap(False)
        # 隐藏垂直表头
        self._table_view.verticalHeader().hide()
        # 设置每次只选择一行
        self._table_view.setSelectionMode(TableView.SelectionMode.SingleSelection)
        # 启用排序
        self._table_view.setSortingEnabled(True)
        # 设置进度条委托
        self._progress_delegate = ProgressBarDelegate(self._table_view)
        self._table_view.setItemDelegateForColumn(LogColumn.PROGRESS, self._progress_delegate)
        # 设置右键菜单
        self._table_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._table_view.customContextMenuRequested.connect(self._onContextMenuRequested)
        # 连接列宽变化信号，保存列宽
        self._table_view.horizontalHeader().sectionResized.connect(self._onColumnResized)

        # 恢复列宽
        self._restoreColumnWidths()

        self._main_layout.addWidget(self._table_view)

    def _restoreColumnWidths(self):
        """从配置恢复列宽"""
        if widths := appcfg.get(appcfg.logTableColumnWidths):
            for col, width in enumerate(widths):
                self._table_view.horizontalHeader().resizeSection(col, width)

    # ==================== 槽函数 ====================

    @Slot()
    def _onColumnResized(self):
        """保存列宽到配置"""
        header = self._table_view.horizontalHeader()
        widths = [header.sectionSize(col) for col in range(self._log_table_model.columnCount())]
        appcfg.set(appcfg.logTableColumnWidths, widths)

    @Slot(QPoint)
    def _onContextMenuRequested(self, pos: QPoint):
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
            view_log_action = Action(FluentIcon.DOCUMENT, "查看日志")
            view_log_action.triggered.connect(lambda: self._onViewLog(index))
            menu.addAction(view_log_action)

            view_template_action = Action(FluentIcon.BOOK_SHELF, "查看模板")
            view_template_action.triggered.connect(lambda: self._onViewTemplate(index))
            menu.addAction(view_template_action)
        elif status == LogStatus.NOT_EXTRACTED:
            # 未提取的日志
            extract_action = Action(FluentIcon.PLAY, "开始提取")
            extract_action.triggered.connect(lambda: self._onExtractLog(index))
            menu.addAction(extract_action)
        else:
            # 正在提取的日志
            stop_action = Action(FluentIcon.CANCEL, "终止提取")
            stop_action.triggered.connect(lambda: self._log_table_model.requestInterruptTask(index))
            menu.addAction(stop_action)

        menu.addSeparator()

        # 删除操作
        delete_action = Action(FluentIcon.DELETE, "删除")
        delete_action.triggered.connect(lambda: self._onDeleteLog(index))
        menu.addAction(delete_action)

        # 显示菜单
        menu.exec(self._table_view.viewport().mapToGlobal(pos))

    @Slot()
    def _onAddLog(self):
        """新增日志"""
        # TODO:这里的父类考虑设置为主窗口
        dialog = AddLogMessageBox(self)
        if dialog.exec():
            log_uri = dialog.log_uri
            if dialog.is_local_file:
                self._log_table_model.requestAdd("本地文件", log_uri)
                return
            else:
                self._log_table_model.requestAdd("网络地址", log_uri, "Drain3")

    @Slot(QModelIndex)
    def _onExtractLog(self, index: QModelIndex):
        """处理提取日志请求"""
        dialog = ExtractLogMessageBox(self)
        if dialog.exec():
            # 请求模型执行提取
            self._log_table_model.requestExtract(
                index,
                dialog.logparser_type,
                dialog.format_type,
                dialog.log_format,
                dialog.log_regex,
            )

    @Slot(QModelIndex)
    def _onViewLog(self, index: QModelIndex):
        """处理查看日志请求，发送信号给主窗口跳转"""
        log_id = index.data(LogTableModel.LOG_ID_ROLE)
        self.viewLogRequested.emit(log_id)

    @Slot(QModelIndex)
    def _onViewTemplate(self, index: QModelIndex):
        """处理查看模板请求，发送信号给主窗口跳转"""
        log_id = index.data(LogTableModel.LOG_ID_ROLE)
        self.viewTemplateRequested.emit(log_id)

    @Slot(QModelIndex)
    def _onDeleteLog(self, index: QModelIndex):
        """处理删除日志请求"""
        confirm = MessageBox("确认删除", "确定删除该日志吗？", self)
        if confirm.exec():
            self._log_table_model.requestDelete(index)

    @Slot(int, int)
    def _onExtractFinished(self, _: int, line_count: int):
        """处理提取完成"""
        InfoBar.success(
            title="提取成功",
            content=f"日志模板提取完成，共 {line_count:,} 行",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )

    @Slot(int)
    def _onExtractInterrupted(self, _: int):
        """处理提取中断"""
        InfoBar.info(
            title="提取中断",
            content="日志提取已终止",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )

    @Slot(int, str)
    def _onExtractError(self, _: int, msg: str):
        """处理提取错误"""
        InfoBar.error(
            title="提取失败",
            content=msg,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=5000,
            parent=self,
        )

    @Slot()
    def _onAddSuccess(self):
        """处理添加成功"""
        InfoBar.success(
            title="导入成功",
            content="日志已导入",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )

    @Slot()
    def _onAddDuplicate(self):
        """处理添加重复"""
        InfoBar.warning(
            title="导入失败",
            content="该日志已存在",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=4000,
            parent=self,
        )

    @Slot(str)
    def _onAddError(self, msg: str):
        """处理添加失败"""
        InfoBar.error(
            title="导入失败",
            content=msg,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=5000,
            parent=self,
        )

    @Slot()
    def _onDeleteSuccess(self):
        """处理删除成功"""
        InfoBar.success(
            title="删除成功",
            content="日志已删除",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )

    @Slot(str)
    def _onDeleteError(self, msg: str):
        """处理删除失败"""
        InfoBar.error(
            title="删除失败",
            content=msg,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=5000,
            parent=self,
        )

    # ==================== 公共方法 ====================

    def hasExtractingTasks(self) -> bool:
        """是否有正在提取的任务"""
        return self._log_table_model.hasExtractingTasks()

    def interruptAllExtractTasks(self):
        """中断所有正在提取的任务"""
        self._log_table_model.interruptAllTasks()
