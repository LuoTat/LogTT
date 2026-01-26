from pathlib import Path

from PySide6.QtCore import (
    Qt,
    Slot,
    Signal,
    QPoint,
    QModelIndex
)
from PySide6.QtWidgets import (
    QWidget,
    QHBoxLayout,
    QVBoxLayout
)
from qfluentwidgets import (
    Action,
    InfoBar,
    RoundMenu,
    TableView,
    FluentIcon,
    MessageBox,
    PushButton,
    SearchLineEdit,
    InfoBarPosition,
    MenuAnimationType,
    PrimaryPushButton
)

from ui.APPConfig import appcfg
from modules.models import LogTableModel, LogStatus, LogColumn

from .AddLogMessageBox import AddLogMessageBox
from .ProgressBarDelegate import ProgressBarDelegate
from .ExtractLogMessageBox import ExtractLogMessageBox


class LogManagePage(QWidget):
    """日志管理页面"""

    # 查看日志请求信号 (log_id)
    viewLogRequested = Signal(int)
    # 查看模板请求信号 (log_id) - 待后续实现
    viewTemplateRequested = Signal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("LogManagePage")

        # 主布局
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(24, 24, 24, 24)
        self.main_layout.setSpacing(16)

        # 初始化模型
        self._initModel()

        self._initToolbar()
        self._initTableView()

    def _initModel(self):
        """初始化数据模型"""
        self.model = LogTableModel(self)

        # 连接模型信号 -> UI 反馈
        self.model.extractFinished.connect(self._onExtractFinished)
        self.model.extractInterrupted.connect(self._onExtractInterrupted)
        self.model.extractError.connect(self._onExtractError)

        self.model.addSuccess.connect(self._onAddSuccess)
        self.model.addDuplicate.connect(self._onAddDuplicate)
        self.model.addError.connect(self._onAddError)

        self.model.deleteSuccess.connect(self._onDeleteSuccess)
        self.model.deleteError.connect(self._onDeleteError)

    def _initToolbar(self):
        """初始化工具栏"""
        tool_bar_layout = QHBoxLayout()
        tool_bar_layout.setSpacing(16)

        self.search_input = SearchLineEdit(self)
        self.search_input.setPlaceholderText("按名称搜索")
        self.search_input.setClearButtonEnabled(True)
        self.search_input.searchSignal.connect(lambda keyword: self.model.searchByName(keyword))
        self.search_input.clearSignal.connect(self.model.clearSearch)

        self.refresh_button = PushButton(FluentIcon.SYNC, "刷新", self)
        self.refresh_button.clicked.connect(self.model.refresh)

        self.add_button = PrimaryPushButton(FluentIcon.ADD, "新增日志", self)
        self.add_button.clicked.connect(self._onAddLog)

        tool_bar_layout.addWidget(self.search_input, 1)
        tool_bar_layout.addStretch(1)
        tool_bar_layout.addWidget(self.refresh_button)
        tool_bar_layout.addWidget(self.add_button)

        self.main_layout.addLayout(tool_bar_layout)

    def _initTableView(self):
        """初始化表格视图"""
        self.table_view = TableView(self)
        self.table_view.setBorderVisible(True)
        self.table_view.setBorderRadius(8)
        self.table_view.setModel(self.model)

        # 禁用单元格换行
        self.table_view.setWordWrap(False)
        # 隐藏垂直表头
        self.table_view.verticalHeader().hide()
        # 禁用编辑
        self.table_view.setEditTriggers(TableView.EditTrigger.NoEditTriggers)
        # 设置最后一列拉伸填充
        # self.table_view.horizontalHeader().setStretchLastSection(True)
        # 设置每次只选择一行
        self.table_view.setSelectionMode(TableView.SelectionMode.SingleSelection)
        # 启用排序
        # self.table_view.setSortingEnabled(True)
        # 设置进度条委托
        self.progress_delegate = ProgressBarDelegate(self.table_view)
        self.table_view.setItemDelegateForColumn(LogColumn.PROGRESS, self.progress_delegate)
        # 设置右键菜单
        self.table_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self._onContextMenuRequested)
        # 连接列宽变化信号，保存列宽
        self.table_view.horizontalHeader().sectionResized.connect(self._onColumnResized)

        # 恢复列宽
        self._restoreColumnWidths()

        self.main_layout.addWidget(self.table_view)

    def _restoreColumnWidths(self):
        """从配置恢复列宽"""
        widths = appcfg.get(appcfg.logTableColumnWidths)
        if widths and len(widths) == self.model.columnCount():
            header = self.table_view.horizontalHeader()
            for col, width in enumerate(widths):
                if width > 0:
                    header.resizeSection(col, width)

    def _showToast(self, text: str):
        """显示提示信息"""
        InfoBar.info(
            title=text,
            content="功能待实现",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self
        )

    def hasExtractingTasks(self) -> bool:
        """是否有正在提取的任务"""
        return self.model.hasExtractingTasks()

    def interruptAllExtractTasks(self):
        """中断所有正在提取的任务"""
        self.model.interruptAllTasks()

    # ==================== 槽函数 ====================

    @Slot(int, int, int)
    def _onColumnResized(self):
        """保存列宽到配置"""
        header = self.table_view.horizontalHeader()
        widths = [header.sectionSize(col) for col in range(self.model.columnCount())]
        appcfg.set(appcfg.logTableColumnWidths, widths)

    @Slot(QPoint)
    def _onContextMenuRequested(self, pos: QPoint):
        """处理右键菜单请求"""
        index = self.table_view.indexAt(pos)
        if not index.isValid():
            return

        # 右键选中该行
        self.table_view.setCurrentIndex(index)

        # 获取日志项状态
        status = index.data(LogTableModel.LOG_STATUS_ROLE)

        # 创建菜单
        menu = RoundMenu(parent=self.table_view)

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
            stop_action.triggered.connect(lambda: self.model.requestInterruptTask(index))
            menu.addAction(stop_action)

        menu.addSeparator()

        # 删除操作
        delete_action = Action(FluentIcon.DELETE, "删除")
        delete_action.triggered.connect(lambda: self._onDeleteLog(index))
        menu.addAction(delete_action)

        # 显示菜单
        menu.exec(self.table_view.viewport().mapToGlobal(pos), aniType=MenuAnimationType.DROP_DOWN)

    @Slot()
    def _onAddLog(self):
        """新增日志"""
        dialog = AddLogMessageBox(self)
        if dialog.exec():
            if dialog.selected_file_path:
                log_uri = Path(dialog.selected_file_path).resolve().as_posix()
                self.model.requestAdd("本地文件", log_uri)
                return

            if dialog.url_input.text():
                log_uri = dialog.url_input.text().strip()
                self.model.requestAdd("网络地址", log_uri, "Drain3")
                return

            InfoBar.warning(
                title="未选择文件",
                content="请选择UDP/TCP日志源或本地日志文件",
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=4000,
                parent=self
            )

    @Slot(int)
    def _onExtractLog(self, index: QModelIndex):
        """处理提取日志请求"""
        dialog = ExtractLogMessageBox(self)
        if dialog.exec():
            if dialog.is_custom_mode:
                dialog.format_config_manager.save_custom_format(
                    dialog.selected_format_type,
                    dialog.selected_log_format,
                    dialog.selected_regex
                )

            # 请求模型执行提取
            self.model.requestExtract(
                index,
                dialog.selected_algorithm,
                dialog.selected_format_type,
                dialog.selected_log_format,
                dialog.selected_regex
            )

    @Slot(int)
    def _onViewLog(self, index: QModelIndex):
        """处理查看日志请求，发送信号给主窗口跳转"""
        log_id = self.model.data(
            self.model.index(index.row(), SqlColumn.ID),
            Qt.ItemDataRole.DisplayRole
        )
        if log_id is not None:
            self.viewLogRequested.emit(int(log_id))

    @Slot(int)
    def _onViewTemplate(self, index: QModelIndex):
        """处理查看模板请求，发送信号给主窗口跳转"""
        log_id = self.model.data(
            self.model.index(index.row(), SqlColumn.ID),
            Qt.ItemDataRole.DisplayRole
        )
        if log_id is not None:
            self.viewTemplateRequested.emit(int(log_id))

    @Slot(int)
    def _onDeleteLog(self, index: QModelIndex):
        """处理删除日志请求"""
        confirm = MessageBox("确认删除", f"确定删除该日志吗？", self)
        if confirm.exec():
            self.model.requestDelete(index)

    # ==================== 模型信号回调 ====================

    @Slot(int, int)
    def _onExtractFinished(self, log_id: int, line_count: int):
        """处理提取完成"""
        InfoBar.success(
            title="提取成功",
            content=f"日志模板提取完成，共 {line_count:,} 行",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self
        )

    @Slot(int)
    def _onExtractInterrupted(self, log_id: int):
        """处理提取中断"""
        InfoBar.info(
            title="提取中断",
            content="日志提取已终止",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self
        )

    @Slot(int, int)
    def _onExtractProgress(self, log_id: int, progress: int):
        """处理提取进度更新"""
        # 进度更新由模型内部处理，UI 不需要额外操作
        pass

    @Slot(int, str)
    def _onExtractError(self, log_id: int, msg: str):
        """处理提取错误"""
        InfoBar.error(
            title="提取失败",
            content=msg,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=5000,
            parent=self
        )

    @Slot(int)
    def _onAddSuccess(self):
        """处理添加成功"""
        InfoBar.success(
            title="导入成功",
            content="日志已导入",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self
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
            parent=self
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
            parent=self
        )

    @Slot(int)
    def _onDeleteSuccess(self):
        """处理删除成功"""
        InfoBar.success(
            title="删除成功",
            content="日志已删除",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self
        )

    @Slot(int, str)
    def _onDeleteError(self, msg: str):
        """处理删除失败"""
        InfoBar.error(
            title="删除失败",
            content=msg,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=5000,
            parent=self
        )