import sqlite3
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass

from PySide6.QtCore import (
    Qt,
    Slot,
    QPoint,
    QThread
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
from modules.log import LogService, LogRecord
from .LogExtractTask import LogExtractTask
from .AddLogMessageBox import AddLogMessageBox
from .ProgressBarDelegate import ProgressBarDelegate
from .ExtractLogMessageBox import ExtractLogMessageBox
from .LogTableModel import LogTableModel, LogTableHeader, LogStatus


@dataclass
class LogExtractTaskInfo:
    """日志提取任务信息"""

    thread: QThread
    task: LogExtractTask
    progress: int = 0


class LogManagePage(QWidget):
    """日志管理页面 - 使用MVD架构"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("LogManagePage")

        self.log_service = LogService()

        # 存储正在提取的任务信息: log_id -> LogExtractTaskInfo
        self.log_extract: dict[int, LogExtractTaskInfo] = {}

        # 主布局
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(24, 24, 24, 24)
        self.main_layout.setSpacing(16)

        # 初始化模型
        self._initModel()

        self._initToolbar()
        self._initTableView()

        # 刷新表格数据
        self._onRefreshModel()

    def _initModel(self):
        """初始化数据模型"""
        self.model = LogTableModel(self)
        # 连接模型信号
        self.model.extract.connect(self._onExtractLog)
        self.model.viewLog.connect(self._onViewLog)
        self.model.viewTemplate.connect(self._onViewTemplate)
        self.model.delete.connect(self._onDeleteLog)

    def _initToolbar(self):
        """初始化工具栏"""
        tool_bar_layout = QHBoxLayout()
        tool_bar_layout.setSpacing(16)

        self.search_input = SearchLineEdit(self)
        self.search_input.setPlaceholderText("按URI搜索")
        self.search_input.setClearButtonEnabled(True)
        self.search_input.searchSignal.connect(self._onSearchLog)
        self.search_input.clearSignal.connect(self._onRefreshModel)

        self.refresh_button = PushButton(FluentIcon.SYNC, "刷新", self)
        self.refresh_button.clicked.connect(self._onRefreshModel)

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
        self.table_view.setWordWrap(False)
        self.table_view.setModel(self.model)

        # 隐藏垂直表头
        self.table_view.verticalHeader().hide()
        # 禁用编辑
        self.table_view.setEditTriggers(TableView.EditTrigger.NoEditTriggers)
        # 设置每次只选择一行
        self.table_view.setSelectionMode(TableView.SelectionMode.SingleSelection)
        # 设置进度条委托
        self.progress_delegate = ProgressBarDelegate(self.table_view)
        self.table_view.setItemDelegateForColumn(LogTableHeader.PROGRESS, self.progress_delegate)
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

    def _getLogProgress(self) -> dict[int, int]:
        """获取正在提取的任务进度映射"""
        return {log_id: info.progress for log_id, info in self.log_extract.items()}

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

    def _interruptExtractTask(self, log_id: int):
        """中断正在提取的任务"""
        # 清理任务信息
        if log_id in self.log_extract:
            task_info = self.log_extract.pop(log_id)
            task_info.thread.requestInterruption()
            task_info.thread.quit()
            task_info.thread.wait()

    def hasExtractingTasks(self) -> bool:
        """是否有正在提取的任务"""
        return len(self.log_extract) > 0

    def interruptAllExtractTasks(self):
        """中断所有正在提取的任务"""
        for log_id, task_info in self.log_extract.items():
            task_info.thread.requestInterruption()
            task_info.thread.quit()
            task_info.thread.wait()
        self.log_extract.clear()

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

        # 获取日志项信息
        log_id = index.data(LogTableModel.LogIdRole)
        status = index.data(LogTableModel.StatusRole)

        # 创建菜单
        menu = RoundMenu(parent=self.table_view)

        if status == LogStatus.EXTRACTED:
            # 已提取的日志
            view_log_action = Action(FluentIcon.DOCUMENT, "查看日志")
            view_log_action.triggered.connect(lambda: self._onViewLog(log_id))
            menu.addAction(view_log_action)

            view_template_action = Action(FluentIcon.BOOK_SHELF, "查看模板")
            view_template_action.triggered.connect(lambda: self._onViewTemplate(log_id))
            menu.addAction(view_template_action)
        elif status == LogStatus.NOT_EXTRACTED:
            # 未提取的日志
            extract_action = Action(FluentIcon.PLAY, "开始提取")
            extract_action.triggered.connect(lambda: self._onExtractLog(log_id))
            menu.addAction(extract_action)
        else:
            # 正在提取的日志
            stop_action = Action(FluentIcon.CANCEL, "终止提取")
            stop_action.triggered.connect(lambda: self._onInterruptExtract(log_id))
            menu.addAction(stop_action)

        menu.addSeparator()

        # 删除操作
        delete_action = Action(FluentIcon.DELETE, "删除")
        delete_action.triggered.connect(lambda: self._onDeleteLog(log_id))
        menu.addAction(delete_action)

        # 显示菜单
        menu.exec(self.table_view.viewport().mapToGlobal(pos), aniType=MenuAnimationType.DROP_DOWN)

    @Slot()
    def _onRefreshModel(self):
        """刷新表格数据"""
        logs = self.log_service.get_all_logs()
        log_progress = self._getLogProgress()
        self.model.setLogs(logs, log_progress)

    @Slot(str)
    def _onSearchLog(self, keyword: str):
        """搜索日志"""
        keyword = keyword.strip()
        if not keyword:
            self._onRefreshModel()
            return

        filtered_logs = self.log_service.search_by_uri(keyword)
        extracting_progress = self._getLogProgress()
        self.model.setLogs(filtered_logs, extracting_progress)

    @Slot()
    def _onAddLog(self):
        """新增日志"""
        dialog = AddLogMessageBox(self)
        if dialog.exec():
            if dialog.selected_file_path:
                try:
                    log = LogRecord(
                        id=-1,
                        log_type="本地文件",
                        format_type=None,
                        log_uri=Path(dialog.selected_file_path).resolve().as_posix(),
                        create_time=datetime.now(),
                        is_extracted=False,
                        extract_method=None,
                        line_count=None
                    )
                    log.id = self.log_service.add_log(log)
                    self.model.addLog(log)
                    InfoBar.success(
                        title="导入成功",
                        content="本地日志已导入",
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=3000,
                        parent=self
                    )
                except sqlite3.IntegrityError:
                    InfoBar.warning(
                        title="导入失败",
                        content="该日志文件已存在",
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=4000,
                        parent=self
                    )
                except Exception as exc:
                    InfoBar.error(
                        title="导入失败",
                        content=str(exc),
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=5000,
                        parent=self
                    )
                return

            if dialog.url_input.text():
                try:
                    log = LogRecord(
                        id=-1,
                        log_type="网络地址",
                        format_type=None,
                        log_uri=dialog.url_input.text().strip(),
                        create_time=datetime.now(),
                        is_extracted=False,
                        extract_method="Drain3",
                        line_count=None
                    )
                    log.id = self.log_service.add_log(log)
                    self.model.addLog(log)
                    InfoBar.success(
                        title="导入成功",
                        content="网络日志源已添加",
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=3000,
                        parent=self
                    )
                except sqlite3.IntegrityError:
                    InfoBar.warning(
                        title="导入失败",
                        content="该网络日志源已存在",
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=4000,
                        parent=self
                    )
                except Exception as exc:
                    InfoBar.error(
                        title="导入失败",
                        content=str(exc),
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=5000,
                        parent=self
                    )
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
    def _onExtractLog(self, log_id: int):
        """处理提取日志请求"""
        item = self.model.getLog(log_id)
        if item is None:
            return

        dialog = ExtractLogMessageBox(self)
        if dialog.exec():
            if dialog.is_custom_mode:
                dialog.format_config_manager.save_custom_format(
                    dialog.selected_format_type,
                    dialog.selected_log_format,
                    dialog.selected_regex
                )

            # 创建工作线程和提取任务
            thread = QThread()
            task = LogExtractTask(
                log_id,
                Path(item.log_uri),
                dialog.selected_algorithm,
                dialog.selected_format_type,
                dialog.selected_log_format,
                dialog.selected_regex
            )
            task.moveToThread(thread)

            # 保存任务信息
            task_info = LogExtractTaskInfo(thread, task)
            self.log_extract[log_id] = task_info

            # 连接信号
            thread.started.connect(task.run)
            task.finished.connect(self._onExtractFinished)
            task.interrupted.connect(self._onExtractInterrupted)
            task.progress.connect(self._onExtractProgress)
            task.error.connect(self._onExtractErrored)

            # 启动线程
            thread.start()

    @Slot(int)
    def _onInterruptExtract(self, log_id: int):
        """终止提取任务"""
        # 如果有正在提取的任务，中断线程
        self._interruptExtractTask(log_id)
        self._onRefreshModel()

    @Slot(int, int)
    def _onExtractFinished(self, log_id: int, line_count: int):
        """处理提取完成"""
        # 清理任务信息
        if log_id in self.log_extract:
            task_info = self.log_extract.pop(log_id)
            task_info.thread.quit()
            task_info.thread.wait()

        InfoBar.success(
            title="提取成功",
            content=f"日志模板提取完成，共 {line_count:,} 行",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self
        )

        self._onRefreshModel()

    @Slot(int)
    def _onExtractInterrupted(self, log_id: int):
        """处理提取中断"""
        # 清理任务信息
        if log_id in self.log_extract:
            task_info = self.log_extract.pop(log_id)
            task_info.thread.quit()
            task_info.thread.wait()

        InfoBar.info(
            title="提取中断",
            content="日志提取已终止",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self
        )

        self._onRefreshModel()

    @Slot(int, int)
    def _onExtractProgress(self, log_id: int, progress: int):
        """处理提取进度更新"""
        if log_id in self.log_extract:
            self.log_extract[log_id].progress = progress
            # 更新进度
            self.model.setProgress(log_id, progress)

    @Slot(int, str)
    def _onExtractErrored(self, log_id: int, msg: str):
        """处理提取错误"""
        # 清理任务信息
        if log_id in self.log_extract:
            task_info = self.log_extract.pop(log_id)
            task_info.thread.quit()
            task_info.thread.wait()

        InfoBar.error(
            title="提取失败",
            content=msg,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=5000,
            parent=self
        )

        self._onRefreshModel()

    @Slot(int)
    def _onViewLog(self, log_id: int):
        """处理查看日志请求"""
        item = self.model.getLog(log_id)
        if item:
            self._showToast(f"查看日志：{item.log_uri}")

    @Slot(int)
    def _onViewTemplate(self, log_id: int):
        """处理查看模板请求"""
        item = self.model.getLog(log_id)
        if item:
            self._showToast(f"查看模板：{item.log_uri}")

    @Slot(int)
    def _onDeleteLog(self, log_id: int):
        """处理删除日志请求"""
        item = self.model.getLog(log_id)
        if item is None:
            return

        confirm = MessageBox("确认删除", f"确定删除该日志吗？\n{item.log_uri}", self)
        if confirm.exec():
            # 如果有正在提取的任务，中断线程
            self._interruptExtractTask(log_id)

            try:
                self.log_service.delete_log(log_id)
                self.model.remove(log_id)
                InfoBar.success(
                    title="删除成功",
                    content="日志已删除",
                    orient=Qt.Orientation.Horizontal,
                    isClosable=True,
                    position=InfoBarPosition.TOP,
                    duration=3000,
                    parent=self
                )
            except Exception as exc:
                InfoBar.error(
                    title="删除失败",
                    content=str(exc),
                    orient=Qt.Orientation.Horizontal,
                    isClosable=True,
                    position=InfoBarPosition.TOP,
                    duration=5000,
                    parent=self
                )