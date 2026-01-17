import sqlite3
from pathlib import Path

from PyQt6.QtCore import Qt, QObject, QThread, pyqtSlot, pyqtSignal
from PyQt6.QtWidgets import QWidget, QHBoxLayout, QHeaderView, QVBoxLayout
from qfluentwidgets import (
    InfoBar,
    FluentIcon,
    MessageBox,
    PushButton,
    TableView,
    SearchLineEdit,
    InfoBarPosition,
    PrimaryPushButton
)

from .LogSourceTableModel import LogSourceTableModel, LogSourceColumn
from .LogSourceDelegate import ProgressBarDelegate, ActionButtonDelegate
from .AddLogMessageBox import AddLogMessageBox
from .ExtractLogMessageBox import ExtractLogMessageBox
from modules.logparser import ParserFactory
from modules.log_source_service import LogSourceService


class ExtractTask(QObject):
    """日志提取工作线程"""

    finished = pyqtSignal(int)  # 提取完成信号，参数为行数
    error = pyqtSignal(str)  # 提取失败信号，参数为错误信息
    progress = pyqtSignal(int)  # 进度信号，参数为进度值 (0-100)

    def __init__(self, log_id: int, log_file: Path, algorithm: str, format_type: str, log_format: str, regex: list[str]):
        super().__init__()
        self.log_id = log_id
        self.log_file = log_file
        self.algorithm = algorithm
        self.format_type = format_type
        self.log_format = log_format
        self.regex = regex
        self.log_source_service = LogSourceService()

    @pyqtSlot()
    def run(self):
        try:
            # 保存日志格式到数据库
            self.log_source_service.update_format_type(self.log_id, self.format_type)
            # 保存提取算法到数据库
            self.log_source_service.update_extract_method(self.log_id, self.algorithm)

            parser_type = ParserFactory.get_parser_type(self.algorithm)
            result = parser_type(
                self.log_file,
                self.log_format,
                self.regex,
                lambda: QThread.currentThread().isInterruptionRequested(),
                self.progress.emit
            ).parse()

            # 将日志设置为已提取
            self.log_source_service.update_is_extracted(self.log_id, True)
            # 保存日志行数
            self.log_source_service.update_line_count(self.log_id, result.line_count)

            # TODO：保存提取的模板内容

            # 提取完成
            self.finished.emit(result.line_count)
        except InterruptedError as e:
            print(f"Log extraction interrupted: {self.log_file}")
            self.error.emit("日志提取已中断")
        except Exception as e:
            self.error.emit(str(e))


class ExtractTaskInfo:
    """日志提取任务信息"""

    def __init__(self, thread: QThread, task: ExtractTask):
        self.thread = thread
        self.task = task
        self.progress: int = 0


class LogManagePage(QWidget):
    """日志管理页面 - 使用MVD架构"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("LogManagePage")

        self.log_source_service = LogSourceService()

        # 存储正在提取的任务信息: log_id -> ExtractingTaskInfo
        self.log_extracting: dict[int, ExtractTaskInfo] = {}

        # 主布局
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(24, 24, 24, 24)
        self.main_layout.setSpacing(16)

        # 初始化模型
        self._initModel()

        self._initToolbar()
        self._initTableView()

        # 刷新表格数据
        self._refreshModel()

    def _initToolbar(self):
        """初始化工具栏"""
        tool_bar_layout = QHBoxLayout()
        tool_bar_layout.setSpacing(10)

        self.search_input = SearchLineEdit(self)
        self.search_input.setPlaceholderText("按URI搜索")
        self.search_input.setClearButtonEnabled(True)
        self.search_input.setFixedHeight(36)
        self.search_input.searchSignal.connect(self._onSearchLog)
        self.search_input.clearSignal.connect(self._refreshModel)

        self.refresh_button = PushButton(FluentIcon.SYNC, "刷新", self)
        self.refresh_button.setFixedHeight(36)
        self.refresh_button.clicked.connect(self._refreshModel)

        self.add_button = PrimaryPushButton(FluentIcon.ADD, "新增日志", self)
        self.add_button.setFixedHeight(36)
        self.add_button.clicked.connect(self._onAddLog)

        tool_bar_layout.addWidget(self.search_input, 1)
        tool_bar_layout.addStretch(1)
        tool_bar_layout.addWidget(self.refresh_button)
        tool_bar_layout.addWidget(self.add_button)

        self.main_layout.addLayout(tool_bar_layout)

    def _initModel(self):
        """初始化数据模型"""
        self.model = LogSourceTableModel(self)
        # 连接模型信号
        self.model.extract.connect(self._onExtractLog)
        self.model.viewLog.connect(self._onViewLog)
        self.model.viewTemplate.connect(self._onViewTemplate)
        self.model.delete.connect(self._onDeleteLog)

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

        # 设置选择行为
        # self.table_view.setSelectionBehavior(TableView.SelectionBehavior.SelectRows)
        # self.table_view.setSelectionMode(TableView.SelectionMode.SingleSelection)

        # 设置列委托
        self.progress_delegate = ProgressBarDelegate(self.table_view)
        self.action_delegate = ActionButtonDelegate(self.model, self.table_view)
        self.table_view.setItemDelegateForColumn(LogSourceColumn.PROGRESS, self.progress_delegate)
        self.table_view.setItemDelegateForColumn(LogSourceColumn.ACTIONS, self.action_delegate)

        self.main_layout.addWidget(self.table_view)

    def _openPersistentEditors(self):
        """为所有行开启持久编辑器"""
        for row in range(self.model.rowCount()):
            # 进度条列
            progress_index = self.model.index(row, LogSourceColumn.PROGRESS)
            self.table_view.openPersistentEditor(progress_index)

            # 操作按钮列
            action_index = self.model.index(row, LogSourceColumn.ACTIONS)
            self.table_view.openPersistentEditor(action_index)

    def _updateTableLayout(self):
        """更新表格布局"""
        header = self.table_view.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        # 固定进度列和操作列宽度
        header.setSectionResizeMode(LogSourceColumn.PROGRESS, QHeaderView.ResizeMode.Fixed)
        self.table_view.setColumnWidth(LogSourceColumn.PROGRESS, 120)
        header.setSectionResizeMode(LogSourceColumn.ACTIONS, QHeaderView.ResizeMode.ResizeToContents)

        # 行高自适应
        self.table_view.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)

    def _getLogProgress(self) -> dict[int, int]:
        """获取正在提取的任务进度映射"""
        return {task_id: info.progress for task_id, info in self.log_extracting.items()}

    @pyqtSlot()
    def _refreshModel(self):
        """刷新表格数据"""
        logs = self.log_source_service.get_all_logs()
        log_progress = self._getLogProgress()
        self.model.setLogSources(logs, log_progress)
        self._updateTableLayout()
        self._openPersistentEditors()

    @pyqtSlot(str)
    def _onSearchLog(self, keyword: str):
        """搜索日志"""
        keyword = keyword.strip()
        if not keyword:
            self._refreshModel()
            return

        filtered_logs = self.log_source_service.search_by_uri(keyword)
        extracting_progress = self._getLogProgress()
        self.model.setLogSources(filtered_logs, extracting_progress)
        self._updateTableLayout()

    @pyqtSlot()
    def _onAddLog(self):
        """新增日志"""
        dialog = AddLogMessageBox(self)
        if dialog.exec():
            if dialog.selected_file_path:
                try:
                    self.log_source_service.add_local_log(dialog.selected_file_path)
                    InfoBar.success(
                        title="导入成功",
                        content="本地日志已导入",
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=3000,
                        parent=self,
                    )
                except sqlite3.IntegrityError:
                    InfoBar.warning(
                        title="导入失败",
                        content="该日志文件已存在",
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=4000,
                        parent=self,
                    )
                except Exception as exc:
                    InfoBar.error(
                        title="导入失败",
                        content=str(exc),
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=5000,
                        parent=self,
                    )
                self._refreshModel()
                return

            if dialog.url_input.text():
                try:
                    self.log_source_service.add_network_log(dialog.url_input.text().strip())
                    InfoBar.success(
                        title="导入成功",
                        content="网络日志源已添加",
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=3000,
                        parent=self,
                    )
                except sqlite3.IntegrityError:
                    InfoBar.warning(
                        title="导入失败",
                        content="该网络日志源已存在",
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=4000,
                        parent=self,
                    )
                except Exception as exc:
                    InfoBar.error(
                        title="导入失败",
                        content=str(exc),
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=5000,
                        parent=self,
                    )
                self._refreshModel()
                return

            InfoBar.warning(
                title="未选择文件",
                content="请选择UDP/TCP日志源或本地日志文件",
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=4000,
                parent=self,
            )

    @pyqtSlot(int)
    def _onExtractLog(self, log_id: int):
        """处理提取日志请求"""
        item = self.model.getItemById(log_id)
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
            task = ExtractTask(
                log_id,
                Path(item.source_uri),
                dialog.selected_algorithm,
                dialog.selected_format_type,
                dialog.selected_log_format,
                dialog.selected_regex
            )
            task.moveToThread(thread)

            # 保存任务信息
            task_info = ExtractTaskInfo(thread, task)
            self.log_extracting[log_id] = task_info

            # 更新模型状态
            self.model.setExtracting(log_id, True, 0)

            # 连接信号
            thread.started.connect(task.run)
            task.progress.connect(lambda p, lid=log_id: self._onExtractProgress(lid, p))
            task.finished.connect(lambda line_count, lid=log_id: self._onExtractFinished(lid, True, f"共 {line_count:,} 行"))
            task.error.connect(lambda msg, lid=log_id: self._onExtractFinished(lid, False, msg))

            # 启动线程
            thread.start()

    @pyqtSlot(int, int)
    def _onExtractProgress(self, log_id: int, progress: int):
        """处理提取进度更新"""
        if log_id in self.log_extracting:
            self.log_extracting[log_id].progress = progress
        # 更新模型，触发UI刷新
        self.model.updateProgress(log_id, progress)

    @pyqtSlot(int, bool, str)
    def _onExtractFinished(self, log_id: int, success: bool, msg: str):
        """处理提取完成"""
        # 清理任务信息
        if log_id in self.log_extracting:
            task_info = self.log_extracting.pop(log_id)
            task_info.thread.quit()
            task_info.thread.wait()

        # 更新模型状态
        self.model.setExtracting(log_id, False)

        if success:
            InfoBar.success(
                title="提取成功",
                content=f"日志模板提取完成，{msg}",
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=3000,
                parent=self,
            )
        else:
            InfoBar.error(
                title="提取失败",
                content=msg,
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=5000,
                parent=self,
            )

        # 刷新数据以获取最新状态
        self._refreshModel()

    @pyqtSlot(int)
    def _onViewLog(self, log_id: int):
        """处理查看日志请求"""
        item = self.model.getItemById(log_id)
        if item:
            self._showToast(f"查看日志：{item.source_uri}")

    @pyqtSlot(int)
    def _onViewTemplate(self, log_id: int):
        """处理查看模板请求"""
        item = self.model.getItemById(log_id)
        if item:
            self._showToast(f"查看模板：{item.source_uri}")

    @pyqtSlot(int)
    def _onDeleteLog(self, log_id: int):
        """处理删除日志请求"""
        item = self.model.getItemById(log_id)
        if item is None:
            return

        confirm = MessageBox("确认删除", f"确定删除该日志源吗？\n{item.source_uri}", self)
        if confirm.exec():
            # 如果有正在提取的任务，安全中断线程
            if log_id in self.log_extracting:
                task_info = self.log_extracting.pop(log_id)
                task_info.thread.requestInterruption()
                task_info.thread.quit()
                task_info.thread.wait()

            try:
                self.log_source_service.delete_log(log_id)
                InfoBar.success(
                    title="删除成功",
                    content="日志源已删除",
                    orient=Qt.Orientation.Horizontal,
                    isClosable=True,
                    position=InfoBarPosition.TOP,
                    duration=3000,
                    parent=self,
                )
            except Exception as exc:
                InfoBar.error(
                    title="删除失败",
                    content=str(exc),
                    orient=Qt.Orientation.Horizontal,
                    isClosable=True,
                    position=InfoBarPosition.TOP,
                    duration=5000,
                    parent=self,
                )

            self._refreshModel()

    def _showToast(self, text: str):
        """显示提示信息"""
        InfoBar.info(
            title=text,
            content="功能待实现",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )

    def hasExtractingTasks(self) -> bool:
        """是否有正在提取的任务"""
        return len(self.log_extracting) > 0

    def interruptAllExtractingTasks(self):
        """中断所有正在提取的任务"""
        for log_id, task_info in self.log_extracting.items():
            task_info.thread.requestInterruption()
            task_info.thread.quit()
            task_info.thread.wait()
        self.log_extracting.clear()