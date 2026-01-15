import sqlite3
from pathlib import Path

from PyQt6.QtCore import (
    Qt,
    QObject,
    QThread,
    pyqtSlot,
    pyqtSignal
)
from PyQt6.QtWidgets import (
    QWidget,
    QHBoxLayout,
    QHeaderView,
    QVBoxLayout,
    QTableWidgetItem
)
from qfluentwidgets import (
    InfoBar,
    LineEdit,
    FluentIcon,
    MessageBox,
    PushButton,
    ProgressBar,
    TableWidget,
    InfoBarPosition,
    PrimaryPushButton
)

from .AddLogMessageBox import AddLogMessageBox
from .ExtractLogMessageBox import ExtractLogMessageBox
from modules.logparser import ParserFactory
from modules.log_source_record import LogSourceRecord
from modules.log_source_service import LogSourceService


class ExtractWorker(QObject):
    """日志提取工作线程"""

    finished = pyqtSignal(int)  # 提取完成信号，参数为行数
    error = pyqtSignal(str)  # 提取失败信号，参数为错误信息
    progress = pyqtSignal(int)  # 进度信号，参数为进度值 (0-100)

    def __init__(self, log_source: LogSourceRecord, algorithm: str, format_type: str, log_format: str, regex: list[str]):
        super().__init__()
        self.log_source = log_source
        self.algorithm = algorithm
        self.format_type = format_type
        self.log_format = log_format
        self.regex = regex
        self.log_source_service = LogSourceService()

    @pyqtSlot()
    def run(self):
        try:
            # 保存日志格式到数据库
            self.log_source_service.update_format_type(self.log_source.id, self.format_type)
            # 保存提取算法到数据库
            self.log_source_service.update_extract_method(self.log_source.id, self.algorithm)

            parser_type = ParserFactory.get_parser_type(self.algorithm)
            result = parser_type(
                Path(self.log_source.source_uri),
                self.log_format,
                self.regex,
                lambda: QThread.currentThread().isInterruptionRequested(),
                self.progress.emit
            ).parse()

            # 将日志设置为已提取
            self.log_source_service.update_is_extracted(self.log_source.id, True)
            # 保存日志行数
            self.log_source_service.update_line_count(self.log_source.id, result.line_count)

            self.finished.emit(result.line_count)
        except InterruptedError:
            print(f"Log extraction interrupted: {self.log_source.source_uri}")
            pass
        except Exception as e:
            self.error.emit(str(e))


class LogActionsWidget(QWidget):
    """日志操作按钮组件"""

    extract_clicked = pyqtSignal(LogSourceRecord)
    view_log_clicked = pyqtSignal(LogSourceRecord)
    view_template_clicked = pyqtSignal(LogSourceRecord)
    delete_clicked = pyqtSignal(LogSourceRecord)

    def __init__(self, log_source: LogSourceRecord, progress: int | None = None, parent=None):
        super().__init__(parent)
        self.setMinimumHeight(40)

        self.extract_button = PrimaryPushButton(FluentIcon.PENCIL_INK, "提取模板", self)
        self.view_log_button = PushButton(FluentIcon.VIEW, "查看日志", self)
        self.view_template_button = PushButton(FluentIcon.VIEW, "查看模板", self)
        self.delete_button = PrimaryPushButton(FluentIcon.DELETE, "删除", self)
        self.progress_bar = ProgressBar(self)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(4)

        # 按钮行
        button_layout = QHBoxLayout()
        button_layout.setSpacing(4)

        self.extract_button.setFixedHeight(28)
        # 检查是否已提取
        if log_source.is_extracted:
            self.extract_button.setEnabled(False)
            self.extract_button.setText("已提模板")

        self.view_log_button.setFixedHeight(28)
        self.view_log_button.setEnabled(log_source.is_extracted)

        self.view_template_button.setFixedHeight(28)
        self.view_template_button.setEnabled(log_source.is_extracted)

        self.delete_button.setFixedHeight(28)

        button_layout.addWidget(self.extract_button)
        button_layout.addWidget(self.view_log_button)
        button_layout.addWidget(self.view_template_button)
        button_layout.addWidget(self.delete_button)

        layout.addLayout(button_layout)

        self.progress_bar.setRange(0, 100)
        self.progress_bar.setFixedHeight(4)
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        self.extract_button.clicked.connect(lambda: self.extract_clicked.emit(log_source))
        self.view_log_button.clicked.connect(lambda: self.view_log_clicked.emit(log_source))
        self.view_template_button.clicked.connect(lambda: self.view_template_clicked.emit(log_source))
        self.delete_button.clicked.connect(lambda: self.delete_clicked.emit(log_source))

        # 如果有进度，表示正在提取
        if progress is not None:
            self.extract_button.setEnabled(False)
            self.extract_button.setText("正在提取")
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(progress)

    def set_progress(self, progress: int):
        self.extract_button.setEnabled(False)
        self.extract_button.setText("正在提取")
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(progress)


class LogManagePage(QWidget):
    """日志管理页面"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("LogManagePage")

        self.log_source_service = LogSourceService()
        # 存储日志源列表
        self.log_sources: list[LogSourceRecord] = []
        # 存储日志操作组件
        self.log_actions: dict[int, LogActionsWidget] = {}
        # 存储正在提取的任务进度
        self.log_progress: dict[int, int] = {}
        # 存储工作线程和 worker 的引用，防止被 GC 回收
        self.log_extract_threads: dict[int, tuple[QThread, ExtractWorker]] = {}

        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(24, 24, 24, 24)
        self.main_layout.setSpacing(16)

        self._initToolbar()
        self._initLogTable()

        # 刷新表格
        self._refreshLogTable()

    def _initToolbar(self):
        tool_bar_layout = QHBoxLayout()
        tool_bar_layout.setSpacing(10)

        self.search_input = LineEdit(self)
        self.search_input.setPlaceholderText("按名称或路径搜索")
        self.search_input.setClearButtonEnabled(True)
        self.search_input.setFixedHeight(36)

        self.refresh_button = PushButton(FluentIcon.SYNC, "刷新", self)
        self.refresh_button.setFixedHeight(36)
        self.refresh_button.clicked.connect(self._refreshLogTable)

        self.add_button = PrimaryPushButton(FluentIcon.ADD, "新增日志", self)
        self.add_button.setFixedHeight(36)
        self.add_button.clicked.connect(self._onAddLog)

        tool_bar_layout.addWidget(self.search_input, 1)
        tool_bar_layout.addStretch(1)
        tool_bar_layout.addWidget(self.refresh_button)
        tool_bar_layout.addWidget(self.add_button)

        self.main_layout.addLayout(tool_bar_layout)

    def _initLogTable(self):
        self.log_source_table = TableWidget(self)
        self.log_source_table.setBorderVisible(True)
        self.log_source_table.setBorderRadius(8)
        self.log_source_table.setWordWrap(False)
        self.log_source_table.setColumnCount(7)
        self.log_source_table.setHorizontalHeaderLabels(["类型", "日志格式", "URI", "创建时间", "提取方法", "行数", "操作"])
        self.log_source_table.verticalHeader().hide()
        self.log_source_table.setEditTriggers(self.log_source_table.EditTrigger.NoEditTriggers)

        self.main_layout.addWidget(self.log_source_table)

    def _syncLogSources(self):
        self.log_sources = self.log_source_service.get_all_logs()

    def _syncLogActions(self):
        self.log_actions.clear()
        for log_source in self.log_sources:
            actions_widget = LogActionsWidget(log_source, self.log_progress.get(log_source.id), self.log_source_table)
            actions_widget.extract_clicked.connect(self._onExtractLog)
            actions_widget.view_log_clicked.connect(lambda ls=log_source: self._showToast(f"查看日志：{ls.source_uri}"))
            actions_widget.view_template_clicked.connect(lambda ls=log_source: self._showToast(f"查看模板：{ls.source_uri}"))
            actions_widget.delete_clicked.connect(self._onDeleteLog)
            self.log_actions[log_source.id] = actions_widget

    def _renderLogTable(self):
        self.log_source_table.setRowCount(len(self.log_sources))
        for row_index, log_source in enumerate(self.log_sources):
            source_type_item = QTableWidgetItem(log_source.source_type)
            source_type_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            format_type_item = QTableWidgetItem(log_source.format_type if log_source.format_type is not None else "—")
            format_type_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            source_uri_item = QTableWidgetItem(log_source.source_uri)
            source_uri_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            create_time_item = QTableWidgetItem(log_source.create_time.isoformat(" ", "seconds"))
            create_time_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            extract_method_item = QTableWidgetItem(log_source.extract_method if log_source.extract_method is not None else "—")
            extract_method_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            line_count_item = QTableWidgetItem(f"{log_source.line_count:,}" if log_source.line_count is not None else "—")
            line_count_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            self.log_source_table.setItem(row_index, 0, source_type_item)
            self.log_source_table.setItem(row_index, 1, format_type_item)
            self.log_source_table.setItem(row_index, 2, source_uri_item)
            self.log_source_table.setItem(row_index, 3, create_time_item)
            self.log_source_table.setItem(row_index, 4, extract_method_item)
            self.log_source_table.setItem(row_index, 5, line_count_item)
            self.log_source_table.setCellWidget(row_index, 6, self.log_actions[log_source.id])

        self.log_source_table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.log_source_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.log_source_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)

    @pyqtSlot()
    def _refreshLogTable(self):
        self._syncLogSources()
        self._syncLogActions()
        self._renderLogTable()

    @pyqtSlot()
    def _onAddLog(self):
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
                self._refreshLogTable()
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
                self._refreshLogTable()
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

    @pyqtSlot(object)
    def _onExtractLog(self, log_source: LogSourceRecord):
        dialog = ExtractLogMessageBox(self)
        if dialog.exec():
            if dialog.is_custom_mode:
                dialog.format_config_manager.save_custom_format(dialog.selected_format_type, dialog.selected_log_format, dialog.selected_regex)

            # 把任务标记为正在提取，初始进度为 0
            self.log_progress[log_source.id] = 0
            # 更新操作组件状态
            self.log_actions[log_source.id].set_progress(0)

            # 创建工作线程和 worker
            thread = QThread()
            worker = ExtractWorker(
                log_source,
                dialog.selected_algorithm,
                dialog.selected_format_type,
                dialog.selected_log_format,
                dialog.selected_regex
            )
            worker.moveToThread(thread)

            # 保存引用，防止被 GC 回收
            self.log_extract_threads[log_source.id] = (thread, worker)

            # 连接信号
            thread.started.connect(worker.run)
            worker.progress.connect(lambda p: self._onExtractProgress(log_source, p))
            worker.finished.connect(lambda line_count: self._onExtractFinished(log_source, True, f"共 {line_count:,} 行"))
            worker.error.connect(lambda msg: self._onExtractFinished(log_source, False, msg))

            # 启动线程
            thread.start()

    @pyqtSlot(object, int)
    def _onExtractProgress(self, log_source: LogSourceRecord, progress: int):
        self.log_progress[log_source.id] = progress
        self.log_actions[log_source.id].set_progress(progress)

    @pyqtSlot(object, bool, str)
    def _onExtractFinished(self, log_source: LogSourceRecord, success: bool, msg: str):
        # 取消提取标记
        self.log_progress.pop(log_source.id)

        # 清理线程
        thread, worker = self.log_extract_threads.pop(log_source.id)
        thread.quit()
        thread.wait()

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

        # 刷新表格
        self._refreshLogTable()

    @pyqtSlot(object)
    def _onDeleteLog(self, log_source: LogSourceRecord):
        confirm = MessageBox("确认删除", f"确定删除该日志源吗？\n{log_source.source_uri}", self)
        if confirm.exec():
            # 如果有正在提取的任务，安全中断线程
            if log_source.id in self.log_extract_threads:
                thread, worker = self.log_extract_threads.pop(log_source.id)
                thread.requestInterruption()
                thread.quit()
                thread.wait()
                self.log_progress.pop(log_source.id, None)
            try:
                self.log_source_service.delete_log(log_source.id)
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
            self._refreshLogTable()

    @pyqtSlot(str)
    def _showToast(self, text: str):
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
        return len(self.log_extract_threads) > 0

    def interruptAllExtractingTasks(self):
        for log_source_id, (thread, worker) in self.log_extract_threads.items():
            thread.requestInterruption()
            thread.quit()
            thread.wait()
        self.log_extract_threads.clear()
        self.log_progress.clear()