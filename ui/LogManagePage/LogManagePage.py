import sqlite3
import asyncio
import time
from pathlib import Path
from qasync import asyncSlot

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QTableWidgetItem, QHeaderView
from qfluentwidgets import MessageBox, TableWidget, LineEdit, PushButton, PrimaryPushButton, FluentIcon, InfoBar, InfoBarPosition, IndeterminateProgressBar

from .AddLogMessageBox import AddLogMessageBox
from .ExtractLogMessageBox import ExtractLogMessageBox
from modules.logparser import ParserFactory
from modules.log_source_record import LogSourceRecord
from modules.log_source_service import LogSourceService


class LogManagePage(QWidget):
    """日志管理页面"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("LogManagePage")

        self.log_source_service = LogSourceService()
        # 存储正在提取的任务相关的UI组件
        self.extracting_log: set[int] = set()
        # 缓存列表数据与行索引，避免仅刷新按钮时重复查询
        self.log_sources: list[LogSourceRecord] = []

        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(24, 24, 24, 24)
        self.main_layout.setSpacing(16)

        self._initToolbar()
        self._initLogTable()
        self._syncLogTable()

    def _initToolbar(self):
        tool_bar_layout = QHBoxLayout()
        tool_bar_layout.setSpacing(10)

        self.search_input = LineEdit(self)
        self.search_input.setPlaceholderText("按名称或路径搜索")
        self.search_input.setClearButtonEnabled(True)
        self.search_input.setFixedHeight(36)

        self.refresh_button = PushButton(FluentIcon.SYNC, "刷新", self)
        self.refresh_button.setFixedHeight(36)
        self.refresh_button.clicked.connect(self._syncLogTable)

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
        self.log_source_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.log_source_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)

        self.main_layout.addWidget(self.log_source_table)

    def _syncLogTable(self):
        self.log_sources = self.log_source_service.get_all_logs()
        self._renderLogTable()

    def _renderLogTable(self):
        self.log_source_table.setRowCount(len(self.log_sources))
        for row_index, log_source in enumerate(self.log_sources):
            source_type_item = QTableWidgetItem(log_source.source_type)
            source_type_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            format_type_item = QTableWidgetItem(log_source.format_type if log_source.format_type is not None else "—")
            format_type_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            source_uri_item = QTableWidgetItem(log_source.source_uri)
            source_uri_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            create_time_item = QTableWidgetItem(log_source.create_time.isoformat(timespec="seconds"))
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
            self.log_source_table.setCellWidget(row_index, 6, self._buildRowActions(row_index, log_source))

        self.log_source_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.log_source_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)

    def _refreshRowActions(self, row_index: int):
        if row_index is None or row_index < 0 or row_index >= len(self.log_sources):
            return
        log_source = self.log_sources[row_index]
        self.log_source_table.setCellWidget(row_index, 6, self._buildRowActions(row_index, log_source))

    def _buildRowActions(self, row_index: int, log_source: LogSourceRecord) -> QWidget:
        container = QWidget(self.log_source_table)
        layout = QVBoxLayout(container)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(4)

        # 按钮行
        button_layout = QHBoxLayout()
        button_layout.setSpacing(6)

        extract_button = PrimaryPushButton(FluentIcon.PENCIL_INK, "提取模板", container)
        extract_button.setFixedHeight(28)

        # 检查是否已提取
        if log_source.is_extracted:
            extract_button.setEnabled(False)
            extract_button.setText("已提取")
        else:
            # 只对本地文件启用提取功能
            if log_source.source_type == "本地文件":
                extract_button.clicked.connect(lambda: self._onExtractLog(row_index, log_source))
            else:
                extract_button.clicked.connect(lambda: self._showToast("提取网络日志"))

        view_log_button = PushButton(FluentIcon.VIEW, "查看日志", container)
        view_log_button.setFixedHeight(28)
        view_log_button.clicked.connect(lambda: self._showToast("查看日志"))
        view_log_button.setEnabled(log_source.is_extracted)

        view_template_button = PushButton(FluentIcon.VIEW, "查看模板", container)
        view_template_button.setFixedHeight(28)
        view_template_button.clicked.connect(lambda: self._showToast("查看模板"))
        view_template_button.setEnabled(log_source.is_extracted)

        delete_button = PrimaryPushButton(FluentIcon.DELETE, "删除", container)
        delete_button.setFixedHeight(28)
        delete_button.clicked.connect(lambda: self._onDeleteLog(row_index, log_source))

        button_layout.addWidget(extract_button)
        button_layout.addWidget(view_log_button)
        button_layout.addWidget(view_template_button)
        button_layout.addWidget(delete_button)
        button_layout.addStretch(1)

        layout.addLayout(button_layout)

        # 如果正在提取，添加进度条
        if log_source.id in self.extracting_log:
            extract_button.setEnabled(False)
            extract_button.setText("正在提取")
            progress_bar = IndeterminateProgressBar(container)
            progress_bar.setFixedHeight(4)
            layout.addWidget(progress_bar)

        return container

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
                self._syncLogTable()
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
                self._syncLogTable()
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

    @asyncSlot()
    async def _onExtractLog(self, row_index: int, log_source: LogSourceRecord):
        dialog = ExtractLogMessageBox(self)
        if dialog.exec():
            if dialog.is_custom_mode:
                dialog.format_config_manager.save_custom_format(dialog.selected_format_name, dialog.selected_log_format, dialog.selected_regex)

            # 把任务标记为正在提取
            self.extracting_log.add(log_source.id)
            # 更新UI
            self._refreshRowActions(row_index)

            # 使用 asyncio.to_thread 异步执行模板提取任务
            try:
                await asyncio.to_thread(lambda: self._extractLog(log_source, dialog.selected_algorithm, dialog.selected_log_format, dialog.selected_regex))
                success = True
                message = "提取成功"
            except Exception as e:
                success = False
                message = str(e)

            # 取消提取标记
            self.extracting_log.remove(log_source.id)
            # 更新UI
            self._refreshRowActions(row_index)

            if success:
                InfoBar.success(
                    title="提取成功",
                    content="日志模板提取完成",
                    orient=Qt.Orientation.Horizontal,
                    isClosable=True,
                    position=InfoBarPosition.TOP,
                    duration=3000,
                    parent=self,
                )
            else:
                InfoBar.error(
                    title="提取失败",
                    content=message,
                    orient=Qt.Orientation.Horizontal,
                    isClosable=True,
                    position=InfoBarPosition.TOP,
                    duration=5000,
                    parent=self,
                )

    def _onDeleteLog(self, row_index: int, log_source: LogSourceRecord):
        confirm = MessageBox("确认删除", f"确定删除该日志源吗？\n{log_source.source_uri}", self)
        if confirm.exec():
            try:
                self.log_source_service.delete_log(log_source.id)
                del self.log_sources[row_index]
                self.log_source_table.removeRow(row_index)
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

    def _extractLog(self, log_source: LogSourceRecord, algorithm: str, log_format: str, regex: list[str]):
        # TODO: 保存日志格式到数据库
        # LogSourceService().update_format_type(self.log_source_id, self.format_data["name"])

        time.sleep(10)
        parser_type = ParserFactory.get_parser_type(algorithm)
        result = parser_type(Path(log_source.source_uri), log_format, regex).parse()

        # TODO: 将结果保存到数据库
        # print(f"提取结果: {result}")