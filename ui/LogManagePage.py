import os
import sqlite3
import time
from pathlib import Path

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QFileDialog,
    QTableWidgetItem,
    QHeaderView,
)
from qfluentwidgets import (
    MessageBox,
    TableWidget,
    CardWidget,
    SubtitleLabel,
    BodyLabel,
    LineEdit,
    PushButton,
    PrimaryPushButton,
    FluentIcon,
    InfoBar,
    MessageBoxBase,
    InfoBarPosition,
    ToolTipFilter,
    ToolTipPosition,
    ComboBox,
    IndeterminateProgressBar,
)

from modules.log_source_record import LogSourceRecord
from modules.log_source_service import LogSourceService
from modules.logparser.parser_factory import ParserFactory
from modules.logparser.base_log_parser import ParseResult


class AddLogMessageBox(MessageBoxBase):
    """新增日志对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.selected_file_path: str | None = None

        self.viewLayout.addWidget(self._createNetworkCard())
        self.viewLayout.addWidget(self._createLocalFileCard())
        self.widget.setMinimumWidth(700)

    def _createNetworkCard(self) -> CardWidget:
        card = CardWidget(self)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(18, 16, 18, 16)
        card_layout.setSpacing(10)

        title_layout = QHBoxLayout(card)
        icon_label = QLabel(self)
        icon_label.setPixmap(FluentIcon.GLOBE.icon().pixmap(20, 20))
        title_label = SubtitleLabel("网络日志源", card)
        title_label.setStyleSheet("font-size: 16px; font-weight: 600;")

        title_layout.addWidget(icon_label)
        title_layout.addSpacing(6)
        title_layout.addWidget(title_label)
        title_layout.addStretch(1)

        card_layout.addLayout(title_layout)

        url_label = BodyLabel("Syslog 服务器地址：", card)
        url_label.setStyleSheet("font-weight: 500;")
        card_layout.addWidget(url_label)

        self.url_input = LineEdit(card)
        self.url_input.setPlaceholderText("例如：syslog://192.168.1.100:514")
        self.url_input.setClearButtonEnabled(True)
        self.url_input.setFixedHeight(36)
        card_layout.addWidget(self.url_input)

        hint_label = BodyLabel("支持 UDP/TCP，默认端口 514", card)
        hint_label.setStyleSheet("color: #888; font-size: 12px;")
        card_layout.addWidget(hint_label)

        return card

    def _createLocalFileCard(self) -> CardWidget:
        card = CardWidget(self)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(18, 16, 18, 16)
        card_layout.setSpacing(10)

        title_layout = QHBoxLayout(card)
        icon_label = QLabel(self)
        icon_label.setPixmap(FluentIcon.FOLDER.icon().pixmap(20, 20))
        title_label = SubtitleLabel("本地文件源", card)
        title_label.setStyleSheet("font-size: 16px; font-weight: 600;")

        title_layout.addWidget(icon_label)
        title_layout.addSpacing(6)
        title_layout.addWidget(title_label)
        title_layout.addStretch(1)

        card_layout.addLayout(title_layout)

        file_label = BodyLabel("选择日志文件：", card)
        file_label.setStyleSheet("font-weight: 500;")
        card_layout.addWidget(file_label)

        select_file_layout = QHBoxLayout()
        self.select_file_button = PushButton(FluentIcon.FOLDER_ADD, "选择文件", card)
        self.select_file_button.setFixedHeight(36)
        self.select_file_button.clicked.connect(self._onSelectFile)

        self.file_path_label = BodyLabel("未选择文件", card)
        self.file_path_label.setStyleSheet("color: #888; padding-left: 8px;")

        select_file_layout.addWidget(self.select_file_button)
        select_file_layout.addWidget(self.file_path_label)
        select_file_layout.addStretch(1)

        card_layout.addLayout(select_file_layout)

        hint_label = BodyLabel("支持 .log, .txt 等文本格式", card)
        hint_label.setStyleSheet("color: #888; font-size: 12px;")
        card_layout.addWidget(hint_label)

        return card

    def _onSelectFile(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "选择日志文件",
            "",
            "日志文件 (*.log *.txt);;所有文件 (*.*)",
        )

        if file_path:
            self.selected_file_path = file_path
            self.file_path_label.setText(os.path.basename(file_path))
            self.file_path_label.setStyleSheet("color: #0078d4; padding-left: 8px; font-weight: 500;")
            self.file_path_label.setToolTip(file_path)
            self.file_path_label.installEventFilter(
                ToolTipFilter(self.file_path_label, showDelay=300, position=ToolTipPosition.RIGHT))


class ExtractLogMessageBox(MessageBoxBase):
    """选择提取算法对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.selected_algorithm: str | None = None

        self.viewLayout.addWidget(self._createAlgorithmCard())
        self.widget.setMinimumWidth(400)

    def _createAlgorithmCard(self) -> CardWidget:
        card = CardWidget(self)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(18, 16, 18, 16)
        card_layout.setSpacing(10)

        title_layout = QHBoxLayout(card)
        icon_label = QLabel(self)
        icon_label.setPixmap(FluentIcon.DEVELOPER_TOOLS.icon().pixmap(20, 20))
        title_label = SubtitleLabel("选择提取算法", card)
        title_label.setStyleSheet("font-size: 16px; font-weight: 600;")

        title_layout.addWidget(icon_label)
        title_layout.addSpacing(6)
        title_layout.addWidget(title_label)
        title_layout.addStretch(1)

        card_layout.addLayout(title_layout)

        algorithm_label = BodyLabel("提取算法：", card)
        algorithm_label.setStyleSheet("font-weight: 500;")
        card_layout.addWidget(algorithm_label)

        self.algorithm_combo_box = ComboBox(card)
        self.algorithm_combo_box.setFixedHeight(36)

        # 获取所有的解析器列表
        all_parsers = ParserFactory.get_all_parsers_name()
        self.algorithm_combo_box.addItems(all_parsers)
        if all_parsers:
            self.algorithm_combo_box.setCurrentIndex(0)
            self.selected_algorithm = all_parsers[0]
        self.algorithm_combo_box.currentTextChanged.connect(self._onAlgorithmChanged)
        card_layout.addWidget(self.algorithm_combo_box)

        hint_label = BodyLabel(ParserFactory.get_parser_description(self.selected_algorithm), card)
        hint_label.setStyleSheet("color: #888; font-size: 12px;")
        card_layout.addWidget(hint_label)

        return card

    def _onAlgorithmChanged(self, text: str):
        self.selected_algorithm = text


class ExtractWorker(QThread):
    finished = pyqtSignal(int, bool, ParseResult, str)  # log_source_id, success, result, message

    def __init__(self, log_source_id: int, algorithm: str, parent=None):
        super().__init__(parent)
        self.log_source_id = log_source_id
        self.algorithm = algorithm

    def run(self):
        try:
            # 获取日志源信息
            log_source = LogSourceService().get_log(self.log_source_id)
            if log_source is None:
                raise ValueError(f"日志源不存在: {self.log_source_id}")

            time.sleep(10)

            # 获取解析器并解析
            parser_type = ParserFactory.get_parser_type(self.algorithm)
            result = parser_type(Path(log_source.source_uri), _, _).parse

            self.finished.emit(self.log_source_id, True, result, "提取成功")
        except Exception as e:
            self.finished.emit(self.log_source_id, False, None, str(e))


class LogManagePage(QWidget):
    """日志管理页面"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("LogManagePage")

        self.log_source_service = LogSourceService()
        # self.extractService = LogExtractService()
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(24, 24, 24, 24)
        self.main_layout.setSpacing(16)

        # 存储正在提取的任务相关的UI组件
        self.extracting_widgets: dict[
            int, dict] = {}  # {log_source_id: {"button": btn, "progress": bar, "worker": worker}}

        self._initToolbar()
        self._initLogTable()
        self._loadLogSources()

    def _initToolbar(self):
        tool_bar_layout = QHBoxLayout(self)
        tool_bar_layout.setSpacing(10)

        self.search_input = LineEdit(self)
        self.search_input.setPlaceholderText("按名称或路径搜索")
        self.search_input.setClearButtonEnabled(True)
        self.search_input.setFixedHeight(36)

        self.refresh_button = PushButton(FluentIcon.SYNC, "刷新", self)
        self.refresh_button.setFixedHeight(36)
        self.refresh_button.clicked.connect(self._onRefresh)

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
        self.log_source_table.setColumnCount(6)
        self.log_source_table.setHorizontalHeaderLabels(["类型", "URI", "创建时间", "提取方法", "行数", "操作"])
        self.log_source_table.verticalHeader().hide()
        self.log_source_table.setEditTriggers(self.log_source_table.EditTrigger.NoEditTriggers)
        self.log_source_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.log_source_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)

        self.main_layout.addWidget(self.log_source_table)

    def _loadLogSources(self):
        log_sources = self.log_source_service.get_all_logs()
        self.log_source_table.setRowCount(len(log_sources))
        for index, log_source in enumerate(log_sources):
            source_type_item = QTableWidgetItem(log_source.source_type)
            source_type_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            source_uri_item = QTableWidgetItem(log_source.source_uri)
            source_uri_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            create_time_item = QTableWidgetItem(log_source.create_time.isoformat(timespec="seconds"))
            create_time_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            extract_method_item = QTableWidgetItem(
                log_source.extract_method if log_source.extract_method is not None else "—")
            extract_method_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            line_count_item = QTableWidgetItem(
                f"{log_source.line_count:,}" if log_source.line_count is not None else "—")
            line_count_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            self.log_source_table.setItem(index, 0, source_type_item)
            self.log_source_table.setItem(index, 1, source_uri_item)
            self.log_source_table.setItem(index, 2, create_time_item)
            self.log_source_table.setItem(index, 3, extract_method_item)
            self.log_source_table.setItem(index, 4, line_count_item)
            self.log_source_table.setCellWidget(index, 5, self._buildActions(log_source))

        self.log_source_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.log_source_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)

    def _buildActions(self, log_source: LogSourceRecord) -> QWidget:
        container = QWidget(self.log_source_table)
        layout = QVBoxLayout(container)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(4)

        # 按钮行
        button_layout = QHBoxLayout()
        button_layout.setSpacing(6)

        extract_button = PrimaryPushButton(FluentIcon.PENCIL_INK, "提取模板", container)
        extract_button.setFixedHeight(28)

        # 检查是否正在提取
        if log_source.id in self.extracting_widgets:
            extract_button.setEnabled(False)
            extract_button.setText("正在提取")
        elif log_source.is_extracted:
            extract_button.setEnabled(False)
            extract_button.setText("已提取")
        else:
            # 只对本地文件启用提取功能
            if log_source.source_type == "本地文件":
                extract_button.clicked.connect(lambda _, log_source_id=log_source.id: self._onExtractLog(log_source_id))
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
        delete_button.clicked.connect(
            lambda _, log_source_id=log_source.id, log_source_uri=log_source.source_uri: self._onDeleteLog(
                log_source_id, log_source_uri))

        button_layout.addWidget(extract_button)
        button_layout.addWidget(view_log_button)
        button_layout.addWidget(view_template_button)
        button_layout.addWidget(delete_button)
        button_layout.addStretch(1)

        layout.addLayout(button_layout)

        # 如果正在提取，添加进度条
        if log_source.id in self.extracting_widgets:
            progress_bar = self.extracting_widgets[log_source.id].get("progress")
            if progress_bar is None:
                progress_bar = IndeterminateProgressBar(container)
                progress_bar.setFixedHeight(4)
                self.extracting_widgets[log_source.id]["progress"] = progress_bar
            layout.addWidget(progress_bar)
            # 更新存储的按钮引用
            self.extracting_widgets[log_source.id]["button"] = extract_button

        return container

    def _onExtractLog(self, log_source_id: int):
        dialog = ExtractLogMessageBox(self)
        if dialog.exec():
            # 创建工作线程
            worker = ExtractWorker(log_source_id, dialog.selected_algorithm, self)
            worker.finished.connect(self._onExtractionFinished)

            # 存储相关信息
            self.extracting_widgets[log_source_id] = {
                "button": None,
                "progress": None,
                "worker": worker
            }

            # 刷新表格以显示进度条
            self._loadLogSources()

            # 启动线程
            worker.start()

            InfoBar.info(
                title="开始提取",
                content=f"正在使用 {dialog.selected_algorithm} 算法提取模板...",
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=3000,
                parent=self,
            )

    def _onExtractionFinished(self, log_source_id: int, success: bool, message: str):
        # 清理存储的信息
        if log_source_id in self.extracting_widgets:
            del self.extracting_widgets[log_source_id]

        # 刷新表格
        self._loadLogSources()

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

    def _onAddLog(self):
        dialog = AddLogMessageBox(self)
        if dialog.exec():
            if dialog.selected_file_path:
                try:
                    self.log_source_service.add_local_log(dialog.selected_file_path)
                    self._loadLogSources()
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
                        duration=5000,
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
                return
            if dialog.url_input.text():
                try:
                    self.log_source_service.add_network_log(dialog.url_input.text().strip())
                    self._loadLogSources()
                    InfoBar.success(
                        title="添加成功",
                        content="网络日志源已添加",
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=3000,
                        parent=self,
                    )
                except sqlite3.IntegrityError:
                    InfoBar.warning(
                        title="添加失败",
                        content="该网络日志源已存在",
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=5000,
                        parent=self,
                    )
                except Exception as exc:
                    InfoBar.error(
                        title="添加失败",
                        content=str(exc),
                        orient=Qt.Orientation.Horizontal,
                        isClosable=True,
                        position=InfoBarPosition.TOP,
                        duration=5000,
                        parent=self,
                    )
                return
            InfoBar.warning(
                title="未选择文件",
                content="请选择UDP/TCP日志源或本地日志文件",
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=3000,
                parent=self,
            )

    def _onDeleteLog(self, log_source_id: int, log_source_uri: str):
        confirm = MessageBox("确认删除", f'确定删除该日志源吗？\n{log_source_uri}', self)
        if confirm.exec():
            try:
                self.log_source_service.delete_log(log_source_id)
                self._loadLogSources()
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

    def _onRefresh(self):
        self._loadLogSources()

    def _showToast(self, text: str):
        InfoBar.info(
            title=text,
            content="功能待实现",
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=1800,
            parent=self,
        )