import os
import sqlite3

from PyQt6.QtCore import Qt
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
)

from modules.log_source_service import LogSourceService


class AddLogMessageBox(MessageBoxBase):
    """新增日志对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.selectedFilePath: str | None = None

        self.viewLayout.addWidget(self._createNetworkCard())
        self.viewLayout.addWidget(self._createLocalFileCard())
        self.widget.setMinimumWidth(700)

    def _createNetworkCard(self) -> CardWidget:
        card = CardWidget(self)
        cardLayout = QVBoxLayout(card)
        cardLayout.setContentsMargins(18, 16, 18, 16)
        cardLayout.setSpacing(10)

        titleLayout = QHBoxLayout(card)
        iconLabel = QLabel(self)
        iconLabel.setPixmap(FluentIcon.GLOBE.icon().pixmap(20, 20))
        titleLabel = SubtitleLabel("网络日志源", card)
        titleLabel.setStyleSheet("font-size: 16px; font-weight: 600;")

        titleLayout.addWidget(iconLabel)
        titleLayout.addSpacing(6)
        titleLayout.addWidget(titleLabel)
        titleLayout.addStretch(1)

        cardLayout.addLayout(titleLayout)

        urlLabel = BodyLabel("Syslog 服务器地址：", card)
        urlLabel.setStyleSheet("font-weight: 500;")
        cardLayout.addWidget(urlLabel)

        self.urlInput = LineEdit(card)
        self.urlInput.setPlaceholderText("例如：syslog://192.168.1.100:514")
        self.urlInput.setClearButtonEnabled(True)
        self.urlInput.setFixedHeight(36)
        cardLayout.addWidget(self.urlInput)

        hintLabel = BodyLabel("支持 UDP/TCP，默认端口 514", card)
        hintLabel.setStyleSheet("color: #888; font-size: 12px;")
        cardLayout.addWidget(hintLabel)

        return card

    def _createLocalFileCard(self) -> CardWidget:
        card = CardWidget(self)
        cardLayout = QVBoxLayout(card)
        cardLayout.setContentsMargins(18, 16, 18, 16)
        cardLayout.setSpacing(10)

        titleLayout = QHBoxLayout(card)
        iconLabel = QLabel(self)
        iconLabel.setPixmap(FluentIcon.FOLDER.icon().pixmap(20, 20))
        titleLabel = SubtitleLabel("本地文件源", card)
        titleLabel.setStyleSheet("font-size: 16px; font-weight: 600;")

        titleLayout.addWidget(iconLabel)
        titleLayout.addSpacing(6)
        titleLayout.addWidget(titleLabel)
        titleLayout.addStretch(1)

        cardLayout.addLayout(titleLayout)

        fileLabel = BodyLabel("选择日志文件：", card)
        fileLabel.setStyleSheet("font-weight: 500;")
        cardLayout.addWidget(fileLabel)

        fileSelectLayout = QHBoxLayout()
        self.selectFileButton = PushButton(FluentIcon.FOLDER_ADD, "选择文件", card)
        self.selectFileButton.setFixedHeight(36)
        self.selectFileButton.clicked.connect(self._onSelectFile)

        self.filePathLabel = BodyLabel("未选择文件", card)
        self.filePathLabel.setStyleSheet("color: #888; padding-left: 8px;")

        fileSelectLayout.addWidget(self.selectFileButton)
        fileSelectLayout.addWidget(self.filePathLabel)
        fileSelectLayout.addStretch(1)

        cardLayout.addLayout(fileSelectLayout)

        hintLabel = BodyLabel("支持 .log, .txt 等文本格式", card)
        hintLabel.setStyleSheet("color: #888; font-size: 12px;")
        cardLayout.addWidget(hintLabel)

        return card

    def _onSelectFile(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "选择日志文件",
            "",
            "日志文件 (*.log *.txt);;所有文件 (*.*)",
        )

        if file_path:
            self.selectedFilePath = file_path
            self.filePathLabel.setText(os.path.basename(file_path))
            self.filePathLabel.setStyleSheet("color: #0078d4; padding-left: 8px; font-weight: 500;")
            self.filePathLabel.setToolTip(file_path)
            self.filePathLabel.installEventFilter(
                ToolTipFilter(self.filePathLabel, showDelay=300, position=ToolTipPosition.RIGHT))


class LogManagePage(QWidget):
    """日志管理页面"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("LogManagePage")

        self.logService = LogSourceService()
        self.mainLayout = QVBoxLayout(self)
        self.mainLayout.setContentsMargins(24, 24, 24, 24)
        self.mainLayout.setSpacing(16)

        self._initToolbar()
        self._initLogTable()
        self._loadLogSources()

    def _initToolbar(self):
        bar = QHBoxLayout(self)
        bar.setSpacing(10)

        self.searchInput = LineEdit(self)
        self.searchInput.setPlaceholderText("按名称或路径搜索")
        self.searchInput.setClearButtonEnabled(True)
        self.searchInput.setFixedHeight(36)

        self.refreshButton = PushButton(FluentIcon.SYNC, "刷新", self)
        self.refreshButton.setFixedHeight(36)
        self.refreshButton.clicked.connect(self._onRefresh)

        self.addButton = PrimaryPushButton(FluentIcon.ADD, "新增日志", self)
        self.addButton.setFixedHeight(36)
        self.addButton.clicked.connect(self._onAddLog)

        bar.addWidget(self.searchInput, 1)
        bar.addStretch(1)
        bar.addWidget(self.refreshButton)
        bar.addWidget(self.addButton)

        self.mainLayout.addLayout(bar)

    def _initLogTable(self):
        self.logTable = TableWidget(self)
        self.logTable.setBorderVisible(True)
        self.logTable.setBorderRadius(8)
        self.logTable.setWordWrap(False)
        self.logTable.setColumnCount(6)
        self.logTable.setHorizontalHeaderLabels(["类型", "URI", "创建时间", "提取方法", "行数", "操作"])
        self.logTable.verticalHeader().hide()
        self.logTable.setEditTriggers(self.logTable.EditTrigger.NoEditTriggers)
        self.logTable.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.logTable.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)

        self.mainLayout.addWidget(self.logTable)

    def _loadLogSources(self):
        logSources = self.logService.get_all_logs()
        self.logTable.setRowCount(len(logSources))
        for index, logSource in enumerate(logSources):
            sourceTypeItem = QTableWidgetItem(logSource.source_type)
            sourceTypeItem.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            sourceURIItem = QTableWidgetItem(logSource.source_uri)
            sourceURIItem.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            createTimeItem = QTableWidgetItem(logSource.create_time.isoformat(timespec="seconds"))
            createTimeItem.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            extractMethodItem = QTableWidgetItem(
                logSource.extract_method if logSource.extract_method is not None else "—")
            extractMethodItem.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            lineCountItem = QTableWidgetItem(f"{logSource.line_count:,}" if logSource.line_count is not None else "—")
            lineCountItem.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            self.logTable.setItem(index, 0, sourceTypeItem)
            self.logTable.setItem(index, 1, sourceURIItem)
            self.logTable.setItem(index, 2, createTimeItem)
            self.logTable.setItem(index, 3, extractMethodItem)
            self.logTable.setItem(index, 4, lineCountItem)
            self.logTable.setCellWidget(index, 5, self._buildActions(logSource))

        self.logTable.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.logTable.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)

    def _buildActions(self, log_source) -> QWidget:
        container = QWidget(self.logTable)
        layout = QHBoxLayout(container)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(6)

        extractBtn = PrimaryPushButton(FluentIcon.PENCIL_INK, "提取模板", container)
        extractBtn.setFixedHeight(28)
        extractBtn.clicked.connect(lambda: self._showToast("提取模板"))
        if log_source.is_extracted:
            extractBtn.setEnabled(False)
            extractBtn.setText("已提取")

        viewLogBtn = PushButton(FluentIcon.VIEW, "查看日志", container)
        viewLogBtn.setFixedHeight(28)
        viewLogBtn.clicked.connect(lambda: self._showToast("查看日志"))
        viewLogBtn.setEnabled(log_source.is_extracted)

        viewTemplateBtn = PushButton(FluentIcon.VIEW, "查看模板", container)
        viewTemplateBtn.setFixedHeight(28)
        viewTemplateBtn.clicked.connect(lambda: self._showToast("查看模板"))
        viewTemplateBtn.setEnabled(log_source.is_extracted)

        deleteBtn = PrimaryPushButton(FluentIcon.DELETE, "删除", container)
        deleteBtn.setFixedHeight(28)
        deleteBtn.clicked.connect(lambda _, id=log_source.id, uri=log_source.source_uri: self._onDeleteLog(id, uri))

        layout.addWidget(extractBtn)
        layout.addWidget(viewLogBtn)
        layout.addWidget(viewTemplateBtn)
        layout.addWidget(deleteBtn)
        layout.addStretch(1)
        return container

    def _onAddLog(self):
        dialog = AddLogMessageBox(self)
        if dialog.exec():
            if dialog.selectedFilePath:
                try:
                    self.logService.add_local_log(dialog.selectedFilePath)
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
            if dialog.urlInput.text():
                try:
                    self.logService.add_network_log(dialog.urlInput.text().strip())
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

    def _onDeleteLog(self, log_id: int, source_uri: str):
        confirm = MessageBox("确认删除", f'确定删除该日志源吗？\n{source_uri}', self)
        if confirm.exec():
            try:
                self.logService.delete_log(log_id)
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