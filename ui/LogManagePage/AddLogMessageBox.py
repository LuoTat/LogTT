from pathlib import Path

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import QFileDialog, QHBoxLayout, QLabel, QVBoxLayout
from qfluentwidgets import (
    BodyLabel,
    CardWidget,
    FluentIcon,
    InfoBar,
    InfoBarPosition,
    LineEdit,
    MessageBoxBase,
    PushButton,
    SubtitleLabel,
    ToolTipFilter,
    ToolTipPosition,
)


class AddLogMessageBox(MessageBoxBase):
    """新增日志对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._selected_file_path: str = str()

        self._initNetworkCard()
        self._initLocalFileCard()
        self.widget.setMinimumWidth(700)

    # ==================== 重写方法 ====================

    def validate(self) -> bool:
        """验证用是否输入合法"""
        if not self._selected_file_path and not self._url_edit.text().strip():
            InfoBar.warning(
                title="未选择文件",
                content="请选择UDP/TCP日志源或本地日志文件",
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=4000,
                parent=self,
            )
            return False

        return True

    # ==================== 私有方法 ====================

    def _initNetworkCard(self):
        card = CardWidget(self)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(18, 16, 18, 16)
        card_layout.setSpacing(10)

        title_layout = QHBoxLayout()
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

        self._url_edit = LineEdit(card)
        self._url_edit.setPlaceholderText("例如：syslog://192.168.1.100:514")
        self._url_edit.setClearButtonEnabled(True)
        self._url_edit.setFixedHeight(36)
        card_layout.addWidget(self._url_edit)

        hint_label = BodyLabel("支持 UDP/TCP，默认端口 514", card)
        hint_label.setStyleSheet("color: #888; font-size: 12px;")
        card_layout.addWidget(hint_label)

        self.viewLayout.addWidget(card)

    def _initLocalFileCard(self):
        card = CardWidget(self)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(18, 16, 18, 16)
        card_layout.setSpacing(10)

        title_layout = QHBoxLayout()
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
        self._select_file_button = PushButton(FluentIcon.FOLDER_ADD, "选择文件", card)
        self._select_file_button.setFixedHeight(36)
        self._select_file_button.clicked.connect(self._onSelectFile)

        self._file_path_label = BodyLabel("未选择文件", card)
        self._file_path_label.setStyleSheet("color: #888; padding-left: 8px;")

        select_file_layout.addWidget(self._select_file_button)
        select_file_layout.addWidget(self._file_path_label)
        select_file_layout.addStretch(1)

        card_layout.addLayout(select_file_layout)

        hint_label = BodyLabel("支持 .log, .txt 等文本格式", card)
        hint_label.setStyleSheet("color: #888; font-size: 12px;")
        card_layout.addWidget(hint_label)

        self.viewLayout.addWidget(card)

    # ==================== 槽函数 ====================

    @Slot()
    def _onSelectFile(self):
        self._selected_file_path, _ = QFileDialog.getOpenFileName(
            self,
            "选择日志文件",
            "",
            "日志文件 (*.log *.txt);;所有文件 (*.*)",
        )

        if self._selected_file_path:
            self._file_path_label.setText(Path(self._selected_file_path).name)
            self._file_path_label.setStyleSheet("color: #0078d4; padding-left: 8px; font-weight: 500;")
            self._file_path_label.setToolTip(self._selected_file_path)
            self._file_path_label.installEventFilter(
                ToolTipFilter(self._file_path_label, showDelay=300, position=ToolTipPosition.RIGHT)
            )

    # ==================== 公共方法 ====================

    @property
    def log_uri(self) -> str:
        """获取用户输入的日志源 URI"""
        return self._selected_file_path or self._url_edit.text().strip()

    @property
    def is_local_file(self) -> bool:
        """判断日志源是否为本地文件"""
        return bool(self._selected_file_path)
