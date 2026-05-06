from pathlib import Path

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import QFileDialog, QHBoxLayout
from qfluentwidgets import (
    BodyLabel,
    FluentIcon,
    InfoBar,
    InfoBarPosition,
    MessageBoxBase,
    PushButton,
)

from ui.Widgets import TitleCard


class AddLogMessageBox(MessageBoxBase):
    """新增日志对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._selected_file_paths: list[str] = []

        # self._init_network_card()
        self._init_local_file_card()
        self.widget.setMinimumWidth(700)

    # ==================== 重写方法 ====================

    def validate(self) -> bool:
        """验证用户是否选择了日志文件"""
        if not self._selected_file_paths:
            InfoBar.warning(
                title=self.tr("未选择文件"),
                content=self.tr("请选择本地日志文件"),
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=4000,
                parent=self,
            )
            return False

        return True

    # ==================== 私有方法 ====================

    def _init_local_file_card(self):
        card = TitleCard(FluentIcon.FOLDER, self.tr("本地文件源"), self)

        file_label = BodyLabel(self.tr("选择日志文件："), card)
        file_label.setStyleSheet("font-weight: 500;")
        card.viewLayout.addWidget(file_label)

        select_file_layout = QHBoxLayout()

        self._select_file_button = PushButton(
            FluentIcon.FOLDER_ADD,
            self.tr("选择文件"),
            card,
        )
        self._select_file_button.clicked.connect(self._on_select_file)
        select_file_layout.addWidget(self._select_file_button)

        self._file_path_label = BodyLabel(self.tr("未选择文件"), card)
        self._file_path_label.setStyleSheet("color: #888; padding-left: 8px;")
        select_file_layout.addWidget(self._file_path_label)

        card.viewLayout.addLayout(select_file_layout)

        hint_label = BodyLabel(self.tr("支持 .log, .txt 等文本格式"), card)
        hint_label.setStyleSheet("color: #888; font-size: 12px;")
        card.viewLayout.addWidget(hint_label)

        self.viewLayout.addWidget(card)

    # ==================== 槽函数 ====================

    @Slot()
    def _on_select_file(self):
        self._selected_file_paths, _ = QFileDialog.getOpenFileNames(
            self,
            self.tr("选择日志文件"),
            "",
            self.tr("日志文件 (*.log *.txt);;所有文件 (*.*)"),
        )

        if self._selected_file_paths:
            self._file_path_label.setText(
                ", ".join(Path(p).name for p in self._selected_file_paths)
            )
            self._file_path_label.setStyleSheet(
                "color: #0078d4; padding-left: 8px; font-weight: 500;"
            )

    # ==================== 公共方法 ====================

    @property
    def file_paths(self) -> list[str]:
        """获取用户输入的日志文件路径"""
        return self._selected_file_paths
