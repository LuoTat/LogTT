from PySide6.QtWidgets import QHBoxLayout, QVBoxLayout
from qfluentwidgets import (
    CardWidget,
    FluentIcon,
    IconWidget,
    SubtitleLabel,
)


class TitleCard(CardWidget):
    """标题卡片，包含一个图标和一个标题文本"""

    def __init__(self, icon: FluentIcon, title: str, parent=None):
        super().__init__(parent)
        self.viewLayout = QVBoxLayout(self)
        self.viewLayout.setContentsMargins(24, 24, 24, 24)
        self.viewLayout.setSpacing(16)

        title_layout = QHBoxLayout()

        icon_widget = IconWidget(icon, self)
        icon_widget.setFixedSize(20, 20)
        title_layout.addWidget(icon_widget)

        title_layout.addSpacing(6)

        title_label = SubtitleLabel(title, icon_widget)
        title_label.setStyleSheet("font-size: 16px; font-weight: 600;")
        title_layout.addWidget(title_label)

        self.viewLayout.addLayout(title_layout)
