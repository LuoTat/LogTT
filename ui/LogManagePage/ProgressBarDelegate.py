from PySide6.QtCore import Qt, QSize

from PySide6.QtWidgets import (
    QStyle,
    QApplication,
    QStyledItemDelegate,
    QStyleOptionProgressBar
)

# 定义进度角色常量，与 LogModel 和 LogTableModel 保持一致
PROGRESS_ROLE = Qt.ItemDataRole.UserRole + 3


class ProgressBarDelegate(QStyledItemDelegate):
    """进度条列委托 - 用于显示日志提取进度"""

    def __init__(self, parent=None):
        super().__init__(parent)

    def paint(self, painter, option, index):
        progress = index.data(PROGRESS_ROLE)

        # 配置进度条样式选项
        progress_bar = QStyleOptionProgressBar()
        progress_bar.rect = option.rect
        progress_bar.minimum = 0
        progress_bar.maximum = 100
        progress_bar.progress = progress
        progress_bar.text = f"{progress}%"
        progress_bar.textVisible = True

        # 绘制进度条
        QApplication.style().drawControl(QStyle.ControlElement.CE_ProgressBar, progress_bar, painter)

    def sizeHint(self, option, index):
        return QSize(256, 16)