from PyQt6.QtCore import (
    Qt,
    QRect,
    QSize,
    QEvent,
)
from PyQt6.QtGui import QPixmap
from PyQt6.QtWidgets import QStyledItemDelegate
from qfluentwidgets import ProgressBar

from .LogSourceTableModel import LogSourceTableModel


class ProgressBarDelegate(QStyledItemDelegate):
    """进度条列委托 - 用于显示日志提取进度，使用 qfluentwidgets 的 ProgressBar"""

    # 类变量控制进度条尺寸
    PROGRESS_WIDTH = 100
    PROGRESS_HEIGHT = 4
    SPACING = 4  # 进度条与文字间隔
    MARGIN_H = 16  # 水平边距
    MARGIN_V = 8  # 垂直边距

    def __init__(self, parent=None):
        super().__init__(parent)
        # 创建一个隐藏的 ProgressBar 作为渲染模板
        self.progress_bar = ProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setFixedSize(self.PROGRESS_WIDTH, self.PROGRESS_HEIGHT)
        self.progress_bar.hide()

    def paint(self, painter, option, index):
        progress = index.data(LogSourceTableModel.ProgressRole)

        if progress is not None:
            painter.save()

            # 使用固定宽度文字 "100%" 计算布局，避免跳动
            font_metrics = painter.fontMetrics()
            fixed_text_width = font_metrics.horizontalAdvance("100%")
            # 计算总内容宽度
            total_width = self.PROGRESS_WIDTH + self.SPACING + fixed_text_width

            # 计算表格项中心位置
            center_x = option.rect.center().x()
            center_y = option.rect.center().y()

            # 1. 绘制进度条
            bar_x = center_x - total_width // 2
            bar_y = center_y - self.PROGRESS_HEIGHT // 2
            self.progress_bar.setValue(progress)
            pixmap = QPixmap(self.PROGRESS_WIDTH, self.PROGRESS_HEIGHT)
            self.progress_bar.render(pixmap)
            painter.drawPixmap(bar_x, bar_y, pixmap)

            # 2. 绘制百分比文字（右对齐到固定宽度区域）
            text = f"{progress}%"
            text_rect = QRect(
                bar_x + self.PROGRESS_WIDTH + self.SPACING,
                option.rect.top(),
                fixed_text_width,
                option.rect.height()
            )
            painter.setPen(option.palette.text().color())
            painter.drawText(text_rect, Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight, text)

            painter.restore()

    def editorEvent(self, event, model, option, index):
        if event.type() == QEvent.Type.MouseButtonPress:
            return True
        return super().editorEvent(event, model, option, index)

    def sizeHint(self, option, index):
        # 估算文字宽度（"100%"）
        text_width = 35
        total_width = self.PROGRESS_WIDTH + self.SPACING + text_width + 2 * self.MARGIN_H
        total_height = max(self.PROGRESS_HEIGHT, 16) + 2 * self.MARGIN_V
        return QSize(total_width, total_height)