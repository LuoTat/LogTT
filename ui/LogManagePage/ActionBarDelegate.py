from PyQt6.QtCore import (
    Qt,
    QRect,
    QSize,
    QEvent,
)
from PyQt6.QtGui import QPixmap
from PyQt6.QtWidgets import QStyledItemDelegate
from qfluentwidgets import (
    PushButton,
    PrimaryPushButton
)

from .LogSourceTableModel import LogSourceTableModel


class ActionBarDelegate(QStyledItemDelegate):
    """操作按钮列委托 - 使用 paint 绘制按钮"""

    BUTTON_WIDTH = 70
    BUTTON_HEIGHT = 28
    BUTTON_SPACING = 4

    BUTTONS = [
        {"key": "extract", "text": "提取模板", "primary": True},
        {"key": "view_log", "text": "查看日志", "primary": False},
        {"key": "view_template", "text": "查看模板", "primary": False},
        {"key": "delete", "text": "删除", "primary": True},
    ]

    def __init__(self, model: LogSourceTableModel, parent=None):
        super().__init__(parent)
        self._model = model
        # 创建隐藏的按钮模板
        self._primary_btn = PrimaryPushButton()
        self._primary_btn.hide()
        self._normal_btn = PushButton()
        self._normal_btn.hide()

    def paint(self, painter, option, index):
        painter.save()

        # 计算所有按钮位置
        btn_rects = self._getButtonRects(option.rect)

        for i, btn_config in enumerate(self.BUTTONS):
            btn_rect = btn_rects[i]
            btn_text = btn_config["text"]
            is_primary = btn_config["primary"]

            # 选择按钮模板
            btn_template = self._primary_btn if is_primary else self._normal_btn
            btn_template.setText(btn_text)
            btn_template.setFixedSize(btn_rect.width(), btn_rect.height())

            # 渲染到 Pixmap
            pixmap = QPixmap(btn_rect.size())
            pixmap.fill(Qt.GlobalColor.transparent)
            btn_template.render(pixmap)

            # 绘制到 painter
            painter.drawPixmap(btn_rect.topLeft(), pixmap)

        painter.restore()

    def _getButtonRects(self, cell_rect):
        """计算所有按钮位置"""
        total_width = len(self.BUTTONS) * self.BUTTON_WIDTH + (len(self.BUTTONS) - 1) * self.BUTTON_SPACING
        start_x = cell_rect.center().x() - total_width // 2
        y = cell_rect.center().y() - self.BUTTON_HEIGHT // 2

        rects = []
        for i in range(len(self.BUTTONS)):
            x = start_x + i * (self.BUTTON_WIDTH + self.BUTTON_SPACING)
            rects.append(QRect(x, y, self.BUTTON_WIDTH, self.BUTTON_HEIGHT))
        return rects

    def _getButtonAtPos(self, cell_rect, pos):
        """获取点击位置对应的按钮 key"""
        btn_rects = self._getButtonRects(cell_rect)
        for i, rect in enumerate(btn_rects):
            if rect.contains(pos):
                return self.BUTTONS[i]["key"]
        return None

    def editorEvent(self, event, model, option, index):
        if event.type() == QEvent.Type.MouseButtonRelease:
            btn_key = self._getButtonAtPos(option.rect, event.pos())
            if btn_key:
                log_id = index.data(LogSourceTableModel.LogIdRole)
                if log_id is not None:
                    if btn_key == "extract":
                        self._model.requestExtract(log_id)
                    elif btn_key == "view_log":
                        self._model.requestViewLog(log_id)
                    elif btn_key == "view_template":
                        self._model.requestViewTemplate(log_id)
                    elif btn_key == "delete":
                        self._model.requestDelete(log_id)
                return True
        return False

    def sizeHint(self, option, index):
        total_width = len(self.BUTTONS) * self.BUTTON_WIDTH + (len(self.BUTTONS) - 1) * self.BUTTON_SPACING + 36
        return QSize(total_width, self.BUTTON_HEIGHT + 12)