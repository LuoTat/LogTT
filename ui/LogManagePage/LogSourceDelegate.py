from PyQt6.QtCore import Qt, QRect, QSize, QModelIndex, QEvent, QPoint
from PyQt6.QtGui import QPainter, QMouseEvent
from PyQt6.QtWidgets import (
    QStyle,
    QHBoxLayout,
    QStyleOptionButton,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QApplication,
    QWidget
)

from qfluentwidgets import (
    ProgressBar,
    FluentIcon,
    PrimaryPushButton,
    PushButton
)

from .LogSourceTableModel import LogSourceTableModel


class ProgressBarDelegate(QStyledItemDelegate):
    """进度条列委托 - 用于显示日志提取进度"""

    def createEditor(self, parent, option, index):
        # 返回 qfluentwidgets 的 ProgressBar
        bar = ProgressBar(parent)
        bar.setRange(0, 100)
        return bar

    def setEditorData(self, editor: ProgressBar, index):
        progress = index.data(LogSourceTableModel.ProgressRole)
        is_extracting = index.data(LogSourceTableModel.IsExtractingRole)
        if is_extracting and progress is not None:
            editor.setValue(progress)
            editor.show()
        else:
            editor.hide()
    #
    # def updateEditorGeometry(self, editor, option, index):
    #     editor.setGeometry(option.rect.adjusted(8, 8, -8, -8))


class ActionButtonsWidget(QWidget):
    """操作按钮组件 - 包含提取、查看日志、查看模板、删除按钮"""

    def __init__(self, parent=None):
        super().__init__(parent)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(6)

        # 创建按钮
        self.extract_btn = PrimaryPushButton(FluentIcon.PENCIL_INK, "提取模板", self)
        self.view_log_btn = PushButton(FluentIcon.DOCUMENT, "查看日志", self)
        self.view_template_btn = PushButton(FluentIcon.VIEW, "查看模板", self)
        self.delete_btn = PrimaryPushButton(FluentIcon.DELETE, "删除", self)

        # 设置按钮大小
        for btn in [self.extract_btn, self.view_log_btn, self.view_template_btn, self.delete_btn]:
            btn.setFixedHeight(26)

        layout.addWidget(self.extract_btn)
        layout.addWidget(self.view_log_btn)
        layout.addWidget(self.view_template_btn)
        layout.addWidget(self.delete_btn)

        # 存储当前绑定的 log_source_id
        self._log_source_id: int | None = None
        self._model: LogSourceTableModel | None = None

    def bindModel(self, model: LogSourceTableModel):
        """绑定模型，用于触发操作信号"""
        self._model = model
        self.extract_btn.clicked.connect(self._onExtractClicked)
        self.view_log_btn.clicked.connect(self._onViewLogClicked)
        self.view_template_btn.clicked.connect(self._onViewTemplateClicked)
        self.delete_btn.clicked.connect(self._onDeleteClicked)

    def updateState(self, log_source_id: int, is_extracted: bool, is_extracting: bool):
        """更新按钮状态"""
        self._log_source_id = log_source_id

        # 提取按钮：未提取且未在提取中才可用
        self.extract_btn.setEnabled(not is_extracted and not is_extracting)
        # 查看日志/模板：已提取才可用
        self.view_log_btn.setEnabled(is_extracted)
        self.view_template_btn.setEnabled(is_extracted)
        # 删除按钮始终可用
        self.delete_btn.setEnabled(True)

    def _onExtractClicked(self):
        if self._model and self._log_source_id is not None:
            self._model.requestExtract(self._log_source_id)

    def _onViewLogClicked(self):
        if self._model and self._log_source_id is not None:
            self._model.requestViewLog(self._log_source_id)

    def _onViewTemplateClicked(self):
        if self._model and self._log_source_id is not None:
            self._model.requestViewTemplate(self._log_source_id)

    def _onDeleteClicked(self):
        if self._model and self._log_source_id is not None:
            self._model.requestDelete(self._log_source_id)


class ActionButtonDelegate(QStyledItemDelegate):
    """操作按钮列委托 - 使用持久编辑器显示真实按钮组件"""

    def __init__(self, model: LogSourceTableModel, parent=None):
        super().__init__(parent)
        self._model = model

    def createEditor(self, parent, option, index) -> ActionButtonsWidget:
        """创建按钮组件作为编辑器"""
        widget = ActionButtonsWidget(parent)
        widget.bindModel(self._model)
        return widget

    def setEditorData(self, editor: ActionButtonsWidget, index: QModelIndex):
        """根据模型数据更新按钮状态"""
        log_source_id = index.data(LogSourceTableModel.LogIdRole)
        is_extracted = index.data(LogSourceTableModel.IsExtractedRole) or False
        is_extracting = index.data(LogSourceTableModel.IsExtractingRole) or False

        if log_source_id is not None:
            editor.updateState(log_source_id, is_extracted, is_extracting)

    def updateEditorGeometry(self, editor, option, index):
        """设置编辑器位置"""
        editor.setGeometry(option.rect)

    def sizeHint(self, option: QStyleOptionViewItem, index: QModelIndex) -> QSize:
        return QSize(340, 40)