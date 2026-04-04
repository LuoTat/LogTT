from PySide6.QtCore import QModelIndex, QPoint, Qt, Slot
from PySide6.QtWidgets import QHBoxLayout, QLabel
from qfluentwidgets import (
    Action,
    FluentIcon,
    InfoBar,
    InfoBarPosition,
    ListView,
    MessageBoxBase,
    PrimaryPushButton,
    RoundMenu,
    SubtitleLabel,
)

from modules.models.log_parser_config_list_model import LogParserConfigListModel

from .LogParserConfigEditDialog import LogParserConfigEditDialog


class LogParserConfigManageDialog(MessageBoxBase):
    """日志格式配置管理对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)

        # 初始化模型
        self._init_model()

        self._init_title()
        self._init_list_view()

        self.yesButton.hide()
        self.widget.setMinimumWidth(700)

    # ==================== 私有方法 ====================

    def _init_model(self):
        """初始化数据模型"""
        self._log_parser_config_list_model = LogParserConfigListModel(self, False)

        # 连接模型信号 -> UI 反馈
        self._log_parser_config_list_model.addSuccess.connect(self._on_add_success)
        self._log_parser_config_list_model.deleteSuccess.connect(
            self._on_delete_success
        )
        self._log_parser_config_list_model.editSuccess.connect(self._on_edit_success)

    def _init_title(self):
        """初始化工具栏"""
        title_layout = QHBoxLayout()
        title_layout.setSpacing(10)

        icon_label = QLabel(self)
        icon_label.setPixmap(FluentIcon.DOCUMENT.icon().pixmap(20, 20))
        title_label = SubtitleLabel(self.tr("自定义日志格式配置"), self)
        title_label.setStyleSheet("font-size: 16px; font-weight: 600;")
        title_layout.addWidget(icon_label)

        title_layout.addSpacing(6)
        title_layout.addWidget(title_label)

        title_layout.addStretch(1)
        self._add_button = PrimaryPushButton(FluentIcon.ADD, self.tr("新增"), self)
        self._add_button.clicked.connect(self._on_add)
        title_layout.addWidget(self._add_button)

        self.viewLayout.addLayout(title_layout)

    def _init_list_view(self):
        """初始化列表视图"""
        self._list_view = ListView(self)
        self._list_view.setModel(self._log_parser_config_list_model)
        self._list_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._list_view.customContextMenuRequested.connect(
            self._on_context_menu_requested
        )

        self.viewLayout.addWidget(self._list_view)

    # ==================== 槽函数 ====================

    @Slot(QPoint)
    def _on_context_menu_requested(self, pos: QPoint):
        """处理右键菜单请求"""
        index = self._list_view.indexAt(pos)
        if not index.isValid():
            return

        # 右键选中该行
        self._list_view.setCurrentIndex(index)

        # 创建菜单
        menu = RoundMenu(parent=self._list_view)

        edit_action = Action(FluentIcon.EDIT, self.tr("编辑"))
        edit_action.triggered.connect(lambda: self._on_edit(index))
        menu.addAction(edit_action)

        menu.addSeparator()

        delete_action = Action(FluentIcon.DELETE, self.tr("删除"))
        delete_action.triggered.connect(lambda: self._on_delete(index))
        menu.addAction(delete_action)

        menu.exec(self._list_view.viewport().mapToGlobal(pos))

    @Slot()
    def _on_add(self):
        dialog = LogParserConfigEditDialog(self)
        if dialog.exec():
            self._log_parser_config_list_model.request_add(dialog.get_config())

    @Slot(QModelIndex)
    def _on_edit(self, index: QModelIndex):
        old_config = index.data(LogParserConfigListModel.LOG_PARSER_CONFIG_ROLE)
        dialog = LogParserConfigEditDialog(self, old_config)
        if dialog.exec():
            self._log_parser_config_list_model.request_edit(index, dialog.get_config())

    @Slot(QModelIndex)
    def _on_delete(self, index: QModelIndex):
        self._log_parser_config_list_model.request_delete(index)

    @Slot()
    def _on_add_success(self):
        InfoBar.success(
            title=self.tr("添加成功"),
            content=self.tr("已添加新配置"),
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )

    @Slot()
    def _on_delete_success(self):
        InfoBar.success(
            title=self.tr("删除成功"),
            content=self.tr("已删除配置"),
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )

    @Slot()
    def _on_edit_success(self):
        InfoBar.success(
            title=self.tr("编辑成功"),
            content=self.tr("已更新配置"),
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )
