from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import QHBoxLayout, QLabel, QVBoxLayout
from qfluentwidgets import (
    BodyLabel,
    CardWidget,
    FluentIcon,
    InfoBar,
    InfoBarPosition,
    MessageBoxBase,
    ModelComboBox,
    SubtitleLabel,
)

from modules.logparser import LogParserConfig, LogParserProtocol
from modules.models import LogParserConfigListModel, LogParserListModel


class ExtractLogMessageBox(MessageBoxBase):
    """选择提取算法对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)

        self._log_parser_list_model = LogParserListModel(self)
        self._log_parser_config_list_model = LogParserConfigListModel(self)

        self._init_log_parser_card()
        self._init_format_type_card()
        self.widget.setMinimumWidth(700)

    # ==================== 重写方法 ====================

    def validate(self) -> bool:
        """验证用户是否选择了提取算法和日志格式"""
        if self._log_parser_combo_box.currentIndex() < 0:
            InfoBar.warning(
                title=self.tr("未选择提取算法"),
                content=self.tr("请选择一个提取算法"),
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=4000,
                parent=self,
            )
            return False

        if self._format_type_combo_box.currentIndex() < 0:
            InfoBar.warning(
                title=self.tr("未选择日志格式"),
                content=self.tr("请选择一个日志格式"),
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=4000,
                parent=self,
            )
            return False

        return True

    # ==================== 私有方法 ====================

    def _init_log_parser_card(self):
        card = CardWidget(self)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(18, 16, 18, 16)
        card_layout.setSpacing(10)

        title_layout = QHBoxLayout()
        icon_label = QLabel(self)
        icon_label.setPixmap(FluentIcon.DEVELOPER_TOOLS.icon().pixmap(20, 20))
        title_label = SubtitleLabel(self.tr("选择提取算法"), card)
        title_label.setStyleSheet("font-size: 16px; font-weight: 600;")

        title_layout.addWidget(icon_label)
        title_layout.addSpacing(6)
        title_layout.addWidget(title_label)
        title_layout.addStretch(1)

        card_layout.addLayout(title_layout)

        log_parser_label = BodyLabel(self.tr("提取算法："), card)
        log_parser_label.setStyleSheet("font-weight: 500;")
        card_layout.addWidget(log_parser_label)

        self._log_parser_combo_box = ModelComboBox(card)
        self._log_parser_combo_box.setModel(self._log_parser_list_model)
        self._log_parser_combo_box.setPlaceholderText(self.tr("请选择提取算法"))
        self._log_parser_combo_box.currentIndexChanged.connect(
            self._on_log_parser_selected
        )
        card_layout.addWidget(self._log_parser_combo_box)

        self._hint_label = BodyLabel(card)
        self._hint_label.setStyleSheet("color: #888; font-size: 12px;")
        card_layout.addWidget(self._hint_label)

        self.viewLayout.addWidget(card)

    def _init_format_type_card(self):
        card = CardWidget(self)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(18, 16, 18, 16)
        card_layout.setSpacing(10)

        title_layout = QHBoxLayout()
        icon_label = QLabel(self)
        icon_label.setPixmap(FluentIcon.DOCUMENT.icon().pixmap(20, 20))
        title_label = SubtitleLabel(self.tr("日志格式配置"), card)
        title_label.setStyleSheet("font-size: 16px; font-weight: 600;")

        title_layout.addWidget(icon_label)
        title_layout.addSpacing(6)
        title_layout.addWidget(title_label)
        title_layout.addStretch(1)
        card_layout.addLayout(title_layout)

        format_type_label = BodyLabel(self.tr("选择格式："), card)
        format_type_label.setStyleSheet("font-weight: 500;")
        card_layout.addWidget(format_type_label)

        self._format_type_combo_box = ModelComboBox(card)
        self._format_type_combo_box.setModel(self._log_parser_config_list_model)
        self._format_type_combo_box.setPlaceholderText(self.tr("请选择日志格式类型"))
        card_layout.addWidget(self._format_type_combo_box)

        self.viewLayout.addWidget(card)

    # ==================== 槽函数 ====================

    @Slot(int)
    def _on_log_parser_selected(self, index: int):
        model_index = self._log_parser_list_model.index(index)
        parser_discription = model_index.data(
            LogParserListModel.LOG_PARSER_DISCRIPTION_ROLE
        )

        self._hint_label.setText(parser_discription)

    # ==================== 公共方法 ====================

    @property
    def log_parser_type(self) -> type[LogParserProtocol]:
        """获取用户选择的提取算法类型"""
        model_index = self._log_parser_list_model.index(
            self._log_parser_combo_box.currentIndex()
        )
        return model_index.data(LogParserListModel.LOG_PARSER_TYPE_ROLE)

    @property
    def log_parser_config(self) -> LogParserConfig:
        """获取用户选择的格式类型名称"""
        model_index = self._log_parser_config_list_model.index(
            self._format_type_combo_box.currentIndex()
        )
        return model_index.data(LogParserConfigListModel.LOG_PARSER_CONFIG_ROLE)
