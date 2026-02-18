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

from modules.logparser.base_log_parser import BaseLogParser
from modules.models.format_type_list_model import FormatTypeListModel
from modules.models.logparser_list_model import LogParserListModel


class ExtractLogMessageBox(MessageBoxBase):
    """选择提取算法对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)

        self._logparser_list_model = LogParserListModel(self)
        self._format_type_list_model = FormatTypeListModel(self)

        self._init_log_parser_card()
        self._init_format_type_card()
        self.widget.setMinimumWidth(700)

    # ==================== 重写方法 ====================

    def validate(self) -> bool:
        """验证用户是否选择了提取算法和日志格式"""
        if self._logparser_combo_box.currentIndex() < 0:
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

    # ==================== 重写方法 ====================

    # def validate(self) -> bool:
    #     if not self._is_custom_mode:
    #         format_config = self._format_config_manager.get_format_config(self.selected_format_type)
    #         self.selected_log_format = format_config.log_format
    #         self.selected_regex = format_config.regex
    #         return True

    #     # 验证格式名称
    #     format_name = self.format_name_input.text().strip()
    #     if not format_name:
    #         InfoBar.warning(
    #             title="创建失败",
    #             content="请输入格式名称",
    #             orient=Qt.Orientation.Horizontal,
    #             isClosable=True,
    #             position=InfoBarPosition.TOP,
    #             duration=4000,
    #             parent=self,
    #         )
    #         return False
    #     if format_name == "自定义":
    #         InfoBar.warning(
    #             title="创建失败",
    #             content="格式名称不能为 '自定义'",
    #             orient=Qt.Orientation.Horizontal,
    #             isClosable=True,
    #             position=InfoBarPosition.TOP,
    #             duration=4000,
    #             parent=self,
    #         )
    #         return False

    #     if self._format_config_manager.is_format_type_exists(format_name):
    #         InfoBar.warning(
    #             title="创建失败",
    #             content=f"格式名称 '{format_name}' 已存在，请使用其他名称",
    #             orient=Qt.Orientation.Horizontal,
    #             isClosable=True,
    #             position=InfoBarPosition.TOP,
    #             duration=4000,
    #             parent=self,
    #         )
    #         return False

    #     # 验证正则表达式
    #     regex_text = self.regex_input.toPlainText().strip()
    #     regex_list = []
    #     if regex_text:
    #         for line in regex_text.split("\n"):
    #             line = line.strip()
    #             if line:
    #                 try:
    #                     regex.compile(line)
    #                     regex_list.append(line)
    #                 except regex.error as e:
    #                     InfoBar.warning(
    #                         title="创建失败",
    #                         content=f"无效的正则表达式: {line}\n错误: {str(e)}",
    #                         orient=Qt.Orientation.Horizontal,
    #                         isClosable=True,
    #                         position=InfoBarPosition.TOP,
    #                         duration=4000,
    #                         parent=self,
    #                     )
    #                     return False

    #     # 保存自定义配置
    #     # self.selected_format_type = format_name
    #     # self.selected_log_format = self.log_format_input.text().strip()
    #     # self.selected_regex = regex_list

    #     return True

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

        logparser_label = BodyLabel(self.tr("提取算法："), card)
        logparser_label.setStyleSheet("font-weight: 500;")
        card_layout.addWidget(logparser_label)

        self._logparser_combo_box = ModelComboBox(card)
        self._logparser_combo_box.setModel(self._logparser_list_model)
        self._logparser_combo_box.setPlaceholderText(self.tr("请选择提取算法"))
        self._logparser_combo_box.currentIndexChanged.connect(
            self._on_log_parser_selected
        )
        card_layout.addWidget(self._logparser_combo_box)

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
        self._format_type_combo_box.setModel(self._format_type_list_model)
        self._format_type_combo_box.setPlaceholderText(self.tr("请选择日志格式类型"))
        self._format_type_combo_box.currentIndexChanged.connect(
            self._on_format_type_selected
        )
        card_layout.addWidget(self._format_type_combo_box)

        # log_format展示框
        # log_format_label = BodyLabel(self.tr("日志格式："), card)
        # log_format_label.setStyleSheet("font-weight: 500;")
        # card_layout.addWidget(log_format_label)

        # self._log_format_edit = LineEdit(card)
        # self._log_format_edit.setReadOnly(True)
        # self.log_format_input.setPlaceholderText("例如：<Date> <Time> <Level>:<Content>")
        # card_layout.addWidget(self._log_format_edit)

        # log_format_hint = BodyLabel("通常情况下是每一个间隔的内容都需要一个分组", card)
        # log_format_hint.setStyleSheet("color: #888; font-size: 12px;")
        # card_layout.addWidget(log_format_hint)

        # mask展示框
        # mask_label = BodyLabel(self.tr("掩码正则表达式（每行一个）："), card)
        # mask_label.setStyleSheet("font-weight: 500;")
        # card_layout.addWidget(mask_label)

        # self._mask_edit = PlainTextEdit(card)
        # self._mask_edit.setReadOnly(True)
        # self.regex_input.setPlaceholderText(
        #     """
        #     每行输入一个正则表达式，用于预处理日志，例如：
        #     (\\d+\\.){3}\\d+(:\\d+)?:? # IP
        #     """
        # )
        # card_layout.addWidget(self._mask_edit)

        # delimiters展示框
        # delimiters_label = BodyLabel(self.tr("分隔符（每行一个）："), card)
        # delimiters_label.setStyleSheet("font-weight: 500;")
        # card_layout.addWidget(delimiters_label)

        # self._delimiters_edit = PlainTextEdit(card)
        # self._delimiters_edit.setReadOnly(True)

        # card_layout.addWidget(self._delimiters_edit)

        # regex_hint = BodyLabel("正则表达式用于预处理，可为空", card)
        # regex_hint.setStyleSheet("color: #888; font-size: 12px;")
        # card_layout.addWidget(regex_hint)

        self.viewLayout.addWidget(card)

    # ==================== 槽函数 ====================

    @Slot(int)
    def _on_log_parser_selected(self, index: int):
        model_index = self._logparser_list_model.index(index)
        parser_discription = model_index.data(
            LogParserListModel.LOG_PARSER_DISCRIPTION_ROLE
        )

        self._hint_label.setText(parser_discription)

    @Slot(int)
    def _on_format_type_selected(self, index: int):
        model_index = self._format_type_list_model.index(index)
        log_format = model_index.data(FormatTypeListModel.LOG_FORMAT_ROLE)
        mask_list = model_index.data(FormatTypeListModel.LOG_FORMAT_MASK_ROLE)
        delimiters_list = model_index.data(FormatTypeListModel.LOG_FORMAT_DELIM_ROLE)

        # self._log_format_edit.setText(log_format)
        # self._mask_edit.setPlainText(
        #     "\n".join(map(lambda x: f"{x[0]} -> {x[1]}", mask_list))
        # )
        # self._delimiters_edit.setPlainText("\n".join(delimiters_list))

    # ==================== 公共方法 ====================

    @property
    def logparser_type(self) -> type[BaseLogParser]:
        """获取用户选择的提取算法类型"""
        model_index = self._logparser_list_model.index(
            self._logparser_combo_box.currentIndex()
        )
        return model_index.data(LogParserListModel.LOG_PARSER_TYPE_ROLE)

    @property
    def format_type(self) -> str:
        """获取用户选择的格式类型名称"""
        model_index = self._format_type_list_model.index(
            self._format_type_combo_box.currentIndex()
        )
        return model_index.data()

    @property
    def log_format(self) -> str:
        """获取用户选择的日志格式"""
        model_index = self._format_type_list_model.index(
            self._format_type_combo_box.currentIndex()
        )
        return model_index.data(FormatTypeListModel.LOG_FORMAT_ROLE)

    @property
    def log_mask(self) -> list[tuple[str, str]]:
        """获取用户选择的掩码列表"""
        model_index = self._format_type_list_model.index(
            self._format_type_combo_box.currentIndex()
        )
        return model_index.data(FormatTypeListModel.LOG_FORMAT_MASK_ROLE)

    @property
    def log_delimiters(self) -> list[str]:
        """获取用户选择的分隔符列表"""
        model_index = self._format_type_list_model.index(
            self._format_type_combo_box.currentIndex()
        )
        return model_index.data(FormatTypeListModel.LOG_FORMAT_DELIM_ROLE)
