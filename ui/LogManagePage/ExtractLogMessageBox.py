import regex

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QLabel,
    QHBoxLayout,
    QVBoxLayout
)
from qfluentwidgets import (
    InfoBar,
    ComboBox,
    LineEdit,
    BodyLabel,
    CardWidget,
    FluentIcon,
    PlainTextEdit,
    SubtitleLabel,
    MessageBoxBase,
    InfoBarPosition
)

from modules.logparser import ParserFactory
from modules.format_config import FormatConfigManager


class ExtractLogMessageBox(MessageBoxBase):
    """选择提取算法对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.format_config_manager = FormatConfigManager()
        self.selected_algorithm: str | None = None
        self.selected_format_type: str | None = None
        self.selected_log_format: str | None = None
        self.selected_regex: list[str] | None = None
        self.is_custom_mode = False

        self.viewLayout.addWidget(self._createAlgorithmCard())
        self.viewLayout.addWidget(self._createFormatCard())
        self.widget.setMinimumWidth(600)

    def _createAlgorithmCard(self) -> CardWidget:
        card = CardWidget(self)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(18, 16, 18, 16)
        card_layout.setSpacing(10)

        title_layout = QHBoxLayout()
        icon_label = QLabel(self)
        icon_label.setPixmap(FluentIcon.DEVELOPER_TOOLS.icon().pixmap(20, 20))
        title_label = SubtitleLabel("选择提取算法", card)
        title_label.setStyleSheet("font-size: 16px; font-weight: 600;")

        title_layout.addWidget(icon_label)
        title_layout.addSpacing(6)
        title_layout.addWidget(title_label)
        title_layout.addStretch(1)

        card_layout.addLayout(title_layout)

        algorithm_label = BodyLabel("提取算法：", card)
        algorithm_label.setStyleSheet("font-weight: 500;")
        card_layout.addWidget(algorithm_label)

        self.algorithm_combo_box = ComboBox(card)
        self.algorithm_combo_box.setFixedHeight(36)

        # 获取所有的解析器列表
        all_parsers = ParserFactory.get_all_parsers_name()
        self.algorithm_combo_box.addItems(all_parsers)
        if all_parsers:
            self.algorithm_combo_box.setCurrentIndex(0)
            self.selected_algorithm = all_parsers[0]
        self.algorithm_combo_box.currentTextChanged.connect(self._onAlgorithmChanged)
        card_layout.addWidget(self.algorithm_combo_box)

        hint_label = BodyLabel(ParserFactory.get_parser_description(self.selected_algorithm), card)
        hint_label.setStyleSheet("color: #888; font-size: 12px;")
        card_layout.addWidget(hint_label)

        return card

    def _createFormatCard(self) -> CardWidget:
        card = CardWidget(self)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(18, 16, 18, 16)
        card_layout.setSpacing(10)

        # 标题
        title_layout = QHBoxLayout()
        icon_label = QLabel(self)
        icon_label.setPixmap(FluentIcon.DOCUMENT.icon().pixmap(20, 20))
        title_label = SubtitleLabel("日志格式配置", card)
        title_label.setStyleSheet("font-size: 16px; font-weight: 600;")

        title_layout.addWidget(icon_label)
        title_layout.addSpacing(6)
        title_layout.addWidget(title_label)
        title_layout.addStretch(1)

        card_layout.addLayout(title_layout)

        # 格式选择下拉框
        format_label = BodyLabel("选择格式：", card)
        format_label.setStyleSheet("font-weight: 500;")
        card_layout.addWidget(format_label)

        self.format_combo_box = ComboBox(card)
        self.format_combo_box.setFixedHeight(36)
        format_names = ["自定义"] + self.format_config_manager.get_all_format_types()
        self.format_combo_box.addItems(format_names)
        # 默认选择第二个（第一个是"自定义"）
        default_index = 1 if len(format_names) > 1 else 0
        self.format_combo_box.setCurrentIndex(default_index)
        self.selected_format_type = format_names[default_index]
        self.is_custom_mode = (default_index == 0)
        self.format_combo_box.currentTextChanged.connect(self._onFormatChanged)
        card_layout.addWidget(self.format_combo_box)

        # 格式名称输入框
        name_label = BodyLabel("格式名称：", card)
        name_label.setStyleSheet("font-weight: 500;")
        card_layout.addWidget(name_label)

        self.format_name_input = LineEdit(card)
        self.format_name_input.setFixedHeight(36)
        self.format_name_input.setPlaceholderText("输入自定义格式名称")
        card_layout.addWidget(self.format_name_input)

        # log_format输入框
        log_format_label = BodyLabel("日志格式：", card)
        log_format_label.setStyleSheet("font-weight: 500;")
        card_layout.addWidget(log_format_label)

        self.log_format_input = LineEdit(card)
        self.log_format_input.setFixedHeight(36)
        self.log_format_input.setPlaceholderText("例如：<Date> <Time> <Level>:<Content>")
        card_layout.addWidget(self.log_format_input)

        log_format_hint = BodyLabel("格式中至少包含 <Date> <Time> <Level> <Content>", card)
        log_format_hint.setStyleSheet("color: #888; font-size: 12px;")
        card_layout.addWidget(log_format_hint)

        # regex输入框（多行）
        regex_label = BodyLabel("正则表达式（每行一个）：", card)
        regex_label.setStyleSheet("font-weight: 500;")
        card_layout.addWidget(regex_label)

        self.regex_input = PlainTextEdit(card)
        self.regex_input.setFixedHeight(80)
        self.regex_input.setPlaceholderText("每行输入一个正则表达式，用于预处理日志\n例如：(\\d+\\.){3}\\d+(:\\d+)?:? # IP")
        card_layout.addWidget(self.regex_input)

        regex_hint = BodyLabel("正则表达式用于预处理，可为空", card)
        regex_hint.setStyleSheet("color: #888; font-size: 12px;")
        card_layout.addWidget(regex_hint)

        # 加载对应的格式配置
        self._loadFormatConfig()

        return card

    def _onAlgorithmChanged(self, text: str):
        self.selected_algorithm = text

    def _onFormatChanged(self, text: str):
        self.selected_format_type = text
        self.is_custom_mode = (text == "自定义")
        self._loadFormatConfig()

    def _loadFormatConfig(self):
        if self.is_custom_mode:
            # 自定义模式：清空并启用编辑
            self.format_name_input.setText("")
            self.log_format_input.setText("")
            self.regex_input.setPlainText("")
            self.format_name_input.setReadOnly(False)
            self.log_format_input.setReadOnly(False)
            self.regex_input.setReadOnly(False)
        else:
            # 预设模式：填充内容并禁用编辑
            config = self.format_config_manager.get_format_config(self.selected_format_type)
            if config:
                self.format_name_input.setText(self.selected_format_type)
                self.log_format_input.setText(config.log_format)
                self.regex_input.setPlainText("\n".join(config.regex))
            self.format_name_input.setReadOnly(True)
            self.log_format_input.setReadOnly(True)
            self.regex_input.setReadOnly(True)

    def validate(self) -> bool:
        if not self.is_custom_mode:
            format_config = self.format_config_manager.get_format_config(self.selected_format_type)
            self.selected_log_format = format_config.log_format
            self.selected_regex = format_config.regex
            return True

        # 验证格式名称
        format_name = self.format_name_input.text().strip()
        if not format_name:
            InfoBar.warning(
                title="创建失败",
                content="请输入格式名称",
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=4000,
                parent=self
            )
            return False
        if format_name == "自定义":
            InfoBar.warning(
                title="创建失败",
                content="格式名称不能为 '自定义'",
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=4000,
                parent=self
            )
            return False

        if self.format_config_manager.is_format_type_exists(format_name):
            InfoBar.warning(
                title="创建失败",
                content=f"格式名称 '{format_name}' 已存在，请使用其他名称",
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=4000,
                parent=self
            )
            return False

        # 验证log_format
        log_format = self.log_format_input.text().strip()
        required_fields = ["<Date>", "<Time>", "<Level>", "<Content>"]
        # 找出缺失的占位符
        missing_fields = [field for field in required_fields if field not in log_format]

        if missing_fields:
            InfoBar.warning(
                title="创建失败",
                content=f"日志格式缺少以下占位符: {', '.join(missing_fields)}",
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=4000,
                parent=self
            )
            return False

        # 验证正则表达式
        regex_text = self.regex_input.toPlainText().strip()
        regex_list = []
        if regex_text:
            for line in regex_text.split("\n"):
                line = line.strip()
                if line:
                    try:
                        regex.compile(line)
                        regex_list.append(line)
                    except regex.error as e:
                        InfoBar.warning(
                            title="创建失败",
                            content=f"无效的正则表达式: {line}\n错误: {str(e)}",
                            orient=Qt.Orientation.Horizontal,
                            isClosable=True,
                            position=InfoBarPosition.TOP,
                            duration=4000,
                            parent=self
                        )
                        return False

        # 保存自定义配置
        self.selected_format_type = format_name
        self.selected_log_format = log_format
        self.selected_regex = regex_list

        return True