import json
import re

import formatparse
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QHBoxLayout, QVBoxLayout, QWidget
from qfluentwidgets import (
    BodyLabel,
    FluentIcon,
    InfoBar,
    InfoBarPosition,
    LineEdit,
    MessageBoxBase,
    PlainTextEdit,
    SmoothScrollArea,
    SwitchButton,
)

from modules.logparser import (
    LogParserConfig,
    ParserFactory,
)
from ui.Widgets import ParserParamCard, TitleCard


class LogParserConfigEditDialog(MessageBoxBase):
    """日志格式配置编辑对话框"""

    def __init__(
        self,
        parent=None,
        config: LogParserConfig | None = None,
    ):
        super().__init__(parent)
        self._editing_config = config

        self._scroll_area = SmoothScrollArea(self)
        self._scroll_area.setWidgetResizable(True)

        self._scroll_widget = QWidget(self._scroll_area)
        self._main_layout = QVBoxLayout(self._scroll_widget)
        # 将背景设为透明以同一卡片背景
        self._scroll_widget.setStyleSheet("background: transparent;")
        self._scroll_area.setWidget(self._scroll_widget)

        self.viewLayout.addWidget(self._scroll_area)

        self._init_basic_card()
        self._init_masking_card()
        self._init_ex_args_card()
        self.widget.setMinimumWidth(700)

        if config is not None:
            self._populate_from_config(config)

    # ==================== 重写方法 ====================

    def validate(self) -> bool:
        name = self._name_edit.text().strip()
        if not name:
            self._show_warning(self.tr("名称不能为空"), self.tr("请输入配置名称"))
            return False

        log_format = self._log_format_edit.text().strip()
        if not log_format:
            self._show_warning(self.tr("日志格式不能为空"), self.tr("请输入日志格式"))
            return False

        # log_format 里面有没有 Content 字段
        if "Content" not in formatparse.compile(log_format).named_fields:
            self._show_warning(
                self.tr("日志格式错误"),
                self.tr("日志格式必须包含 {Content} 字段"),
            )
            return False

        # 验证 masking JSON
        masking_text = self._masking_edit.toPlainText().strip()
        if masking_text:
            try:
                masking_list = json.loads(masking_text)
                if not isinstance(masking_list, list):
                    raise ValueError
                for item in masking_list:
                    if (
                        not isinstance(item, list)
                        or len(item) != 2
                        or not isinstance(item[0], str)
                        or not isinstance(item[1], str)
                    ):
                        raise ValueError
                    item[0].encode("ascii")
                    item[1].encode("ascii")
                    re.compile(item[0])
            except json.JSONDecodeError:
                self._show_warning(
                    self.tr("掩码格式错误"),
                    self.tr("请输入有效的 JSON 数组"),
                )
                return False
            except UnicodeEncodeError:
                self._show_warning(
                    self.tr("掩码格式错误"),
                    self.tr("掩码规则只能包含 ASCII 字符"),
                )
                return False
            except re.error:
                self._show_warning(
                    self.tr("掩码正则无效"),
                    self.tr("掩码中包含无效的正则表达式"),
                )
                return False
            except ValueError:
                self._show_warning(
                    self.tr("掩码格式错误"),
                    self.tr('格式须为 [["正则", "替换"], ...] 的二维数组'),
                )
                return False

        # 验证 delimiters
        delimiters_text = self._delimiters_edit.text().strip()
        if delimiters_text:
            try:
                delimiters_text.encode("ascii")
            except UnicodeEncodeError:
                self._show_warning(
                    self.tr("分隔符无效"),
                    self.tr("分隔符只能包含 ASCII 字符"),
                )
                return False

        return True

    # ==================== 私有方法 ====================

    def _show_warning(self, title: str, content: str):
        InfoBar.warning(
            title=title,
            content=content,
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=4000,
            parent=self,
        )

    def _init_basic_card(self):
        card = TitleCard(FluentIcon.EDIT, self.tr("基本信息"), self)

        name_label = BodyLabel(self.tr("配置名称："), card)
        name_label.setStyleSheet("font-weight: 500;")
        card.viewLayout.addWidget(name_label)

        self._name_edit = LineEdit(card)
        self._name_edit.setPlaceholderText(self.tr("例如：MyApp"))
        self._name_edit.setClearButtonEnabled(True)
        card.viewLayout.addWidget(self._name_edit)

        log_format_label = BodyLabel(self.tr("日志格式："), card)
        log_format_label.setStyleSheet("font-weight: 500;")
        card.viewLayout.addWidget(log_format_label)

        self._log_format_edit = LineEdit(card)
        self._log_format_edit.setPlaceholderText(
            self.tr("例如：{Date} {Time} {Level}: {Content}")
        )
        self._log_format_edit.setClearButtonEnabled(True)
        card.viewLayout.addWidget(self._log_format_edit)

        log_format_hint = BodyLabel(
            self.tr("使用 {字段名} 标记日志中的各个部分，必须包含 {Content}"),
            card,
        )
        log_format_hint.setStyleSheet("color: #888; font-size: 12px;")
        card.viewLayout.addWidget(log_format_hint)

        delimiters_label = BodyLabel(self.tr("分隔符："), card)
        delimiters_label.setStyleSheet("font-weight: 500;")
        card.viewLayout.addWidget(delimiters_label)

        self._delimiters_edit = LineEdit(card)
        self._delimiters_edit.setPlaceholderText(self.tr("例如：=:()"))
        self._delimiters_edit.setClearButtonEnabled(True)
        card.viewLayout.addWidget(self._delimiters_edit)

        delimiters_hint = BodyLabel(
            self.tr("用于分词的额外分隔字符，直接拼接输入，可留空。仅支持 ASCII 字符"),
            card,
        )
        delimiters_hint.setStyleSheet("color: #888; font-size: 12px;")
        card.viewLayout.addWidget(delimiters_hint)

        # 使用内置掩码
        masking_toggle_layout = QHBoxLayout()

        masking_toggle_label = BodyLabel(self.tr("使用内置掩码规则："), card)
        masking_toggle_label.setStyleSheet("font-weight: 500;")
        masking_toggle_layout.addWidget(masking_toggle_label)

        masking_toggle_layout.addStretch()

        self._use_builtin_masking_switch = SwitchButton(card)
        self._use_builtin_masking_switch.setChecked(True)
        masking_toggle_layout.addWidget(self._use_builtin_masking_switch)

        card.viewLayout.addLayout(masking_toggle_layout)

        self._main_layout.addWidget(card)

    def _init_masking_card(self):
        card = TitleCard(FluentIcon.FILTER, self.tr("自定义掩码规则"), self)

        masking_label = BodyLabel(self.tr("掩码规则："), card)
        masking_label.setStyleSheet("font-weight: 500;")
        card.viewLayout.addWidget(masking_label)

        self._masking_edit = PlainTextEdit(card)
        self._masking_edit.setPlaceholderText(
            self.tr("""\
例如：
[
    [
        "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(:\\d{0,})?",
        "<#IP#>"
    ]
]""")
        )
        self._masking_edit.setMinimumHeight(160)
        card.viewLayout.addWidget(self._masking_edit)

        masking_hint = BodyLabel(self.tr("每项为 [正则, 替换] 的数组"), card)
        masking_hint.setStyleSheet("color: #888; font-size: 12px;")
        card.viewLayout.addWidget(masking_hint)

        self._main_layout.addWidget(card)

    def _init_ex_args_card(self):
        """初始化各解析器高级参数区域"""
        self._parser_param_cards: list[ParserParamCard] = []

        for parser_cls in ParserFactory.get_all_parsers_type():
            if not parser_cls.get_param_descriptors():
                continue

            card = ParserParamCard(parser_cls, self)
            self._parser_param_cards.append(card)
            self._main_layout.addWidget(card)

    def _populate_from_config(self, config: LogParserConfig):
        """从已有配置填充表单"""
        self._name_edit.setText(config.name)
        self._log_format_edit.setText(config.log_format)
        self._delimiters_edit.setText(config.delimiters)
        self._use_builtin_masking_switch.setChecked(config.use_builtin_maskings)
        self._masking_edit.setPlainText(json.dumps(config.user_maskings, indent=4))

        # 填充 ex_args
        if config.ex_args:
            for card in self._parser_param_cards:
                if (card.parser_cls.name()) in config.ex_args:
                    card.populate(config.ex_args[card.parser_cls.name()])

    # ==================== 公共方法 ====================

    def get_config(self) -> LogParserConfig:
        """从表单构建 LogParserConfig 对象"""
        name = self._name_edit.text().strip()
        log_format = self._log_format_edit.text().strip()

        # masking
        masking = None
        masking_text = self._masking_edit.toPlainText().strip()
        if masking_text:
            raw = json.loads(masking_text)
            masking = [tuple(item) for item in raw]

        # delimiters
        delimiters = ""
        delimiters_text = self._delimiters_edit.text().strip()
        if delimiters_text:
            delimiters = delimiters_text

        use_builtin_masking = self._use_builtin_masking_switch.isChecked()

        # ex_args
        ex_args: dict[str, dict[str, int | float]] = {}
        for card in self._parser_param_cards:
            params = card.get_params()
            if params:
                ex_args[card.parser_cls.name()] = params

        return LogParserConfig(
            name,
            log_format,
            masking,
            delimiters,
            use_builtin_masking,
            ex_args,
        )
