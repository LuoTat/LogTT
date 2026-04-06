from qfluentwidgets import DoubleSpinBox, FluentIcon, GroupHeaderCardWidget, SpinBox

from modules.logparser import ParamDescriptor
from modules.logparser.parser_factory import LogParserProtocol


class ParserParamCard(GroupHeaderCardWidget):
    """单个解析算法的参数卡片"""

    def __init__(  # type: ignore[reportIncompatibleVariableOverride]
        self,
        parser_cls: type[LogParserProtocol],
        parent=None,
    ):
        super().__init__(parent)
        self._parser_cls = parser_cls
        self._widget_pairs: list[tuple[ParamDescriptor, SpinBox | DoubleSpinBox]] = []

        self.setTitle(parser_cls.name())
        self.setBorderRadius(8)

        for desc in parser_cls.get_param_descriptors():
            widget = desc.get_widget()
            widget.setFixedWidth(160)
            self.addGroup(
                FluentIcon.SETTING,
                desc.name,
                desc.description,
                widget,
            )
            self._widget_pairs.append((desc, widget))

    @property
    def parser_cls(self) -> type[LogParserProtocol]:
        return self._parser_cls

    @property
    def widget_pairs(self) -> list[tuple[ParamDescriptor, SpinBox | DoubleSpinBox]]:
        return self._widget_pairs

    def populate(self, params: dict[str, int | float]):
        """从参数字典填充控件值"""
        for desc, widget in self._widget_pairs:
            if desc.arg_name in params:
                if isinstance(widget, SpinBox):
                    widget.setValue(int(params[desc.arg_name]))
                else:
                    widget.setValue(params[desc.arg_name])

    def get_params(self) -> dict[str, int | float]:
        """获取与默认值不同的参数"""
        params: dict[str, int | float] = {}
        for desc, widget in self._widget_pairs:
            val = widget.value()
            if val != desc.default:
                params[desc.arg_name] = val
        return params
