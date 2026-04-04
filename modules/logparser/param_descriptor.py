from dataclasses import dataclass
from enum import Enum

from qfluentwidgets import DoubleSpinBox, SpinBox


class ParamWidgetType(Enum):
    SpinBox = 0
    DoubleSpinBox = 1


@dataclass(frozen=True, slots=True)
class ParamDescriptor:
    arg_name: str
    name: str
    description: str
    widget_type: ParamWidgetType
    default: int | float
    minimum: int | float
    maximum: int | float

    def get_widget(self) -> SpinBox | DoubleSpinBox:
        if self.widget_type == ParamWidgetType.SpinBox:
            widget = SpinBox()
            widget.setRange(int(self.minimum), int(self.maximum))
            widget.setValue(int(self.default))
        else:
            widget = DoubleSpinBox()
            widget.setSingleStep(0.01)
            widget.setRange(self.minimum, self.maximum)
            widget.setValue(self.default)

        return widget
