from dataclasses import dataclass
from enum import Enum

from qfluentwidgets import DoubleSpinBox, SpinBox


class ParamWidgetType(Enum):
    SpinBox = 0
    DoubleSpinBox = 1


@dataclass(frozen=True, slots=True)
class ParamDescriptor:
    name: str
    description: str
    widget_type: ParamWidgetType
    default: int | float
    minimum: int | float
    maximum: int | float

    def get_widget(self) -> SpinBox | DoubleSpinBox:
        if self.widget_type == ParamWidgetType.SpinBox:
            widget = SpinBox()
            widget.setValue(int(self.default))
            widget.setRange(int(self.minimum), int(self.maximum))
        else:
            widget = DoubleSpinBox()
            widget.setSingleStep(0.1)
            widget.setValue(self.default)
            widget.setRange(self.minimum, self.maximum)

        return widget
