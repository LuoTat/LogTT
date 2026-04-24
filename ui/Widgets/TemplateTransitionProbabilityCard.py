import numpy as np
import pyqtgraph as pg
from modules.log_analysis import LogAnalysis
from PySide6.QtWidgets import QVBoxLayout
from qfluentwidgets import (
    BodyLabel,
    CardWidget,
)


class TemplateTransitionProbabilityCard(CardWidget):
    """模板转移概率卡片"""

    def __init__(self, parent=None):
        super().__init__(parent)

        self._main_layout = QVBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        self._title_label = BodyLabel(self.tr("模板转移概率"))
        self._main_layout.addWidget(self._title_label)

        self._plot_widget = pg.PlotWidget(self, "transparent")
        self._plot_widget.setMinimumHeight(1000)
        self._plot_widget.setStyleSheet("background: transparent;")
        self._plot_widget.setTitle("Transition probability P(j|i)")
        self._plot_widget.setLabel("left", "From template")
        self._plot_widget.setLabel("bottom", "To template")
        self._plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self._plot_widget.setAspectLocked(True)
        self._main_layout.addWidget(self._plot_widget)

        self._color_bar = None

    # ==================== 公共方法 ====================

    def setTable(
        self,
        structured_table_name: str,
        template_table_name: str,
    ):
        """设置表名并绘制模板转移概率图"""
        matrix = LogAnalysis.get_template_transition_matrix(
            structured_table_name,
            template_table_name,
        )

        self._plot_widget.clear()
        if self._color_bar is not None:
            layout = getattr(self._plot_widget.plotItem, "layout")
            layout.removeItem(self._color_bar)
            self._color_bar.scene().removeItem(self._color_bar)

        # 计算条件概率 P(j|i)
        row_sum = matrix.sum(axis=1, keepdims=True)
        prob_matrix = np.divide(
            matrix,
            row_sum,
            out=np.zeros_like(matrix, dtype=np.float64),
            where=row_sum > 0,
        )

        img = pg.ImageItem(prob_matrix, axisOrder="row-major")
        self._plot_widget.addItem(img)
        self._color_bar = self._plot_widget.addColorBar(
            img,
            values=(0.0, 1.0),
            colorMap="CET-L8",
            limits=(0.0, 1.0),
            rounding=0.01,
        )

        self._plot_widget.enableAutoRange()

    def clear(self):
        """清空图表"""
        self._plot_widget.clear()
        if self._color_bar is not None:
            layout = getattr(self._plot_widget.plotItem, "layout")
            layout.removeItem(self._color_bar)
            self._color_bar.scene().removeItem(self._color_bar)
            self._color_bar = None
