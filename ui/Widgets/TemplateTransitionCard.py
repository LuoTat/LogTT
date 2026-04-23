import numpy as np
import pyqtgraph as pg
from modules.log_analysis import LogAnalysis
from PySide6.QtWidgets import QVBoxLayout
from qfluentwidgets import (
    BodyLabel,
    CardWidget,
)


class TemplateTransitionCard(CardWidget):
    """模板转移卡片"""

    def __init__(
        self,
        structured_table_name: str | None = None,
        template_table_name: str | None = None,
        parent=None,
    ):
        super().__init__(parent)

        self._main_layout = QVBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        self._title_label = BodyLabel(self.tr("模板转移"))
        self._main_layout.addWidget(self._title_label)

        self._plot_widget = pg.PlotWidget(self, "transparent")
        self._plot_widget.setStyleSheet("background: transparent;")
        self._plot_widget.setTitle("Template i -> Template j")
        self._plot_widget.setLabel("left", "From template")
        self._plot_widget.setLabel("bottom", "To template")
        self._plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self._plot_widget.setAspectLocked(True)
        self._main_layout.addWidget(self._plot_widget)

        self._color_bar = None

        if structured_table_name is not None and template_table_name is not None:
            self.setTable(structured_table_name, template_table_name)

    # ==================== 公共方法 ====================

    def setTable(
        self,
        structured_table_name: str,
        template_table_name: str,
    ):
        """设置表名并绘制模板转移图"""
        matrix = LogAnalysis.get_template_transition_matrix(
            structured_table_name,
            template_table_name,
        )

        self._plot_widget.clear()
        if self._color_bar is not None:
            self._plot_widget.plotItem.layout.removeItem(self._color_bar)
            self._color_bar.scene().removeItem(self._color_bar)

        # 计算 99% 分位数
        nonzero = matrix[matrix > 0]
        vmax = np.percentile(nonzero, 99)
        # 自动计算 rounding
        order = 10 ** np.floor(np.log10(vmax))
        rounding = order / 10
        # log 变换
        log_matrix = np.log1p(matrix)
        log_vmax = np.log1p(vmax)
        log_max = np.log1p(np.max(matrix))

        img = pg.ImageItem(log_matrix, axisOrder="row-major")
        self._plot_widget.addItem(img)
        self._color_bar = self._plot_widget.addColorBar(
            img,
            values=(0, log_vmax),
            colorMap="CET-L8",
            limits=(0, log_max),
            rounding=rounding,
        )

        self._plot_widget.enableAutoRange()

    def clear(self):
        """清空图表"""
        self._plot_widget.clear()
        if self._color_bar is not None:
            self._plot_widget.plotItem.layout.removeItem(self._color_bar)
            self._color_bar.scene().removeItem(self._color_bar)
            self._color_bar = None
