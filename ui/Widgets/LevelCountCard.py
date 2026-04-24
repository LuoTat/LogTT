import pyqtgraph as pg
from modules.log_analysis import LogAnalysis
from PySide6.QtWidgets import QVBoxLayout
from qfluentwidgets import (
    BodyLabel,
    CardWidget,
)

from modules.constants import LEVEL_COLOR_MAP


class LevelCountCard(CardWidget):
    """日志级别分布柱状图卡片"""

    def __init__(self, parent=None):
        super().__init__(parent)

        self._main_layout = QVBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        self._title_label = BodyLabel(self.tr("日志级别分布"), self)
        self._main_layout.addWidget(self._title_label)

        self._plot_widget = pg.PlotWidget(self, "transparent")
        self._plot_widget.setStyleSheet("background: transparent;")
        self._plot_widget.setMouseEnabled(x=False, y=False)
        self._main_layout.addWidget(self._plot_widget)

    # ==================== 公共方法 ====================

    def setTable(self, structured_table_name: str):
        """设置表名并绘制日志级别分布柱状图"""
        distribution = LogAnalysis.get_level_distribution(structured_table_name)

        levels = distribution[0]
        counts = distribution[1]

        self._plot_widget.clear()

        x = range(len(levels))
        bar = pg.BarGraphItem(
            x=x,
            height=counts,
            width=0.6,
            brushes=[
                pg.mkBrush(LEVEL_COLOR_MAP.get(level.upper(), "#78909C"))
                for level in levels
            ],
        )
        self._plot_widget.addItem(bar)

        # 设置 x 轴刻度为日志级别名称
        ax = self._plot_widget.getAxis("bottom")
        ax.setTicks([list(zip(x, levels))])

    def clear(self):
        """清空图表，恢复空框架"""
        self._plot_widget.clear()
