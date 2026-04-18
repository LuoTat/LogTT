import pyqtgraph as pg
from modules.log_analysis import LogAnalysis
from PySide6.QtWidgets import QVBoxLayout
from qfluentwidgets import (
    BodyLabel,
    CardWidget,
)


class LevelCountCard(CardWidget):
    """日志级别分布柱状图卡片"""

    def __init__(self, structured_table_name: str | None = None, parent=None):
        super().__init__(parent)

        self._main_layout = QVBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        self._title_label = BodyLabel(self.tr("日志级别分布"), self)
        self._main_layout.addWidget(self._title_label)

        self._plot_widget = pg.PlotWidget(self, "transparent")
        self._plot_widget.setStyleSheet("background: transparent;")
        self._plot_widget.getPlotItem().hideAxis("bottom")
        self._plot_widget.getPlotItem().hideAxis("left")
        self._plot_widget.setMouseEnabled(x=False, y=False)
        self._main_layout.addWidget(self._plot_widget)

        if structured_table_name is not None:
            self.setTable(structured_table_name)

    # ==================== 私有方法 ====================

    @staticmethod
    def _get_level_colors(levels: list[str]) -> list[str]:
        """根据日志级别名称返回对应颜色"""
        color_map = {
            "FATAL": "#DC143C",
            "EMERG": "#DC143C",
            "ALERT": "#FF4500",
            "CRIT": "#FF4500",
            "CRITICAL": "#FF4500",
            "ERROR": "#FF6347",
            "ERR": "#FF6347",
            "WARN": "#FFA500",
            "WARNING": "#FFA500",
            "NOTICE": "#4682B4",
            "INFO": "#4CAF50",
            "DEBUG": "#9E9E9E",
            "TRACE": "#BDBDBD",
        }
        return [color_map.get(level.upper(), "#78909C") for level in levels]

    # ==================== 公共方法 ====================

    def setTable(self, structured_table_name: str):
        """设置表名并绘制日志级别分布柱状图"""
        distribution = LogAnalysis.get_level_distribution(structured_table_name)

        levels = [item[0] for item in distribution]
        counts = [item[1] for item in distribution]

        self._plot_widget.clear()
        self._plot_widget.getPlotItem().showAxis("bottom")
        self._plot_widget.getPlotItem().showAxis("left")

        x = range(len(levels))
        colors = self._get_level_colors(levels)
        brushes = [pg.mkBrush(c) for c in colors]

        self._bar_item = pg.BarGraphItem(
            x=x,
            height=counts,
            width=0.6,
            brushes=brushes,
        )
        self._plot_widget.addItem(self._bar_item)

        # 设置 x 轴刻度为日志级别名称
        ax = self._plot_widget.getPlotItem().getAxis("bottom")
        ax.setTicks([list(zip(x, levels))])

    def clear(self):
        """清空图表，恢复空框架"""
        self._plot_widget.clear()
        self._bar_item = None
        self._plot_widget.getPlotItem().hideAxis("bottom")
        self._plot_widget.getPlotItem().hideAxis("left")
