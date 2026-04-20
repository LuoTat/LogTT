import pyqtgraph as pg
from modules.log_analysis import LogAnalysis
from PySide6.QtWidgets import QVBoxLayout
from qfluentwidgets import (
    BodyLabel,
    CardWidget,
)

from modules.constants import LEVEL_COLOR_MAP


class LogLevelFrequencyCard(CardWidget):
    """日志级别频率时间线折线图卡片"""

    def __init__(self, structured_table_name: str | None = None, parent=None):
        super().__init__(parent)

        self._main_layout = QVBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        self._title_label = BodyLabel(self.tr("日志级别频率时间线"), self)
        self._main_layout.addWidget(self._title_label)

        self._plot_widget = pg.PlotWidget(
            self,
            "transparent",
            axisItems={"bottom": pg.DateAxisItem()},
        )
        self._plot_widget.setStyleSheet("background: transparent;")
        self._plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self._plot_widget.addLegend()
        self._main_layout.addWidget(self._plot_widget)

        if structured_table_name is not None:
            self.setTable(structured_table_name)

    # ==================== 公共方法 ====================

    def setTable(
        self,
        structured_table_name: str,
        interval: tuple[int, int, int] = (0, 0, 60_000_000),
    ):
        """设置表名并绘制日志频率时间线"""

        months, days, micros = interval
        level_distribution = LogAnalysis.get_log_level_frequency_distribution(
            structured_table_name,
            months,
            days,
            micros,
        )

        # 从 interval 计算柱宽（秒）
        bar_width = months * 30 * 86400 + days * 86400 + micros / 1_000_000
        self._plot_widget.clear()
        for level, (epochs, counts) in level_distribution.items():
            bar = pg.BarGraphItem(
                x=epochs,
                height=counts,
                width=bar_width,
                pen=pg.mkPen(LEVEL_COLOR_MAP.get(level.upper(), "#DDDDDD")),
                brush=pg.mkBrush(LEVEL_COLOR_MAP.get(level.upper(), "#808080") + "88"),
                name=level,
            )
            self._plot_widget.addItem(bar)

        self._plot_widget.enableAutoRange()

    def clear(self):
        """清空图表"""
        self._plot_widget.clear()
