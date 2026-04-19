import pyqtgraph as pg
from modules.log_analysis import LogAnalysis
from PySide6.QtWidgets import QVBoxLayout
from qfluentwidgets import (
    BodyLabel,
    CardWidget,
)

# 时间粒度预设：(显示名, months, days, micros)
GRANULARITIES = [
    ("1 秒", 0, 0, 1_000_000),
    ("10 秒", 0, 0, 10_000_000),
    ("1 分钟", 0, 0, 60_000_000),
    ("5 分钟", 0, 0, 300_000_000),
    ("10 分钟", 0, 0, 600_000_000),
    ("30 分钟", 0, 0, 1_800_000_000),
    ("1 小时", 0, 0, 3_600_000_000),
    ("6 小时", 0, 0, 21_600_000_000),
    ("1 天", 0, 1, 0),
    ("1 周", 0, 7, 0),
    ("1 月", 1, 0, 0),
]


class LogFrequencyCard(CardWidget):
    """日志频率时间线折线图卡片"""

    def __init__(self, structured_table_name: str | None = None, parent=None):
        super().__init__(parent)

        self._main_layout = QVBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        self._title_label = BodyLabel(self.tr("日志频率时间线"), self)
        self._main_layout.addWidget(self._title_label)

        self._plot_widget = pg.PlotWidget(
            self,
            "transparent",
            axisItems={"bottom": pg.DateAxisItem()},
        )
        self._plot_widget.setStyleSheet("background: transparent;")
        self._plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self._main_layout.addWidget(self._plot_widget)

        if structured_table_name is not None:
            self.setTable(structured_table_name)

    # ==================== 公共方法 ====================

    def setTable(self, structured_table_name: str, granularity_index: int = 2):
        """设置表名并绘制日志频率时间线"""

        _, months, days, micros = GRANULARITIES[granularity_index]
        distribution = LogAnalysis.get_log_frequency_distribution(
            structured_table_name,
            months,
            days,
            micros,
        )

        epochs = [item[0] for item in distribution]
        counts = [item[1] for item in distribution]

        self._plot_widget.plot(
            epochs,
            counts,
            symbol="o",
            symbolBrush=pg.mkBrush("#4FC2F7"),
            symbolSize=1,
            pen=pg.mkPen("#4FC2F7"),
            fillLevel=0,
            fillBrush=pg.mkBrush("#4FC3F732"),
            useCache=False,
            skipFiniteCheck=True,
            clear=True,
        )
        self._plot_widget.setDownsampling(auto=True, mode="peak")
        self._plot_widget.setClipToView(True)
        self._plot_widget.enableAutoRange(y=0.8)

    def clear(self):
        """清空图表"""
        self._plot_widget.clear()
