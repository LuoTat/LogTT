import pyqtgraph as pg
from modules.log_analysis import LogAnalysis
from PySide6.QtWidgets import QVBoxLayout
from qfluentwidgets import (
    BodyLabel,
    CardWidget,
)


class LogFrequencyCard(CardWidget):
    """日志频数直方图卡片"""

    def __init__(self, structured_table_name: str | None = None, parent=None):
        super().__init__(parent)

        self._main_layout = QVBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        self._title_label = BodyLabel(self.tr("日志频数"), self)
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

    def setTable(
        self,
        structured_table_name: str,
        interval: tuple[int, int, int] = (0, 0, 60_000_000),
    ):
        """设置表名并绘制日志频数直方图"""
        months, days, micros = interval
        epochs, counts = LogAnalysis.get_log_frequency_distribution(
            structured_table_name,
            months,
            days,
            micros,
        )

        # 从 interval 计算柱宽（秒）
        bar_width = months * 30 * 86400 + days * 86400 + micros / 1_000_000
        self._plot_widget.clear()
        bar = pg.BarGraphItem(
            x=epochs,
            height=counts,
            width=bar_width,
            pen=pg.mkPen("#4FC2F7"),
            brush=pg.mkBrush("#4FC2F788"),
        )
        self._plot_widget.addItem(bar)
        self._plot_widget.enableAutoRange()

    def clear(self):
        """清空图表"""
        self._plot_widget.clear()
