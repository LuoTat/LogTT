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
        compact_matrix, row_count, col_count = self._compact_transition_matrix(matrix)

        self._plot_widget.setTitle(
            f"Template i -> Template j | raw={matrix.shape[0]}x{matrix.shape[1]}, "
            f"compact={compact_matrix.shape[0]}x{compact_matrix.shape[1]}, "
            f"nonzero rows/cols={row_count}/{col_count}"
        )

        self._plot_widget.clear()
        img = pg.ImageItem(compact_matrix, axisOrder="row-major")
        self._plot_widget.addItem(img)
        self._plot_widget.addColorBar(img, colorMap="CET-L8")
        self._plot_widget.enableAutoRange()

    def _compact_transition_matrix(
        self, matrix: np.ndarray
    ) -> tuple[np.ndarray, int, int]:
        """删除所有全零行列，只保留出现过转移的模板。"""
        nonzero_rows = np.any(matrix != 0, axis=1)
        nonzero_cols = np.any(matrix != 0, axis=0)
        row_count = int(np.count_nonzero(nonzero_rows))
        col_count = int(np.count_nonzero(nonzero_cols))

        if row_count == 0 or col_count == 0:
            return matrix[:1, :1], row_count, col_count

        return matrix[np.ix_(nonzero_rows, nonzero_cols)], row_count, col_count

    def clear(self):
        """清空图表"""
        self._plot_widget.clear()
