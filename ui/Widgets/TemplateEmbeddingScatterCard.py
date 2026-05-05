import pyqtgraph as pg
from modules.duckdb_service import DuckDBService
from PySide6.QtWidgets import QVBoxLayout
from qfluentwidgets import (
    BodyLabel,
    CardWidget,
)
from sentence_transformers import SentenceTransformer
from umap import UMAP

from modules.constants import ONNX_PATH


class TemplateEmbeddingScatterCard(CardWidget):
    """模板嵌入散点图卡片"""

    def __init__(self, parent=None):
        super().__init__(parent)

        self._main_layout = QVBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        self._title_label = BodyLabel(self.tr("模板聚类"), self)
        self._main_layout.addWidget(self._title_label)

        self._plot_widget = pg.PlotWidget(self, "transparent")
        self._plot_widget.setMinimumHeight(1000)
        self._plot_widget.setStyleSheet("background: transparent;")
        self._plot_widget.setLabel("left", "Embedding dim-2")
        self._plot_widget.setLabel("bottom", "Embedding dim-1")
        self._plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self._plot_widget.setAspectLocked(True)
        self._main_layout.addWidget(self._plot_widget)

        self._model = None

    # ==================== 公共方法 ====================

    def setTable(self, template_table_name: str):
        """设置模板表名并绘制模板嵌入散点图"""
        if self._model is None:
            self._model = SentenceTransformer(
                "all-mpnet-base-v2",
                backend="onnx",
                cache_folder=ONNX_PATH.as_posix(),
                model_kwargs={"file_name": "onnx/model_qint8_avx512_vnni.onnx"},
            )

        result, _ = DuckDBService.fetch_csv_table(template_table_name, 0, -1)

        templates = [v[0] for v in result]
        counts = [int(v[1]) for v in result]

        # 计算 embedding
        embeddings = self._model.encode(
            templates,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )

        # UMAP降维到2D
        mapper = UMAP(metric="cosine").fit_transform(embeddings)

        self._plot_widget.clear()
        plot = pg.ScatterPlotItem(
            pos=mapper,
        )
        self._plot_widget.addItem(plot)
        self._plot_widget.enableAutoRange()

    def clear(self):
        """清空图表"""
        self._plot_widget.clear()
