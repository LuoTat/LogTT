import pyqtgraph as pg
from hdbscan import HDBSCAN
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

        # 计算原始 embedding
        original_embedding = self._model.encode(templates)

        # 计算可视化 embedding
        visual_embedding = UMAP(
            metric="cosine",
        ).fit_transform(original_embedding)

        # 计算聚类 embedding
        clusterable_embedding = UMAP(
            min_dist=0.0,
            n_neighbors=30,
            n_components=15,
            metric="cosine",
        ).fit_transform(original_embedding)

        # 使用 HDBSCAN 进行聚类
        labels = HDBSCAN().fit_predict(clusterable_embedding)

        # 根据聚类标签生成颜色，异常值使用灰色
        n_clusters = len(set(labels) - {-1})  # 不计算 -1 标签的数量
        cmap = pg.colormap.get("CET-L8")
        brushes = []
        for label in labels:
            if label == -1:
                brushes.append(pg.mkBrush(128, 128, 128, 255))
            else:
                c = cmap.map(label / (n_clusters - 1))
                brushes.append(pg.mkBrush(c))

        self._plot_widget.clear()
        plot = pg.ScatterPlotItem(
            pos=visual_embedding,
            brush=brushes,
            pen=pg.mkPen(None),
            size=8,
        )
        self._plot_widget.addItem(plot)
        self._plot_widget.enableAutoRange()

    def clear(self):
        """清空图表"""
        self._plot_widget.clear()
