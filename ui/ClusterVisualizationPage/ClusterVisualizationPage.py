from PySide6.QtWidgets import QWidget


class ClusterVisualizationPage(QWidget):
    """聚类可视化界面"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("ClusterVisualizationPage")
