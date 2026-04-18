from PySide6.QtWidgets import QWidget


class TemporalAnalysisPage(QWidget):
    """时序分析界面"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("TemporalAnalysisPage")
