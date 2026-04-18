from PySide6.QtWidgets import QWidget


class TemplateAnalysisPage(QWidget):
    """模板分析界面"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("TemplateAnalysisPage")
