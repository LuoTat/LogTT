import sys

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QStyleFactory
from qfluentwidgets import Theme, setTheme

from ui import APPMainWindow

if __name__ == "__main__":
    QApplication.setHighDpiScaleFactorRoundingPolicy(Qt.HighDpiScaleFactorRoundingPolicy.PassThrough)
    QApplication.setAttribute(Qt.ApplicationAttribute.AA_DontCreateNativeWidgetSiblings)

    setTheme(Theme.DARK)
    app = QApplication(sys.argv)
    app.setStyle(QStyleFactory.create("Fusion"))
    w = APPMainWindow()
    w.show()
    app.exec()
