import sys

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication
from qfluentwidgets import setTheme, Theme

from ui.APPMainWindow import APPMainWindow

if __name__ == "__main__":
    QApplication.setHighDpiScaleFactorRoundingPolicy(Qt.HighDpiScaleFactorRoundingPolicy.PassThrough)
    setTheme(Theme.DARK)
    app = QApplication(sys.argv)
    # app.setAttribute(Qt.ApplicationAttribute.AA_DontCreateNativeWidgetSiblings)
    APPMainWindow().show()
    app.exec()