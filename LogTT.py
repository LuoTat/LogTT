import sys

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication
from qfluentwidgets import (
    Theme,
    setTheme
)

from ui import APPMainWindow

if __name__ == "__main__":
    QApplication.setHighDpiScaleFactorRoundingPolicy(Qt.HighDpiScaleFactorRoundingPolicy.PassThrough)
    setTheme(Theme.DARK)

    app = QApplication(sys.argv)
    w = APPMainWindow()
    w.show()
    app.exec()