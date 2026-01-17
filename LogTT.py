import sys
import traceback

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication
from qfluentwidgets import (
    Theme,
    setTheme
)

from ui import APPMainWindow


def handle_exception(exc_type, exc_value, exc_traceback):
    """Print uncaught exceptions with full traceback."""
    traceback.print_exception(exc_type, exc_value, exc_traceback)


sys.excepthook = handle_exception

if __name__ == "__main__":
    QApplication.setHighDpiScaleFactorRoundingPolicy(Qt.HighDpiScaleFactorRoundingPolicy.PassThrough)
    setTheme(Theme.DARK)

    app = QApplication(sys.argv)
    w = APPMainWindow()
    w.show()
    app.exec()

# class A:
#     def __init__(self, a):
#         self.func = lambda: print(a)
#
#
# array = []
#
# for (i) in range(10):
#     b = A(i)
#     b.func()
#     array.append(b)
#
# for item in array:
#     item.func()