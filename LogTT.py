import sys
import qasync

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication
from qfluentwidgets import setTheme, Theme

from ui import APPMainWindow

if __name__ == "__main__":
    QApplication.setHighDpiScaleFactorRoundingPolicy(Qt.HighDpiScaleFactorRoundingPolicy.PassThrough)
    setTheme(Theme.DARK)
    app = QApplication(sys.argv)

    # 创建 QEventLoop
    loop = qasync.QEventLoop(app)

    APPMainWindow().show()

    with loop:
        loop.run_forever()