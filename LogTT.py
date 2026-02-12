import sys

from PySide6.QtCore import Qt, QTranslator
from PySide6.QtWidgets import QApplication, QStyleFactory

from modules.app_config import appcfg
from ui import APPMainWindow

if __name__ == "__main__":
    QApplication.setHighDpiScaleFactorRoundingPolicy(
        Qt.HighDpiScaleFactorRoundingPolicy.PassThrough
    )
    QApplication.setAttribute(Qt.ApplicationAttribute.AA_DontCreateNativeWidgetSiblings)

    app = QApplication(sys.argv)
    app.setStyle(QStyleFactory.create("Fusion"))

    locale = appcfg.get(appcfg.language).value
    translator = QTranslator()
    a = translator.load(f"resource/i18n/{locale.name()}.qm")
    app.installTranslator(translator)

    w = APPMainWindow()
    w.show()
    app.exec()
