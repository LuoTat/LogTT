import sys

from PySide6.QtCore import Qt, QTranslator
from PySide6.QtWidgets import QApplication
from qfluentwidgets import FluentTranslator

from modules.app_config import appcfg
from ui import APPMainWindow

if __name__ == "__main__":
    QApplication.setHighDpiScaleFactorRoundingPolicy(
        Qt.HighDpiScaleFactorRoundingPolicy.PassThrough
    )
    QApplication.setAttribute(Qt.ApplicationAttribute.AA_DontCreateNativeWidgetSiblings)

    app = QApplication(sys.argv)

    locale = appcfg.get(appcfg.language).value
    f_translator = FluentTranslator(locale)
    translator = QTranslator()
    translator.load(f"resource/i18n/{locale.name()}.qm")
    app.installTranslator(translator)
    app.installTranslator(f_translator)

    w = APPMainWindow()
    w.show()
    app.exec()
