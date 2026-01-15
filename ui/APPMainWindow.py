from PyQt6.QtCore import QUrl
from PyQt6.QtWidgets import QApplication
from PyQt6.QtGui import (
    QIcon,
    QDesktopServices
)
from qfluentwidgets import (
    FluentIcon,
    MessageBox,
    FluentWindow,
    NavigationAvatarWidget,
    NavigationItemPosition
)

from .LogManagePage import LogManagePage


class APPMainWindow(FluentWindow):
    """应用主窗口"""

    def __init__(self):
        super().__init__()

        # 日志管理界面
        self.log_manage_page = LogManagePage()

        self.initNavigation()
        self.initWindow()

    def initNavigation(self):
        self.addSubInterface(self.log_manage_page, FluentIcon.LIBRARY, "日志管理")
        # self.navigationInterface.addSeparator()

        # 底部头像按钮
        self.navigationInterface.addWidget(
            routeKey="avatar",
            widget=NavigationAvatarWidget("LuoTat", "ui/resource/LuoTat.jpg"),
            onClick=self._onAvatar,
            position=NavigationItemPosition.BOTTOM,
        )

    def initWindow(self):
        self.resize(1600, 900)
        self.setWindowIcon(QIcon(":/qfluentwidgets/images/logo.png"))
        self.setWindowTitle("结构化日志分析与可视化系统")

        # 把主界面居中
        screen = QApplication.primaryScreen()
        if screen:
            geo = screen.availableGeometry()
            self.move(
                geo.width() // 2 - self.width() // 2,
                geo.height() // 2 - self.height() // 2,
            )

    def _onAvatar(self):
        w = MessageBox(
            "支持作者🥰",
            "个人开发不易，如果这个项目帮助到了您，可以考虑请作者喝一瓶快乐水🥤。您的支持就是作者开发和维护项目的动力🚀",
            self,
        )
        w.yesButton.setText("来啦老弟")
        w.cancelButton.setText("下次一定")

        if w.exec():
            QDesktopServices.openUrl(QUrl("https://github.com/LuoTat"))