from string import Template

from PySide6.QtCore import QUrl, Slot
from PySide6.QtGui import QDesktopServices, QIcon
from PySide6.QtWidgets import QApplication
from qfluentwidgets import (
    FluentIcon,
    FluentWindow,
    MessageBox,
    NavigationAvatarWidget,
    NavigationItemPosition,
)

from .LogManagePage import LogManagePage
from .LogViewPage import LogViewPage
from .TemplateViewPage import TemplateViewPage


class APPMainWindow(FluentWindow):
    """应用主窗口"""

    def __init__(self):
        super().__init__()

        # 日志管理界面
        self.log_manage_page = LogManagePage(self)
        # 日志查看界面
        self.log_view_page = LogViewPage(self)
        # 模板查看界面
        self.template_view_page = TemplateViewPage(self)

        self.log_manage_page.viewLogRequested.connect(self._onViewLogRequested)
        self.log_manage_page.viewTemplateRequested.connect(self._onViewTemplateRequested)

        self._initNavigation()
        self._initWindow()

    def _initNavigation(self):
        self.addSubInterface(self.log_manage_page, FluentIcon.LIBRARY, "日志管理")
        self.addSubInterface(self.log_view_page, FluentIcon.DOCUMENT, "日志查看")
        self.addSubInterface(self.template_view_page, FluentIcon.PIE_SINGLE, "模板查看")
        # self.navigationInterface.addSeparator()

        # 底部头像按钮
        self.navigationInterface.addWidget(
            routeKey="avatar",
            widget=NavigationAvatarWidget("LuoTat", "ui/resource/LuoTat.jpg"),
            onClick=self._onAvatar,
            position=NavigationItemPosition.BOTTOM,
        )

    def _initWindow(self):
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

    # ==================== 槽函数 ====================

    @Slot(int)
    def _onViewLogRequested(self, log_id: int):
        """处理查看日志请求，跳转到日志查看页面"""
        self.log_view_page.setLog(log_id)
        self.switchTo(self.log_view_page)

    @Slot(int)
    def _onViewTemplateRequested(self, log_id: int):
        """处理查看模板请求，跳转到模板查看页面"""
        self.template_view_page.setLog(log_id)
        self.switchTo(self.template_view_page)

    @Slot()
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

    def closeEvent(self, event):
        # 如果有正在提取的任务，弹窗确认
        if self.log_manage_page.hasExtractingTasks():
            confirm = MessageBox(
                "有任务正在提取",
                "仍有日志模板正在提取，确认要关闭并终止所有任务吗？",
                self,
            )
            if confirm.exec():
                self.log_manage_page.interruptAllExtractTasks()
                event.accept()
                return

            event.ignore()
