from PySide6.QtCore import QUrl, Slot
from PySide6.QtGui import QDesktopServices, QIcon
from PySide6.QtWidgets import QApplication, QWidget
from qfluentwidgets import (
    FluentIcon,
    FluentWindow,
    MessageBox,
    NavigationAvatarWidget,
    NavigationItemPosition,
    NavigationPushButton,
    Theme,
    isDarkTheme,
    setTheme,
)

from .ClusterVisualizationPage import ClusterVisualizationPage
from .LogManagePage import LogManagePage
from .LogViewPage import LogViewPage
from .SettingPage import SettingPage
from .StatAnalysisPage import StatAnalysisPage
from .TemplateAnalysisPage import TemplateAnalysisPage
from .TemplateViewPage import TemplateViewPage
from .TemporalAnalysisPage import TemporalAnalysisPage


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

        # 日志分析页面
        self.log_analysis_page = QWidget(self)
        self.log_analysis_page.setObjectName("LogAnalysisPage")
        # 统计分析界面（待实现）
        self.stat_analysis_page = StatAnalysisPage(self)
        # 时序分析界面（待实现）
        self.temporal_analysis_page = TemporalAnalysisPage(self)
        # 模板分析界面（待实现）
        self.template_analysis_page = TemplateAnalysisPage(self)
        # 聚类可视化界面（待实现）
        self.cluster_visualization_page = ClusterVisualizationPage(self)

        # 设置界面
        self.setting_page = SettingPage(self)
        self.setting_page.enableTransparentBackground()

        self.log_manage_page.viewLogRequested.connect(self._on_view_log_requested)
        self.log_manage_page.viewTemplateRequested.connect(
            self._on_view_template_requested
        )

        self._init_window()
        self._init_navigation()

    # ==================== 重写方法 ====================

    def closeEvent(self, event):
        # 如果有正在提取的任务，弹窗确认
        if self.log_manage_page.has_extracting_tasks():
            confirm = MessageBox(
                self.tr("有任务正在提取"),
                self.tr("仍有日志模板正在提取，确认要关闭并终止所有任务吗？"),
                self,
            )
            if confirm.exec():
                self.log_manage_page.kill_tasks()
                event.accept()
                return

            event.ignore()

    # ==================== 私有方法 ====================

    def _init_window(self):
        self.resize(1600, 900)
        self.setWindowIcon(QIcon(":/qfluentwidgets/images/logo.png"))
        self.setWindowTitle(self.tr("结构化日志分析与可视化系统"))

        # 把主界面居中
        screen = QApplication.primaryScreen()
        if screen:
            geo = screen.availableGeometry()
            self.move(
                geo.width() // 2 - self.width() // 2,
                geo.height() // 2 - self.height() // 2,
            )

    def _init_navigation(self):
        self.addSubInterface(
            self.log_manage_page,
            FluentIcon.LIBRARY,
            self.tr("日志管理"),
        )
        self.addSubInterface(
            self.log_view_page,
            FluentIcon.DOCUMENT,
            self.tr("日志查看"),
        )
        self.addSubInterface(
            self.template_view_page,
            FluentIcon.PIE_SINGLE,
            self.tr("模板查看"),
        )
        self.addSubInterface(
            self.log_analysis_page,
            FluentIcon.LINK,
            self.tr("日志分析"),
        )
        self.addSubInterface(
            self.stat_analysis_page,
            FluentIcon.LINK,
            self.tr("统计分析"),
            parent=self.log_analysis_page,
        )
        self.addSubInterface(
            self.temporal_analysis_page,
            FluentIcon.LINK,
            self.tr("时序分析"),
            parent=self.log_analysis_page,
        )
        self.addSubInterface(
            self.template_analysis_page,
            FluentIcon.LINK,
            self.tr("模板分析"),
            parent=self.log_analysis_page,
        )
        self.addSubInterface(
            self.cluster_visualization_page,
            FluentIcon.LINK,
            self.tr("聚类可视化"),
            parent=self.log_analysis_page,
        )

        # 主题切换按钮
        self.navigationInterface.addWidget(
            routeKey="theme_navigation_button",
            widget=NavigationPushButton(
                FluentIcon.CONSTRACT,
                self.tr("主题切换"),
                False,
            ),
            onClick=self._on_toggle_theme,
            position=NavigationItemPosition.BOTTOM,
        )
        # 底部头像按钮
        self.navigationInterface.addWidget(
            routeKey="avatar",
            widget=NavigationAvatarWidget("LuoTat", "resource/images/LuoTat.jpg"),
            onClick=self._on_avatar,
            position=NavigationItemPosition.BOTTOM,
        )
        self.addSubInterface(
            self.setting_page,
            FluentIcon.SETTING,
            self.tr("设置"),
            NavigationItemPosition.BOTTOM,
        )

    @staticmethod
    def _on_toggle_theme():
        if not isDarkTheme():
            setTheme(Theme.DARK, save=True)
        else:
            setTheme(Theme.LIGHT, save=True)

    # ==================== 槽函数 ====================

    @Slot(int)
    def _on_view_log_requested(self, log_id: int):
        """处理查看日志请求，跳转到日志查看页面"""
        self.log_view_page.set_log(log_id)
        self.switchTo(self.log_view_page)

    @Slot(int)
    def _on_view_template_requested(self, log_id: int):
        """处理查看模板请求，跳转到模板查看页面"""
        self.template_view_page.set_log(log_id)
        self.switchTo(self.template_view_page)

    @Slot()
    def _on_avatar(self):
        w = MessageBox(
            self.tr("支持作者🥰"),
            self.tr(
                "个人开发不易，如果这个项目帮助到了您，可以考虑请作者喝一瓶快乐水🥤。您的支持就是作者开发和维护项目的动力🚀"
            ),
            self,
        )
        w.yesButton.setText(self.tr("来啦老弟"))
        w.cancelButton.setText(self.tr("下次一定"))

        if w.exec():
            QDesktopServices.openUrl(QUrl("https://github.com/LuoTat"))
