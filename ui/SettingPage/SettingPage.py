from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import QWidget
from qfluentwidgets import (
    ComboBoxSettingCard,
    CustomColorSettingCard,
    ExpandLayout,
    FluentIcon,
    InfoBar,
    InfoBarPosition,
    PrimaryPushSettingCard,
    ScrollArea,
    setThemeColor,
)

from modules.app_config import appcfg
from modules.duckdb_service import DuckDBService
from humanize import naturalsize


class SettingPage(ScrollArea):
    """设置页面"""

    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.setObjectName("SettingPage")

        self._scroll_widget = QWidget(self)
        self._main_layout = ExpandLayout(self._scroll_widget)

        self.setWidget(self._scroll_widget)
        self.setWidgetResizable(True)

        self._init_theme_color_card()
        self._init_language_card()
        self._init_compact_db_card()

        appcfg.appRestartSig.connect(self._on_need_restart)

    # ==================== 私有方法 ====================

    def _init_theme_color_card(self):
        self._theme_color_card = CustomColorSettingCard(
            appcfg.themeColor,
            FluentIcon.PALETTE,
            self.tr("主题色"),
            self.tr("调整你的应用的主题色"),
            self._scroll_widget,
        )
        self._theme_color_card.colorChanged.connect(lambda c: setThemeColor(c))
        self._main_layout.addWidget(self._theme_color_card)

    def _init_language_card(self):
        self.languageCard = ComboBoxSettingCard(
            appcfg.language,
            FluentIcon.LANGUAGE,
            self.tr("语言"),
            self.tr("选择界面所使用的语言"),
            ["简体中文", "English", self.tr("跟随系统设置")],
            self._scroll_widget,
        )
        self._main_layout.addWidget(self.languageCard)

    def _init_compact_db_card(self):
        self._compact_db_card = PrimaryPushSettingCard(
            self.tr("立即优化"),
            FluentIcon.SPEED_HIGH,
            self.tr("优化数据库"),
            self.tr("压缩数据库文件，释放已删除数据占用的磁盘空间"),
            self._scroll_widget,
        )
        self._compact_db_card.clicked.connect(self._on_compact_database)
        self._main_layout.addWidget(self._compact_db_card)

    # ==================== 槽函数 ====================

    @Slot()
    def _on_need_restart(self):
        """显示重启提示"""
        InfoBar.success(
            title=self.tr("更新成功"),
            content=self.tr("配置在重启软件后生效"),
            orient=Qt.Orientation.Horizontal,
            isClosable=True,
            position=InfoBarPosition.TOP,
            duration=3000,
            parent=self,
        )

    @Slot()
    def _on_compact_database(self):
        """压缩优化数据库"""
        try:
            original_size, new_size = DuckDBService.compact_database()
            saved = original_size - new_size
            InfoBar.success(
                title=self.tr("优化完成"),
                content=self.tr("数据库已压缩：{0} → {1}，释放了 {2}").format(
                    naturalsize(original_size, True),
                    naturalsize(new_size, True),
                    naturalsize(saved, True),
                ),
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=3000,
                parent=self,
            )
        except Exception as e:
            InfoBar.error(
                title=self.tr("优化失败"),
                content=str(e),
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=5000,
                parent=self,
            )
