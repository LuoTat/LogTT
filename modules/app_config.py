from enum import Enum

from PySide6.QtCore import QLocale
from qfluentwidgets import (
    ConfigItem,
    ConfigSerializer,
    OptionsConfigItem,
    OptionsValidator,
    QConfig,
    qconfig,
)

from .constants import CONFIG_PATH


class Language(Enum):
    """Language enumeration"""

    CHINESE_SIMPLIFIED = QLocale(QLocale.Language.Chinese, QLocale.Country.China)
    ENGLISH = QLocale(QLocale.Language.English, QLocale.Country.UnitedStates)
    AUTO = QLocale()


class LanguageSerializer(ConfigSerializer):
    """Language serializer"""

    def serialize(self, language: Language) -> str:
        return language.value.name() if language != Language.AUTO else "Auto"

    def deserialize(self, value: str):
        return Language(QLocale(value)) if value != "Auto" else Language.AUTO


class AppConfig(QConfig):
    """应用配置类"""

    language = OptionsConfigItem(
        "MainWindow",
        "Language",
        Language.AUTO,
        OptionsValidator(Language),
        LanguageSerializer(),
        restart=True,
    )

    # LogManagePage 表格列宽
    logTableColumnWidths = ConfigItem(
        "LogManagePage",
        "LogTableColumnWidths",
        [],
    )

    # 用户自定义日志格式
    userFormatType = ConfigItem("LogConfig", "FormatType", [])


# 创建全局配置实例
appcfg = AppConfig()

# 加载配置文件
qconfig.load(CONFIG_PATH, appcfg)
