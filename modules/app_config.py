from qfluentwidgets import ConfigItem, QConfig, qconfig

from .constants import CONFIG_PATH


class AppConfig(QConfig):
    """应用配置类"""

    # LogManagePage 表格列宽
    logTableColumnWidths = ConfigItem(
        "LogManagePage",
        "LogTableColumnWidths",
        [],
    )

    # 用户自定义日志格式
    userFormatType = ConfigItem("UserConfig", "FormatType", [])


# 创建全局配置实例
appcfg = AppConfig()

# 加载配置文件
qconfig.load(CONFIG_PATH, appcfg)
