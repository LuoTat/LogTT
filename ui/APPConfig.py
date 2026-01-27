from qfluentwidgets import (
    qconfig,
    QConfig,
    ConfigItem
)


class AppConfig(QConfig):
    """应用配置类"""

    # LogManagePage 表格列宽
    logTableColumnWidths = ConfigItem(
        "LogManagePage",
        "LogTableColumnWidths",
        [],
    )


# 创建全局配置实例
appcfg = AppConfig()

# 加载配置文件
qconfig.load("config/config.json", appcfg)