from qfluentwidgets import ConfigItem, QConfig, qconfig


class AppConfig(QConfig):
    """应用配置类"""

    # LogManagePage 表格列宽
    logTableColumnWidths = ConfigItem(
        "LogManagePage",
        "LogTableColumnWidths",
        list(),
    )

    # 用户自定义日志格式
    userFormatType = ConfigItem("UserConfig", "FormatType", list())


# 创建全局配置实例
appcfg = AppConfig()

# 加载配置文件
qconfig.load("config/config.json", appcfg)
