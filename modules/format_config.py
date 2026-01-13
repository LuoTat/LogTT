import yaml
from pathlib import Path
from dataclasses import dataclass, asdict

# 系统配置文件路径
FORMAT_CONFIG_FILE = Path(__file__).resolve().parent.parent / "config" / "format_type.yaml"

# 用户配置文件路径
USER_FORMAT_CONFIG_FILE = Path(__file__).resolve().parent.parent / "config" / "user_format_type.yaml"


@dataclass
class FormatConfig:
    log_format: str  # 日志格式字符串
    regex: list[str]  # 正则表达式列表


class FormatConfigManager:
    def __init__(self):
        self.format_configs: dict[str, FormatConfig] = {}
        self.user_format_configs: dict[str, FormatConfig] = {}

        if FORMAT_CONFIG_FILE.exists():
            with open(FORMAT_CONFIG_FILE, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

            for name, config in data.items():
                self.format_configs[name] = FormatConfig(config.get("log_format", ""), config.get("regex", []))

        if USER_FORMAT_CONFIG_FILE.exists():
            with open(USER_FORMAT_CONFIG_FILE, "r", encoding="utf-8") as f:
                user_data = yaml.safe_load(f) or {}

            for name, config in user_data.items():
                self.user_format_configs[name] = FormatConfig(config.get("log_format", ""), config.get("regex", []))

    def get_all_format_names(self) -> list[str]:
        return list(self.format_configs.keys()) + list(self.user_format_configs.keys())

    def get_format_config(self, name: str) -> FormatConfig | None:
        return self.format_configs.get(name) or self.user_format_configs.get(name)

    def save_custom_format(self, name: str, log_format: str, regex: list[str]) -> bool:
        try:
            # 保存为 FormatConfig 对象
            self.user_format_configs[name] = FormatConfig(log_format, regex)

            with open(USER_FORMAT_CONFIG_FILE, "w", encoding="utf-8") as f:
                yaml.safe_dump(
                    {k: asdict(v) for k, v in self.user_format_configs.items()},
                    f,
                    allow_unicode=True,
                    indent=2,
                    width=float("inf"),
                )

            return True
        except Exception:
            return False

    def is_format_name_exists(self, name: str) -> bool:
        return name in self.format_configs or name in self.user_format_configs