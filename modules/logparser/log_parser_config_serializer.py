from typing import Any

from qfluentwidgets import ConfigSerializer

from .log_parser_config import LogParserConfig


class LogParserConfigSerializer(ConfigSerializer):
    """LogParserConfig 列表的序列化器"""

    def serialize(self, value: list[LogParserConfig]) -> list[dict[str, Any]]:
        return [
            {
                "name": config.name,
                "log_format": config.log_format,
                "user_maskings": config.user_maskings,
                "delimiters": config.delimiters,
                "use_builtin_maskings": config.use_builtin_maskings,
                "ex_args": config.ex_args,
            }
            for config in value
        ]

    def deserialize(self, value: list[dict[str, Any]]) -> list[LogParserConfig]:
        return [LogParserConfig(**config) for config in value]
