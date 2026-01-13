from typing import Dict, Type

from .base_log_parser import BaseLogParser


def parser_register(cls):
    ParserFactory.register_parser(cls)
    return cls


class ParserFactory:
    """解析器工厂"""

    all_parsers: Dict[str, Type[BaseLogParser]] = {}

    @classmethod
    def get_parser_type(cls, parser_name: str) -> Type[BaseLogParser]:
        if parser_name not in cls.all_parsers:
            raise ValueError(f"未知的解析器: {parser_name}")
        return cls.all_parsers[parser_name]

    @classmethod
    def get_parser_description(cls, parser_name: str) -> str:
        parser_type = cls.get_parser_type(parser_name)
        return parser_type.description()

    @classmethod
    def get_all_parsers_name(cls) -> list[str]:
        return list(cls.all_parsers.keys())

    @classmethod
    def register_parser(cls, parser_class: Type[BaseLogParser]):
        cls.all_parsers[parser_class.name()] = parser_class