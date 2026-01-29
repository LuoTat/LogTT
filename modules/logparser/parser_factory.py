from .base_log_parser import BaseLogParser


def parser_register(cls):
    ParserFactory.register_parser(cls)
    return cls


class ParserFactory:
    """解析器工厂"""

    _all_parsers: dict[str, type[BaseLogParser]] = {}

    @classmethod
    def get_parser_type(cls, parser_name: str) -> type[BaseLogParser]:
        if parser_name not in cls._all_parsers:
            raise ValueError(f"未知的解析器: {parser_name}")
        return cls._all_parsers[parser_name]

    @classmethod
    def get_parser_description(cls, parser_name: str) -> str:
        parser_type = cls.get_parser_type(parser_name)
        return parser_type.description()

    @classmethod
    def get_all_parsers_name(cls) -> list[str]:
        return list(cls._all_parsers.keys())

    @classmethod
    def register_parser(cls, parser_class: type[BaseLogParser]):
        cls._all_parsers[parser_class.name()] = parser_class
