from .base_log_parser import BaseLogParser


def parser_register(cls):
    ParserFactory.register_parser(cls)
    return cls


class ParserFactory:
    """解析器工厂"""

    _all_parsers_type: list[type[BaseLogParser]] = list()

    @classmethod
    def register_parser(cls, parser_class: type[BaseLogParser]):
        cls._all_parsers_type.append(parser_class)

    @classmethod
    def get_all_parsers_type(cls) -> list[type[BaseLogParser]]:
        return cls._all_parsers_type.copy()
