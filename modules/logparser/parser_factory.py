from typing import Any, Protocol

from .parse_result import ParseResult


class LogParserProtocol(Protocol):
    def parse(self, *args: Any, **kwargs: Any) -> ParseResult: ...


class LogParserClassProtocol(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> LogParserProtocol: ...
    @staticmethod
    def name() -> str: ...
    @staticmethod
    def description() -> str: ...


def parser_register(cls):
    ParserFactory.register_parser(cls)
    return cls


class ParserFactory:
    """解析器工厂"""

    _all_parsers_type: list[LogParserClassProtocol] = []

    @classmethod
    def register_parser(cls, parser_class: LogParserClassProtocol):
        cls._all_parsers_type.append(parser_class)

    @classmethod
    def get_all_parsers_type(cls) -> list[LogParserClassProtocol]:
        return cls._all_parsers_type.copy()
