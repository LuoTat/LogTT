from typing import Any, Protocol

from .param_descriptor import ParamDescriptor
from .parse_result import ParseResult


class LogParserProtocol(Protocol):
    def parse(self, *args: Any, **kwargs: Any) -> ParseResult: ...


class LogParserClassProtocol(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> LogParserProtocol: ...
    @staticmethod
    def name() -> str: ...
    @staticmethod
    def description() -> str: ...
    @staticmethod
    def get_param_descriptors() -> list[ParamDescriptor]: ...


def parser_register(cls: LogParserClassProtocol):
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
