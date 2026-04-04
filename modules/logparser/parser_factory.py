from typing import Any, Protocol

from .param_descriptor import ParamDescriptor
from .parse_result import ParseResult


class LogParserProtocol(Protocol):
    def __init__(
        self,
        log_format: str,
        maskings: list[tuple[str, str]],
        delimiters: str,
        **kwargs: Any,
    ): ...
    def parse(self, *args: Any, **kwargs: Any) -> ParseResult: ...
    @staticmethod
    def name() -> str: ...
    @staticmethod
    def description() -> str: ...
    @staticmethod
    def get_param_descriptors() -> list[ParamDescriptor]: ...


def parser_register(cls: type[LogParserProtocol]):
    ParserFactory.register_parser(cls)


class ParserFactory:
    """解析器工厂"""

    _all_parsers_type: list[type[LogParserProtocol]] = []

    @classmethod
    def register_parser(cls, parser_class: type[LogParserProtocol]):
        cls._all_parsers_type.append(parser_class)

    @classmethod
    def get_all_parsers_type(cls) -> list[type[LogParserProtocol]]:
        return cls._all_parsers_type.copy()
