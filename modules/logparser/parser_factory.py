def parser_register(cls):
    ParserFactory.register_parser(cls)
    return cls


class ParserFactory:
    """解析器工厂"""

    _all_parsers_type: list[type[object]] = []

    @classmethod
    def register_parser(cls, parser_class: type[object]):
        cls._all_parsers_type.append(parser_class)

    @classmethod
    def get_all_parsers_type(cls) -> list[type[object]]:
        return cls._all_parsers_type.copy()
