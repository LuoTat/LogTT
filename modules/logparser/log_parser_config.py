from .builtin_masking import BUILTIN_MASKINGS


class LogParserConfig:
    def __init__(
        self,
        name: str,
        log_format: str,
        timestamp_fields: list[str],
        timestamp_format: str,
        user_maskings: list[tuple[str, str]] | None = None,
        delimiters: str = "",
        use_builtin_maskings: bool = True,
        ex_args: dict[str, dict[str, int | float]] | None = None,
    ):
        self.name = name
        self.log_format = log_format
        self.timestamp_fields = timestamp_fields
        self.timestamp_format = timestamp_format
        self.user_maskings = user_maskings or []
        self.delimiters = delimiters
        self.use_builtin_maskings = use_builtin_maskings
        self.ex_args = ex_args or {}

    @property
    def maskings(self) -> list[tuple[str, str]]:
        if self.use_builtin_maskings:
            return self.user_maskings + BUILTIN_MASKINGS
        else:
            return self.user_maskings
