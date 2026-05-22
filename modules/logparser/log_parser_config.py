from .builtin_masking import BUILTIN_MASKS


class LogParserConfig:
    def __init__(
        self,
        name: str,
        log_format: str,
        timestamp_fields: list[str],
        timestamp_format: str,
        user_masks: list[tuple[str, str]] | None = None,
        delimiters: str = "",
        use_builtin_masks: bool = True,
        ex_args: dict[str, dict[str, int | float]] | None = None,
    ):
        self.name = name
        self.log_format = log_format
        self.timestamp_fields = timestamp_fields
        self.timestamp_format = timestamp_format
        self.user_masks = user_masks or []
        self.delimiters = delimiters
        self.use_builtin_masks = use_builtin_masks
        self.ex_args = ex_args or {}

    @property
    def masks(self) -> list[tuple[str, str]]:
        if self.use_builtin_masks:
            return self.user_masks + BUILTIN_MASKS
        else:
            return self.user_masks
