from typing import Any

from .builtin_masking import BUILTIN_MASKING
from .parser_factory import LogParserClassProtocol


class LogParserConfig:
    def __init__(
        self,
        name: str,
        log_format: str,
        masking: list[tuple[str, str]] | None = None,
        delimiters: list[str] | None = None,
        use_builtin_masking: bool = True,
        ex_args: dict[LogParserClassProtocol, dict[str, Any]] | None = None,
    ):
        self.name = name
        self.log_format = log_format
        self.masking = masking
        self.user_masking = masking
        self.delimiters = delimiters
        self.use_builtin_masking = use_builtin_masking
        self.ex_args = ex_args or {}

        if use_builtin_masking:
            if self.masking is None:
                self.masking = BUILTIN_MASKING
            else:
                self.masking.extend(BUILTIN_MASKING)
