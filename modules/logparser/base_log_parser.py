from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path

from .parse_result import ParseResult


class BaseLogParser(ABC):
    """日志模板解析器基类"""

    def __init__(
        self,
        log_id: int,
        log_file: Path,
        log_format: str,
        regex: list[str],
        should_stop: Callable[[], bool],
        progress_callback: Callable[[int], None] | None = None,
        keep_para: bool = False,
    ):
        """
        Attributes
        ----------
            log_id : unique identifier for this log
            log_file : path of the input log file
            log_format : log format string
            regex : regular expressions used in preprocessing (step1)
            should_stop : callback function to check if the process should stop
            progress_callback : callback function to report progress (0-100)
            keep_para : whether to keep parameter list in structured log file
        """
        self._log_id = log_id
        self._log_file = log_file
        self._log_format = log_format
        self._regex = regex
        self._should_stop = should_stop
        self._progress_callback = progress_callback
        self._keep_para = keep_para
        self._output_dir = Path(__file__).resolve().parent.parent.parent / "tmp"
        self._log_structured_file = self._output_dir / f"{self._log_id}_{self._log_file.stem}_structured.csv"
        self._log_templates_file = self._output_dir / f"{self._log_id}_{self._log_file.stem}_templates.csv"

    @abstractmethod
    def parse(self) -> ParseResult:
        pass

    @staticmethod
    @abstractmethod
    def name() -> str:
        pass

    @staticmethod
    @abstractmethod
    def description() -> str:
        pass
