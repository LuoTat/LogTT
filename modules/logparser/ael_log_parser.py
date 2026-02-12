# =========================================================================
# Copyright (C) 2016-2023 LOGPAI (https://github.com/logpai).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =========================================================================


from collections.abc import Callable
from datetime import datetime
from functools import reduce
from pathlib import Path

import polars as pl

from .base_log_parser import BaseLogParser
from .parse_result import ParseResult
from .parser_factory import parser_register
from .utils import load_data, output_result


class Event:
    __slots__ = ("logs", "event_str", "event_tokens", "merged")

    def __init__(self, log_idx, event_str=""):
        self.logs = [log_idx]
        self.event_str = event_str
        self.event_tokens = event_str.split()
        self.merged = False


@parser_register
class AELLogParser(BaseLogParser):
    def __init__(self, log_format, regex, min_event_count=2, merge_percent=0.5):
        """
        Attributes
        ----------
            min_event_count : minimum number of events to trigger reconciliation
            merge_percent : maximum percentage of difference to merge two events
        """
        super().__init__(log_format, regex)

        self._min_event_count = min_event_count
        self._merge_percent = merge_percent
        self._merged_events = []
        self._bins = {}

    def _output_result(
        self,
        log_df: pl.DataFrame,
        structured_table_name: str,
        templates_table_name: str,
        keep_para: bool,
    ):
        log_templates = [""] * log_df.height
        for event in self._merged_events:
            for log_idx in event.logs:
                log_templates[log_idx] = event.event_str

        output_result(
            log_df,
            log_templates,
            structured_table_name,
            templates_table_name,
            keep_para,
        )

    @staticmethod
    def _merge_event(e1, e2):
        for pos in range(len(e1.event_tokens)):
            if e1.event_tokens[pos] != e2.event_tokens[pos]:
                e1.event_tokens[pos] = "<*>"

        e1.logs.extend(e2.logs)
        e1.event_str = " ".join(e1.event_tokens)

        return e1

    @staticmethod
    def _has_diff(tokens1, tokens2, merge_percent):
        diff = sum(1 for t1, t2 in zip(tokens1, tokens2) if t1 != t2)
        return 0 < diff / len(tokens1) <= merge_percent

    def _reconcile(self):
        """
        Merge events if a bin has too many events
        """
        for value in self._bins.values():
            if len(value["Events"]) > self._min_event_count:
                to_be_merged = []
                for e1 in value["Events"]:
                    if e1.merged:
                        continue
                    e1.merged = True
                    to_be_merged.append([e1])

                    for e2 in value["Events"]:
                        if e2.merged:
                            continue
                        if AELLogParser._has_diff(
                            e1.event_tokens, e2.event_tokens, self._merge_percent
                        ):
                            to_be_merged[-1].append(e2)
                            e2.merged = True
                for group in to_be_merged:
                    merged_event = reduce(AELLogParser._merge_event, group)
                    self._merged_events.append(merged_event)
            else:
                self._merged_events.extend(value["Events"])

    def _categorize(self, log_df: pl.DataFrame):
        """
        Abstract templates bin by bin
        使用 dict 做 O(1) 查找替代线性扫描，提前提取 Content 列避免逐次跨语言取值
        """
        contents = log_df["Content"].to_list()
        for value in self._bins.values():
            value["Events"] = []
            event_map: dict[str, Event] = {}

            for log_idx in value["Logs"]:
                log = contents[log_idx]
                if log in event_map:
                    event_map[log].logs.append(log_idx)
                else:
                    event = Event(log_idx, log)
                    event_map[log] = event
                    value["Events"].append(event)

    def _tokenize(self, log_df: pl.DataFrame):
        """
        Put logs into bins according to (# of '<*>', # of token)
        使用 Polars 向量化操作替代 Python 循环
        """
        groups = (
            pl.DataFrame(
                {
                    "token_count": log_df["Content"].str.count_matches(r"\S+"),
                    "para_count": log_df["Content"].str.count_matches(r"<\*>"),
                }
            )
            .with_row_index("idx")
            .group_by(["token_count", "para_count"])
            .agg(pl.col("idx"))
        )
        for row in groups.iter_rows():
            self._bins[(row[0], row[1])] = {"Logs": row[2]}

    def parse(
        self,
        log_file: Path,
        structured_table_name: str,
        templates_table_name: str,
        should_stop: Callable[[], bool],
        keep_para: bool = False,
        progress_callback: Callable[[int], None] | None = None,
    ) -> ParseResult:
        print(f"Parsing file: {log_file}")
        start_time = datetime.now()

        log_df = load_data(log_file, self._log_format, self._regex, should_stop)

        self._tokenize(log_df)
        self._categorize(log_df)
        self._reconcile()

        self._output_result(
            log_df, structured_table_name, templates_table_name, keep_para
        )

        print(f"Parsing done. [Time taken: {datetime.now() - start_time}]")
        return ParseResult(
            log_file, log_df.height, structured_table_name, templates_table_name
        )

    @staticmethod
    def name() -> str:
        return "AEL"

    @staticmethod
    def description() -> str:
        return "AEL 是一种基于事件抽象层次的日志解析方法。"
