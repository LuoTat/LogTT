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


from datetime import datetime
from functools import reduce

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
    def __init__(
        self,
        log_id,
        log_file,
        log_format,
        regex,
        should_stop,
        progress_callback=None,
        keep_para=False,
        min_event_count=2,
        merge_percent=0.5,
    ):
        """
        Attributes
        ----------
            min_event_count : minimum number of events to trigger reconciliation
            merge_percent : maximum percentage of difference to merge two events
        """
        super().__init__(log_id, log_file, log_format, regex, should_stop, progress_callback, keep_para)

        self._min_event_count = min_event_count
        self._merge_percent = merge_percent
        self._merged_events = []
        self._bins = {}
        self._df_log = None

    def _output_result(self):
        log_templates = [""] * self._df_log.height
        for event in self._merged_events:
            for log_idx in event.logs:
                log_templates[log_idx] = event.event_str

        output_result(
            self._df_log,
            log_templates,
            self._output_dir,
            self._log_structured_file,
            self._log_templates_file,
            self._keep_para,
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
                        if self._has_diff(e1.event_tokens, e2.event_tokens, self._merge_percent):
                            to_be_merged[-1].append(e2)
                            e2.merged = True
                for group in to_be_merged:
                    merged_event = reduce(self._merge_event, group)
                    self._merged_events.append(merged_event)
            else:
                self._merged_events.extend(value["Events"])

    def _categorize(self):
        """
        Abstract templates bin by bin
        使用 dict 做 O(1) 查找替代线性扫描，提前提取 Content 列避免逐次跨语言取值
        """
        contents = self._df_log["Content"].to_list()
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

    def _tokenize(self):
        """
        Put logs into bins according to (# of '<*>', # of token)
        使用 Polars 向量化操作替代 Python 循环
        """
        groups = (
            pl.DataFrame(
                {
                    "token_count": self._df_log["Content"].str.count_matches(r"\S+"),
                    "para_count": self._df_log["Content"].str.count_matches(r"<\*>"),
                }
            )
            .with_row_index("idx")
            .group_by(["token_count", "para_count"])
            .agg(pl.col("idx"))
        )
        for row in groups.iter_rows():
            self._bins[(row[0], row[1])] = {"Logs": row[2]}

    def parse(self) -> ParseResult:
        print(f"Parsing file: {self._log_file}")
        start_time = datetime.now()

        self._df_log = load_data(self._log_file, self._log_format, self._regex, self._should_stop)

        self._tokenize()
        self._categorize()
        self._reconcile()

        self._output_result()

        print(f"Parsing done. [Time taken: {datetime.now() - start_time}]")
        return ParseResult(self._log_file, self._df_log.height, self._log_structured_file, self._log_templates_file)

    @staticmethod
    def name() -> str:
        return "AEL"

    @staticmethod
    def description() -> str:
        return "AEL 是一种基于事件抽象层次的日志解析方法。"
