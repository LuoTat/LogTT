# =========================================================================
# Copyright (C) 2016-2023 LOGPAI (https://github.com/logpai).
# Copyright (C) 2023 gaiusyu
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


from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TypeAlias

import polars as pl

from .base_log_parser import BaseLogParser, Content, Token
from .parse_result import ParseResult
from .parser_factory import parser_register
from .utils import load_data, output_result


class FToken:
    __slots__ = ["row", "col", "token", "freq"]

    def __init__(self, row: int, col: int, token: Token, freq: int = 1):
        self.row = row
        self.col = col
        self.token = token
        self.freq = freq


@dataclass(frozen=True, slots=True)
class FTuple:
    freq: int
    count: int


class FCounter:
    __slots__ = ["freq_counter"]

    def __init__(self, fcontent: FContent):
        freq_list = [ftoken.freq for ftoken in fcontent]
        self.freq_counter = [
            FTuple(freq_tuple[0], freq_tuple[1])
            for freq_tuple in Counter(freq_list).most_common()
        ]

    def __iter__(self):
        return iter(self.freq_counter)

    def __len__(self):
        return len(self.freq_counter)

    def __getitem__(self, index):
        return self.freq_counter[index]

    def sort_by_count(self):
        self.freq_counter.sort(key=lambda freq_tuple: freq_tuple.count, reverse=True)

    def get_max_fre(self) -> int:
        return max(freq_tuple.freq for freq_tuple in self.freq_counter)


FContent: TypeAlias = list[FToken]


@parser_register
class BrainLogParser(BaseLogParser):
    def __init__(
        self,
        log_format,
        masking,
        delimiters: list[str] | None = None,
        var_thr=2,
    ):
        """
        Args:
            var_thr : Threshold for determining variable columns.
        """
        super().__init__(log_format, masking, delimiters)

        self._var_thr = var_thr

    @staticmethod
    def _output_result(
        log_df: pl.DataFrame,
        fcontents_group: dict[int, list[FContent]],
        structured_table_name: str,
        templates_table_name: str,
        keep_para: bool,
    ):
        log_templates = [""] * log_df.height
        for fcontents in fcontents_group.values():
            for fcontent in fcontents:
                template_tokens = [ftoken.token for ftoken in fcontent]
                log_templates[fcontent[0].row] = " ".join(template_tokens)

        output_result(
            log_df,
            log_templates,
            structured_table_name,
            templates_table_name,
            keep_para,
        )

    def _down_split(
        self,
        root_rows: dict[FTuple, list[int]],
        fcontents: list[FContent],
    ):
        for root, rows in root_rows.items():
            col_max_fre: dict[int, int] = {}
            col_tokens: dict[int, set[str]] = {}

            for row in rows:
                for ftoken in fcontents[row]:
                    col_max_fre[ftoken.col] = max(
                        col_max_fre.get(ftoken.col, 0),
                        ftoken.freq,
                    )
                    col_tokens.setdefault(ftoken.col, set()).add(ftoken.token)

            # 获取子节点列：最大频率小于 root.fre 的列
            child_cols = [
                col for col, max_fre in col_max_fre.items() if max_fre < root.freq
            ]
            child_cols.sort(key=lambda col: len(col_tokens.get(col, set())))
            # 获取变量列：子节点列中不同词数量大于等于 threshold 的列
            variable_child_cols = {
                col
                for col in child_cols
                if len(col_tokens.get(col, set())) >= self._var_thr
            }
            # 将变量列的词置为 <*>
            for row in rows:
                for ftoken in fcontents[row]:
                    if ftoken.col in variable_child_cols:
                        ftoken.token = "<*>"

    @staticmethod
    def _up_split(root_rows: dict[FTuple, list[int]], fcontents: list[FContent]):
        for root, rows in root_rows.items():
            col_max_fre: dict[int, int] = {}
            col_tokens: dict[int, set[Token]] = {}

            for row in rows:
                for ftoken in fcontents[row]:
                    col_max_fre[ftoken.col] = max(
                        col_max_fre.get(ftoken.col, 0),
                        ftoken.freq,
                    )
                    col_tokens.setdefault(ftoken.col, set()).add(ftoken.token)

            # 获取父节点列：最大频率大于 root.fre 的列
            parent_cols = {
                col for col, max_fre in col_max_fre.items() if max_fre > root.freq
            }
            # 获取变量列：父节点列中有多个不同词的列
            variable_parent_cols = {
                col for col in parent_cols if len(col_tokens[col]) > 1
            }
            # 将变量列的词置为 <*>
            for row in rows:
                for ftoken in fcontents[row]:
                    if ftoken.col in variable_parent_cols:
                        ftoken.token = "<*>"

    @staticmethod
    def _find_root(
        fcounters: list[FCounter],
        alpha: float = 0.5,
    ) -> dict[FTuple, list[int]]:
        root_rows: dict[FTuple, list[int]] = {}
        for idx, fcounter in enumerate(fcounters):
            fcounter.sort_by_count()  # 按 count 排序
            freq_thr = fcounter.get_max_fre() * alpha
            matched_freq_tuple = fcounter[0]  # 默认选出现次数最高的 FTuple
            for freq_tuple in fcounter:
                if freq_tuple.freq >= freq_thr:
                    matched_freq_tuple = freq_tuple
                    break
            root_rows.setdefault(matched_freq_tuple, []).append(idx)

        return root_rows

    @staticmethod
    def _get_fcounters_group(
        fcontents_group: dict[int, list[FContent]],
    ) -> dict[int, list[FCounter]]:
        fcounters_group: dict[int, list[FCounter]] = {}
        for length, fcontents in fcontents_group.items():
            for fcontent in fcontents:
                fcounter = FCounter(fcontent)
                fcounters_group.setdefault(length, []).append(fcounter)

        return fcounters_group

    @staticmethod
    def _get_fcontents_group(contents: list[Content]) -> dict[int, list[FContent]]:
        ftoken_group: dict[int, list[FContent]] = {}
        for row, content in enumerate(contents):
            ftoken_list: list[FToken] = []
            for col, token in enumerate(content):
                ftoken_list.append(FToken(row, col, token))
            ftoken_group.setdefault(len(content), []).append(ftoken_list)

        for fcontents in ftoken_group.values():
            # 同组内每行长度一致，所以可以直接 zip(*all_ftokens) 按列取数据
            col_counters = [
                Counter(
                    ftoken.token for ftoken in col_ftokens
                )  # 统计该列每个词出现次数
                for col_ftokens in zip(*fcontents)
            ]
            # 把频率写回每个 FToken
            for fcontent in fcontents:
                for col, ftoken in enumerate(fcontent):
                    ftoken.freq = col_counters[col][ftoken.token]

        return ftoken_group

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

        log_df = load_data(log_file, self._log_format, should_stop)
        # 预处理日志内容：掩码处理 + 分词
        log_df = self._mask_log_df(log_df)
        log_df = self._split_log_df(log_df)
        contents: list[Content] = log_df["Tokens"].to_list()

        fcontents_group = BrainLogParser._get_fcontents_group(contents)
        fcounters_group = BrainLogParser._get_fcounters_group(fcontents_group)

        for fcontents, fcounters in zip(
            fcontents_group.values(),
            fcounters_group.values(),
        ):
            root_rows = BrainLogParser._find_root(fcounters, 0.5)

            BrainLogParser._up_split(root_rows, fcontents)
            self._down_split(root_rows, fcontents)

        BrainLogParser._output_result(
            log_df,
            fcontents_group,
            structured_table_name,
            templates_table_name,
            keep_para,
        )

        print(f"Parsing done. [Time taken: {datetime.now() - start_time}]")
        return ParseResult(
            log_file,
            log_df.height,
            structured_table_name,
            templates_table_name,
        )

    @staticmethod
    def name() -> str:
        return "Brain"

    @staticmethod
    def description() -> str:
        return "Brain 是一种基于双向树结构的高效日志模板提取算法。"
