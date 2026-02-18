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
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from pathlib import Path

import polars as pl

from .base_log_parser import BaseLogParser, Content
from .parse_result import ParseResult
from .parser_factory import parser_register
from .utils import load_data, output_result


class LogCluster:
    __slots__ = ["rows", "content", "merged"]

    def __init__(self, content: Content, rows: list[int]):
        self.content = content
        self.rows = rows
        self.merged = False

    def get_template(self):
        return " ".join(self.content)


@dataclass(frozen=True, slots=True)
class LogBinKey:
    token_count: int
    para_count: int


@parser_register
class AELLogParser(BaseLogParser):
    def __init__(
        self,
        log_format,
        masking=None,
        delimiters=None,
        *,
        log_cluster_thr=2,
        merge_thr=1,
    ):
        """
        Args:
            log_cluster_thr : minimum number of log_clusters to trigger reconciliation.
            merge_thr : maximum percentage of difference to merge two log_clusters.
        """
        super().__init__(log_format, masking, delimiters)

        self._log_cluster_thr = log_cluster_thr
        self._merge_thr = merge_thr

    @staticmethod
    def _output_result(
        log_df: pl.DataFrame,
        structured_table_name: str,
        templates_table_name: str,
        keep_para: bool,
        merged_log_clusters: list[LogCluster],
    ):
        log_templates = [""] * log_df.height
        for log_cluster in merged_log_clusters:
            for row in log_cluster.rows:
                log_templates[row] = log_cluster.get_template()

        output_result(
            log_df,
            log_templates,
            structured_table_name,
            templates_table_name,
            keep_para,
        )

    @staticmethod
    def _merge_log_cluster(
        log_cluster1: LogCluster,
        log_cluster2: LogCluster,
    ) -> LogCluster:
        for idx, (token1, token2) in enumerate(
            zip(log_cluster1.content, log_cluster2.content)
        ):
            if token1 != token2:
                log_cluster1.content[idx] = "<*>"

        log_cluster1.rows.extend(log_cluster2.rows)

        return log_cluster1

    def _has_diff(self, log_cluster1: LogCluster, log_cluster2: LogCluster):
        diff = sum(
            1
            for token1, token2 in zip(log_cluster1.content, log_cluster2.content)
            if token1 != token2
        )
        return 0 < diff / len(log_cluster1.content) <= self._merge_thr

    def _reconcile(
        self,
        log_bin: dict[LogBinKey, list[LogCluster]],
    ) -> list[LogCluster]:
        merged_log_clusters: list[LogCluster] = []
        for log_clusters in log_bin.values():
            if len(log_clusters) <= self._log_cluster_thr:
                merged_log_clusters.extend(log_clusters)
                continue

            log_cluster_groups: list[list[LogCluster]] = []
            for log_cluster1 in log_clusters:
                if log_cluster1.merged:
                    continue

                log_cluster1.merged = True
                log_cluster_groups.append([log_cluster1])

                for log_clusters2 in log_clusters:
                    if log_clusters2.merged:
                        continue

                    if self._has_diff(log_cluster1, log_clusters2):
                        log_clusters2.merged = True
                        log_cluster_groups[-1].append(log_clusters2)

            for group in log_cluster_groups:
                merged_log_cluster = reduce(AELLogParser._merge_log_cluster, group)
                merged_log_clusters.append(merged_log_cluster)

        return merged_log_clusters

    @staticmethod
    def _get_log_bins(log_df: pl.DataFrame) -> dict[LogBinKey, list[LogCluster]]:
        log_bin: dict[LogBinKey, list[LogCluster]] = {}
        groups = (
            log_df.select(
                pl.col("Tokens"),
                pl.col("Tokens").list.len().alias("token_count"),
                pl.col("Tokens")
                .list.eval(pl.element().str.count_matches(r"<§.*§>"))
                .list.sum()
                .alias("para_count"),
            )
            .with_row_index("idx")
            .group_by(["token_count", "para_count", "Tokens"])
            .agg(pl.col("idx"))
        )

        for row in groups.iter_rows():
            key = LogBinKey(row[0], row[1])
            event = LogCluster(row[2], row[3])
            log_bin.setdefault(key, []).append(event)

        return log_bin

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

        log_bin = self._get_log_bins(log_df)
        merged_log_clusters = self._reconcile(log_bin)

        AELLogParser._output_result(
            log_df,
            structured_table_name,
            templates_table_name,
            keep_para,
            merged_log_clusters,
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
        return "AEL 是一种通过分桶与相似度合并来自动抽取日志模板的解析方法。"
