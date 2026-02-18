# SPDX-License-Identifier: MIT
# This file implements the Drain algorithm for log parsing.
# Based on https://github.com/logpai/logparser/blob/master/logparser/Drain/Drain.py by LogPAI team

from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime
from math import isclose
from pathlib import Path

import polars as pl

from .base_log_parser import BaseLogParser, Content
from .parse_result import ParseResult
from .utils import load_data, output_result


class LogCluster:
    __slots__ = ["content", "cluster_id"]

    _id_counter = 0

    def __init__(self, content: Content):
        self.content = content
        self.cluster_id = LogCluster._id_counter
        LogCluster._id_counter += 1

    def get_template(self) -> str:
        return " ".join(self.content)


class Node:
    __slots__ = ["children_node", "cluster_ids"]

    def __init__(self):
        self.children_node: dict[str, Node] = {}
        self.cluster_ids: list[int] = []


class DrainBaseLogParser(BaseLogParser, ABC):
    """Drain算法的基类"""

    def __init__(
        self,
        log_format,
        masking=None,
        delimiters=None,
        depth=4,
        children=100,
        sim_thr=0.4,
    ):
        """
        Args:
            depth: Depth of prefix tree (minimum 3).
            sim_thr: Similarity threshold (0-1).
            children: Max children per tree node.
        """
        super().__init__(log_format, masking, delimiters)

        if depth < 3:
            raise ValueError("depth argument must be at least 3")

        self._depth = depth
        self._children = children
        self._sim_thr = sim_thr

        self._root_node = Node()
        self._id_to_cluster: dict[int, LogCluster] = {}

    def _fast_match(
        self,
        cluster_ids: list[int],
        content: Content,
        include_params: bool,
    ) -> LogCluster | None:
        max_sim: float = -1.0
        max_param_count = -1
        max_cluster = None

        for cluster_id in cluster_ids:
            cluster = self._id_to_cluster[cluster_id]
            cur_sim, param_count = type(self)._get_seq_distance(
                cluster.content, content, include_params
            )
            if cur_sim > max_sim or (
                isclose(cur_sim, max_sim) and param_count > max_param_count
            ):
                max_sim = cur_sim
                max_param_count = param_count
                max_cluster = cluster

        if max_sim >= self._sim_thr:
            return max_cluster

        return None

    @staticmethod
    def _output_result(
        log_df: pl.DataFrame,
        cluster_results: list[LogCluster],
        structured_table_name: str,
        templates_table_name: str,
        keep_para: bool,
    ) -> None:
        output_result(
            log_df,
            [cluster.get_template() for cluster in cluster_results],
            structured_table_name,
            templates_table_name,
            keep_para,
        )

    def _add_content(self, content: Content) -> LogCluster:
        match_cluster = self._tree_search(content, False)

        # Match no existing log cluster
        if match_cluster is None:
            new_cluster = LogCluster(content)
            self._id_to_cluster[new_cluster.cluster_id] = new_cluster
            self._add_seq_to_prefix_tree(new_cluster)
            return new_cluster

        # Add the new log message to the existing cluster
        new_template_tokens = type(self)._create_template(
            content, match_cluster.content
        )
        if new_template_tokens != match_cluster.content:
            match_cluster.content = new_template_tokens

        return match_cluster

    def add_log_message(self, log: str) -> LogCluster:
        # 预处理日志内容：掩码处理 + 分词
        mask_log = self._mask_log(log)
        content = self._split_log(mask_log)

        return self._add_content(content)

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
        cluster_results = []

        log_df = load_data(log_file, self._log_format, should_stop)
        # 预处理日志内容：掩码处理 + 分词
        log_df = self._mask_log_df(log_df)
        log_df = self._split_log_df(log_df)
        contents: list[Content] = log_df["Tokens"].to_list()

        for idx, content in enumerate(contents, start=1):
            if should_stop():
                raise InterruptedError

            match_cluster = self._add_content(content)
            cluster_results.append(match_cluster)

            if idx % 10000 == 0 or idx == log_df.height:
                progress = idx * 100.0 / log_df.height
                print(f"Processed {progress:.1f}% of log lines.")
                if progress_callback:
                    progress_callback(int(progress))

        DrainBaseLogParser._output_result(
            log_df,
            cluster_results,
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
    @abstractmethod
    def _create_template(content1: Content, content2: Content) -> Content: ...

    @abstractmethod
    def _add_seq_to_prefix_tree(self, cluster: LogCluster) -> None: ...

    @staticmethod
    @abstractmethod
    def _get_seq_distance(
        content1: Content, content2: Content, include_params: bool
    ) -> tuple[float, int]: ...

    @abstractmethod
    def _tree_search(
        self, content: Content, include_params: bool
    ) -> LogCluster | None: ...
