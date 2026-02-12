# SPDX-License-Identifier: MIT
# This file implements the Drain algorithm for log parsing.
# Based on https://github.com/logpai/logparser/blob/master/logparser/Drain/Drain.py by LogPAI team

from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime
from math import isclose
from pathlib import Path

import polars as pl

from .base_log_parser import BaseLogParser
from .parse_result import ParseResult
from .utils import load_data, output_result


class LogCluster:
    __slots__ = ["log_template_tokens", "cluster_id"]

    _id_counter = 0

    def __init__(self, log_template_tokens: list[str]):
        self.cluster_id = LogCluster._id_counter
        self.log_template_tokens = log_template_tokens
        LogCluster._id_counter += 1

    def get_template(self) -> str:
        return " ".join(self.log_template_tokens)


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
        regex,
        depth=4,
        sim_th=0.4,
        max_children=100,
        delimiters: list[str] | None = None,
        parametrize_numeric_tokens=True,
    ) -> None:
        """Initialize Drain base parser.

        Args:
            depth: Depth of prefix tree (minimum 3).
            sim_th: Similarity threshold (0-1).
            max_children: Max children per tree node.
            delimiters: Extra delimiters for tokenization.
            parametrize_numeric_tokens: Whether to treat numeric tokens as wildcards.
        """
        super().__init__(log_format, regex)

        if depth < 3:
            raise ValueError("depth argument must be at least 3")

        self._depth = depth - 2  # max depth of a prefix tree node, starting from zero
        self._sim_th = sim_th
        self._max_children = max_children
        self._delimiters = delimiters if delimiters is not None else ["_"]
        self._parametrize_numeric_tokens = parametrize_numeric_tokens

        self._root_node = Node()
        self._id_to_cluster: dict[int, LogCluster] = {}

    @staticmethod
    def _has_numbers(s: str) -> bool:
        return any(char.isdigit() for char in s)

    def _fast_match(
        self, cluster_ids: list[int], tokens: list[str], include_params: bool
    ) -> LogCluster | None:
        max_sim: float = -1.0
        max_param_count = -1
        max_cluster = None

        for cluster_id in cluster_ids:
            cluster = self._id_to_cluster[cluster_id]
            cur_sim, param_count = type(self)._get_seq_distance(
                cluster.log_template_tokens, tokens, include_params
            )
            if cur_sim > max_sim or (
                isclose(cur_sim, max_sim) and param_count > max_param_count
            ):
                max_sim = cur_sim
                max_param_count = param_count
                max_cluster = cluster

        if max_sim >= self._sim_th:
            return max_cluster

        return None

    @staticmethod
    def _output_result(
        log_df: pl.DataFrame,
        structured_table_name: str,
        templates_table_name: str,
        keep_para: bool,
        cluster_results: list[LogCluster],
    ) -> None:
        output_result(
            log_df,
            [cluster.get_template() for cluster in cluster_results],
            structured_table_name,
            templates_table_name,
            keep_para,
        )

    def _get_content_as_tokens(self, content: str) -> list[str]:
        content = content.strip()
        for delimiter in self._delimiters:
            content = content.replace(delimiter, " ")
        content_tokens = content.split()
        return content_tokens

    def add_log_message(self, content: str) -> LogCluster:
        content_tokens = self._get_content_as_tokens(content)

        match_cluster = self._tree_search(content_tokens, False)

        # Match no existing log cluster
        if match_cluster is None:
            new_cluster = LogCluster(content_tokens)
            self._id_to_cluster[new_cluster.cluster_id] = new_cluster
            self._add_seq_to_prefix_tree(new_cluster)
            return new_cluster

        # Add the new log message to the existing cluster
        new_template_tokens = type(self)._create_template(
            content_tokens, match_cluster.log_template_tokens
        )
        if new_template_tokens != match_cluster.log_template_tokens:
            match_cluster.log_template_tokens = new_template_tokens

        return match_cluster

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

        log_df = load_data(log_file, self._log_format, self._regex, should_stop)
        contents = log_df["Content"].to_list()

        for idx, content in enumerate(contents, start=1):
            if should_stop():
                raise InterruptedError

            match_cluster = self.add_log_message(content)
            cluster_results.append(match_cluster)

            if idx % 10000 == 0 or idx == log_df.height:
                progress = idx * 100.0 / log_df.height
                print(f"Processed {progress:.1f}% of log lines.")
                if progress_callback:
                    progress_callback(int(progress))

        DrainBaseLogParser._output_result(
            log_df,
            structured_table_name,
            templates_table_name,
            keep_para,
            cluster_results,
        )

        print(f"Parsing done. [Time taken: {datetime.now() - start_time}]")
        return ParseResult(
            log_file, log_df.height, structured_table_name, templates_table_name
        )

    @staticmethod
    @abstractmethod
    def _create_template(seq1: list[str], seq2: list[str]) -> list[str]: ...

    @abstractmethod
    def _add_seq_to_prefix_tree(self, cluster: LogCluster) -> None: ...

    @staticmethod
    @abstractmethod
    def _get_seq_distance(
        seq1: list[str], seq2: list[str], include_params: bool
    ) -> tuple[float, int]: ...

    @abstractmethod
    def _tree_search(
        self, tokens: list[str], include_params: bool
    ) -> LogCluster | None: ...