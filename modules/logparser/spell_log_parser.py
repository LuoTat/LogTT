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
from pathlib import Path

import polars as pl

from .base_log_parser import BaseLogParser, Content, Token
from .parse_result import ParseResult
from .parser_factory import parser_register
from .utils import load_data, output_result


class LogCluster:
    __slots__ = ["content", "cluster_id"]

    _id_counter = 0

    def __init__(self, content: Content):
        self.cluster_id = LogCluster._id_counter
        self.content = content
        LogCluster._id_counter += 1

    def get_template(self) -> str:
        return " ".join(self.content)


class Node:
    __slots__ = ["children_node", "cluster_id", "template_no"]

    def __init__(self):
        self.children_node: dict[str, Node] = {}
        self.cluster_id: int = -1
        self.template_no: int = 0


@parser_register
class SpellLogParser(BaseLogParser):
    def __init__(
        self,
        log_format,
        masking=None,
        delimiters=None,
        sim_thr=0.5,
    ):
        """
        Args:
            sim_thr : Similarity threshold (0-1).
        """
        super().__init__(log_format, masking, delimiters)

        self._sim_thr = sim_thr

        self._root_node = Node()
        self._id_to_cluster: dict[int, LogCluster] = {}

    @staticmethod
    def _output_result(
        log_df: pl.DataFrame,
        cluster_results: list[LogCluster],
        structured_table_name: str,
        templates_table_name: str,
        keep_para: bool,
    ):
        output_result(
            log_df,
            [cluster.get_template() for cluster in cluster_results],
            structured_table_name,
            templates_table_name,
            keep_para,
        )

    @staticmethod
    def _create_template(lcs: list[Token], content: Content) -> list[str]:
        ret_val = []
        if not lcs:
            return ret_val

        lcs.reverse()
        for i, token in enumerate(content):
            if token == lcs[-1]:
                ret_val.append(token)
                lcs.pop()
            else:
                ret_val.append("<*>")
            if not lcs:
                if i < len(content) - 1:
                    ret_val.append("<*>")
                break
        return ret_val

    def _remove_seq_from_prefix_tree(self, cluster: LogCluster) -> None:
        parent = self._root_node
        const_content = [w for w in cluster.content if w != "<*>"]

        for token in const_content:
            if token in parent.children_node:
                matched_node = parent.children_node[token]
                if matched_node.template_no == 1:
                    del parent.children_node[token]
                    break
                else:
                    matched_node.template_no -= 1
                    parent = matched_node

    def _add_seq_to_prefix_tree(self, new_cluster: LogCluster) -> None:
        cur_node = self._root_node
        # seq = [w for w in new_cluster.log_template if w != "<*>"]

        for token in new_cluster.content:
            if token in cur_node.children_node:
                cur_node.children_node[token].template_no += 1
            else:
                new_node = Node()
                cur_node.children_node[token] = new_node
                new_node.template_no += 1
            cur_node = cur_node.children_node[token]

        assert cur_node.cluster_id == -1
        cur_node.cluster_id = new_cluster.cluster_id

    @staticmethod
    def _lcs(content1: Content, content2: Content) -> list[str]:
        lengths = [[0] * (len(content2) + 1) for _ in range(len(content1) + 1)]
        # row 0 and column 0 are initialized to 0 already
        for i in range(len(content1)):
            for j in range(len(content2)):
                if content1[i] == content2[j]:
                    lengths[i + 1][j + 1] = lengths[i][j] + 1
                else:
                    lengths[i + 1][j + 1] = max(lengths[i + 1][j], lengths[i][j + 1])

        # read the substring out from the matrix
        lcs = []
        len_of_seq1, len_of_seq2 = len(content1), len(content2)
        while len_of_seq1 != 0 and len_of_seq2 != 0:
            if (
                lengths[len_of_seq1][len_of_seq2]
                == lengths[len_of_seq1 - 1][len_of_seq2]
            ):
                len_of_seq1 -= 1
            elif (
                lengths[len_of_seq1][len_of_seq2]
                == lengths[len_of_seq1][len_of_seq2 - 1]
            ):
                len_of_seq2 -= 1
            else:
                assert content1[len_of_seq1 - 1] == content2[len_of_seq2 - 1]
                lcs.append(content1[len_of_seq1 - 1])
                len_of_seq1 -= 1
                len_of_seq2 -= 1
        lcs.reverse()
        return lcs

    def _lcs_match(self, content: Content) -> LogCluster | None:
        max_lcs_count = -1
        max_clust = None

        for cluster in self._id_to_cluster.values():
            # set_template = set(cluster.log_template)
            # if len(set_seq & set_template) < 0.5 * size_seq:
            #     continue
            lcs_length = len(SpellLogParser._lcs(content, cluster.content))
            if lcs_length > max_lcs_count or (
                lcs_length == max_lcs_count
                and len(cluster.content) < len(max_clust.content)
            ):
                max_lcs_count = lcs_length
                max_clust = cluster

        # LCS should be larger than tau * len(itself)
        if max_lcs_count >= self._sim_thr * len(content):
            return max_clust

        return None

    def _subseq_match(self, content: Content) -> LogCluster | None:
        content_length = len(content)
        for cluster in self._id_to_cluster.values():
            if len(cluster.content) < self._sim_thr * content_length:
                continue

            # 匹配子序列
            it = iter(content)
            if all(token in it for token in cluster.content):
                return cluster

        return None

    def _tree_subseq_match(self, content: Content) -> LogCluster | None:
        content_length = len(content)
        cur_node = self._root_node
        # 这个用来记录已经匹配的常量token数量，用来快速匹配的cluster
        # 这个值和也就是对应节点的深度-1
        matched_const_count = 0
        for token in content:
            # 如果当前节点挂载了一个cluster，且常量token数量超过阈值，就直接返回这个cluster
            if (
                cur_node.cluster_id != -1
                and matched_const_count >= self._sim_thr * content_length
            ):
                return self._id_to_cluster[cur_node.cluster_id]

            if token in cur_node.children_node:
                cur_node = cur_node.children_node[token]
                matched_const_count += 1

        return None

    def _add_content(self, content: Content) -> LogCluster:
        match_cluster = (
            self._tree_subseq_match(content)
            or self._subseq_match(content)
            or self._lcs_match(content)
        )

        # Match no existing log cluster
        if match_cluster is None:
            new_cluster = LogCluster(content)
            self._id_to_cluster[new_cluster.cluster_id] = new_cluster
            self._add_seq_to_prefix_tree(new_cluster)
            return new_cluster

        # Add the new log message to the existing cluster
        new_template_tokens = SpellLogParser._create_template(
            SpellLogParser._lcs(content, match_cluster.content),
            match_cluster.content,
        )
        if new_template_tokens != match_cluster.content:
            self._remove_seq_from_prefix_tree(match_cluster)
            match_cluster.content = new_template_tokens
            self._add_seq_to_prefix_tree(match_cluster)

        return match_cluster

    def add_log_message(self, content: str) -> LogCluster:
        # 预处理日志内容：掩码处理 + 分词
        mask_content = self._mask_log(content)
        content_tokens = self._split_log(mask_content)

        return self._add_content(content_tokens)

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
        contents = log_df["Tokens"].to_list()

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

        SpellLogParser._output_result(
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
    def name() -> str:
        return "Spell"

    @staticmethod
    def description() -> str:
        return "Spell 是一种基于前缀树和LCS算法的日志解析方法。"
