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

from .base_log_parser import BaseLogParser
from .parse_result import ParseResult
from .parser_factory import parser_register
from .utils import load_data, output_result


class LogCluster:
    """Class object to store a log group with the same template"""

    __slots__ = ("log_template", "log_id_list")

    def __init__(self, log_template="", log_id_list=None):
        self.log_template = log_template
        self.log_id_list = log_id_list if log_id_list is not None else []


class Node:
    """A node in prefix tree data structure"""

    __slots__ = ("children",)

    def __init__(self, children=None):
        self.children = children if children is not None else {}


@parser_register
class DrainLogParser(BaseLogParser):
    def __init__(
        self,
        log_id,
        log_file,
        log_format,
        regex,
        should_stop,
        progress_callback=None,
        keep_para=False,
        depth=4,
        st=0.4,
        max_child=100,
    ):
        """
        Attributes
        ----------
            depth : depth of all leaf nodes
            st : similarity threshold
            max_child : max number of children of an internal node
        """
        super().__init__(log_id, log_file, log_format, regex, should_stop, progress_callback, keep_para)
        self._depth = depth - 2
        self._st = st
        self._max_child = max_child
        self._df_log = None

    def _output_result(self, log_clusters):
        log_templates = [""] * self._df_log.height
        for cluster in log_clusters:
            template_str = " ".join(cluster.log_template)
            for log_id in cluster.log_id_list:
                log_templates[log_id - 1] = template_str

        output_result(
            self._df_log,
            log_templates,
            self._output_dir,
            self._log_structured_file,
            self._log_templates_file,
            self._keep_para,
        )

    @staticmethod
    def _get_template(seq1, seq2):
        assert len(seq1) == len(seq2)
        return [w1 if w1 == w2 else "<*>" for w1, w2 in zip(seq1, seq2)]

    @staticmethod
    def _has_numbers(s):
        return any(char.isdigit() for char in s)

    def _add_seq_to_prefix_tree(self, root, cluster):
        seq_len = len(cluster.log_template)
        if seq_len not in root.children:
            root.children[seq_len] = Node()

        parent = root.children[seq_len]

        current_depth = 1
        for token in cluster.log_template:
            # Add current log cluster to the leaf node
            if current_depth >= self._depth or current_depth > seq_len:
                if isinstance(parent.children, dict):
                    parent.children = [cluster]
                else:
                    parent.children.append(cluster)
                break

            # If token not matched in this layer of existing tree.
            if token not in parent.children:
                if not self._has_numbers(token):
                    if "<*>" in parent.children:
                        if len(parent.children) < self._max_child:
                            new_node = Node()
                            parent.children[token] = new_node
                            parent = new_node
                        else:
                            parent = parent.children["<*>"]
                    else:
                        if len(parent.children) + 1 < self._max_child:
                            new_node = Node()
                            parent.children[token] = new_node
                            parent = new_node
                        elif len(parent.children) + 1 == self._max_child:
                            new_node = Node()
                            parent.children["<*>"] = new_node
                            parent = new_node
                        else:
                            parent = parent.children["<*>"]
                else:
                    if "<*>" not in parent.children:
                        new_node = Node()
                        parent.children["<*>"] = new_node
                        parent = new_node
                    else:
                        parent = parent.children["<*>"]

            # If the token is matched
            else:
                parent = parent.children[token]

            current_depth += 1

    @staticmethod
    def _seq_dist(seq1, seq2):
        # seq1 is template
        assert len(seq1) == len(seq2)
        sim_tokens = 0
        num_of_par = 0

        for token1, token2 in zip(seq1, seq2):
            if token1 == "<*>":
                num_of_par += 1
                continue
            if token1 == token2:
                sim_tokens += 1

        return sim_tokens / len(seq1), num_of_par

    def _fast_match(self, log_clusters, seq):
        max_sim = -1
        max_num_of_para = -1
        max_clust = None

        for cluster in log_clusters:
            cur_sim, cur_num_of_para = self._seq_dist(cluster.log_template, seq)
            if cur_sim > max_sim or (cur_sim == max_sim and cur_num_of_para > max_num_of_para):
                max_sim = cur_sim
                max_num_of_para = cur_num_of_para
                max_clust = cluster

        if max_sim >= self._st:
            return max_clust

        return None

    def _tree_search(self, root, seq):
        seq_len = len(seq)
        if seq_len not in root.children:
            return None

        parent = root.children[seq_len]

        current_depth = 1
        for token in seq:
            if current_depth >= self._depth or current_depth > seq_len:
                break

            if token in parent.children:
                parent = parent.children[token]
            elif "<*>" in parent.children:
                parent = parent.children["<*>"]
            else:
                return None
            current_depth += 1

        return self._fast_match(parent.children, seq)

    def parse(self) -> ParseResult:
        print(f"Parsing file: {self._log_file}")
        start_time = datetime.now()
        root_node = Node()
        log_clusters = []

        self._df_log = load_data(self._log_file, self._log_format, self._regex, self._should_stop)

        for idx, line in enumerate(self._df_log.iter_rows(named=True)):
            if self._should_stop():
                raise InterruptedError

            log_id = line["LineId"]
            log_message_tokens = line["Content"].strip().split()
            match_cluster = self._tree_search(root_node, log_message_tokens)

            # Match no existing log cluster
            if match_cluster is None:
                new_cluster = LogCluster(log_template=log_message_tokens, log_id_list=[log_id])
                log_clusters.append(new_cluster)
                self._add_seq_to_prefix_tree(root_node, new_cluster)

            # Add the new log message to the existing cluster
            else:
                new_template = self._get_template(log_message_tokens, match_cluster.log_template)
                match_cluster.log_id_list.append(log_id)
                if " ".join(new_template) != " ".join(match_cluster.log_template):
                    match_cluster.log_template = new_template

            if idx % 10000 == 0 or idx == self._df_log.height - 1:
                progress = idx * 100.0 / self._df_log.height
                print(f"Processed {progress:.1f}% of log lines.")
                if self._progress_callback:
                    self._progress_callback(int(progress))

        self._output_result(log_clusters)

        print(f"Parsing done. [Time taken: {datetime.now() - start_time}]")
        return ParseResult(self._log_file, self._df_log.height, self._log_structured_file, self._log_templates_file)

    @staticmethod
    def name() -> str:
        return "Drain"

    @staticmethod
    def description() -> str:
        return "Drain 是一种基于树结构的高效日志模板提取算法"
