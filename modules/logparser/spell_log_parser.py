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

    __slots__ = ("log_cluster", "template_no", "children")

    def __init__(self, template_no=0):
        self.log_cluster = None
        self.template_no = template_no
        self.children = {}


@parser_register
class SpellLogParser(BaseLogParser):
    def __init__(
        self,
        log_id,
        log_file,
        log_format,
        regex,
        should_stop,
        progress_callback=None,
        keep_para=False,
        tau=0.5,
    ):
        """
        Attributes
        ----------
        tau : how much percentage of tokens matched to merge a log message
        """
        super().__init__(log_id, log_file, log_format, regex, should_stop, progress_callback, keep_para)
        self.tau = tau
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
    def add_seq_to_prefix_tree(root, new_cluster):
        parent = root
        seq = [w for w in new_cluster.log_template if w != "<*>"]

        for token in seq:
            if token in parent.children:
                parent.children[token].template_no += 1
            else:
                parent.children[token] = Node(1)
            parent = parent.children[token]

        if parent.log_cluster is None:
            parent.log_cluster = new_cluster

    @staticmethod
    def remove_seq_from_prefix_tree(root, cluster):
        parent = root
        seq = [w for w in cluster.log_template if w != "<*>"]

        for token in seq:
            if token in parent.children:
                matched_node = parent.children[token]
                if matched_node.template_no == 1:
                    del parent.children[token]
                    break
                else:
                    matched_node.template_no -= 1
                    parent = matched_node

    @staticmethod
    def lcs(seq1, seq2):
        lengths = [[0] * (len(seq2) + 1) for _ in range(len(seq1) + 1)]
        # row 0 and column 0 are initialized to 0 already
        for i in range(len(seq1)):
            for j in range(len(seq2)):
                if seq1[i] == seq2[j]:
                    lengths[i + 1][j + 1] = lengths[i][j] + 1
                else:
                    lengths[i + 1][j + 1] = max(lengths[i + 1][j], lengths[i][j + 1])

        # read the substring out from the matrix
        result = []
        len_of_seq1, len_of_seq2 = len(seq1), len(seq2)
        while len_of_seq1 != 0 and len_of_seq2 != 0:
            if lengths[len_of_seq1][len_of_seq2] == lengths[len_of_seq1 - 1][len_of_seq2]:
                len_of_seq1 -= 1
            elif lengths[len_of_seq1][len_of_seq2] == lengths[len_of_seq1][len_of_seq2 - 1]:
                len_of_seq2 -= 1
            else:
                assert seq1[len_of_seq1 - 1] == seq2[len_of_seq2 - 1]
                result.append(seq1[len_of_seq1 - 1])
                len_of_seq1 -= 1
                len_of_seq2 -= 1
        result.reverse()
        return result

    @staticmethod
    def get_template(lcs, seq):
        ret_val = []
        if not lcs:
            return ret_val

        lcs = lcs[::-1]
        for i, token in enumerate(seq, 1):
            if token == lcs[-1]:
                ret_val.append(token)
                lcs.pop()
            else:
                ret_val.append("<*>")
            if not lcs:
                break
        if i < len(seq):
            ret_val.append("<*>")
        return ret_val

    def lcs_match(self, log_clusters, seq):
        max_len = -1
        max_clust = None
        set_seq = set(seq)
        size_seq = len(seq)
        for cluster in log_clusters:
            set_template = set(cluster.log_template)
            if len(set_seq & set_template) < 0.5 * size_seq:
                continue
            lcs = self.lcs(seq, cluster.log_template)
            if len(lcs) > max_len or (len(lcs) == max_len and len(cluster.log_template) < len(max_clust.log_template)):
                max_len = len(lcs)
                max_clust = cluster

        # LCS should be larger than tau * len(itself)
        if max_len >= self.tau * size_seq:
            return max_clust

        return None

    @staticmethod
    def simple_loop_match(log_clusters, seq):
        for cluster in log_clusters:
            if len(cluster.log_template) < 0.5 * len(seq):
                continue
            # Check the template is a subsequence of seq (we use set checking as a proxy here for speedup since
            # incorrect-ordering bad cases rarely occur in logs)
            token_set = set(seq)
            if all(token in token_set or token == "<*>" for token in cluster.log_template):
                return cluster
        return None

    def prefix_tree_match(self, parent, seq, idx):
        length = len(seq)
        for i in range(idx, length):
            if seq[i] in parent.children:
                child = parent.children[seq[i]]
                if child.log_cluster is not None:
                    const_lm = [w for w in child.log_cluster.log_template if w != "<*>"]
                    if len(const_lm) >= self.tau * length:
                        return child.log_cluster
                else:
                    return self.prefix_tree_match(child, seq, i + 1)

        return None

    def parse(self) -> ParseResult:
        print(f"Parsing file: {self._log_file}")
        start_time = datetime.now()
        root_node = Node()
        log_clusters = []

        self._df_log = load_data(self._log_file, self._log_format, self._regex, self._should_stop)

        for idx, line in enumerate(self._df_log.iter_rows(named=True)):
            log_id = line["LineId"]
            log_message_tokens = line["Content"].split()
            const_log_tokens = [w for w in log_message_tokens if w != "<*>"]

            # Find an existing matched log cluster
            match_cluster = self.prefix_tree_match(root_node, const_log_tokens, 0)

            if match_cluster is None:
                match_cluster = self.simple_loop_match(log_clusters, const_log_tokens)

                if match_cluster is None:
                    match_cluster = self.lcs_match(log_clusters, log_message_tokens)

                    # Match no existing log cluster
                    if match_cluster is None:
                        new_cluster = LogCluster(log_message_tokens, [log_id])
                        log_clusters.append(new_cluster)
                        self.add_seq_to_prefix_tree(root_node, new_cluster)
                    # Add the new log message to the existing cluster
                    else:
                        new_template = self.get_template(
                            self.lcs(log_message_tokens, match_cluster.log_template),
                            match_cluster.log_template,
                        )
                        if " ".join(new_template) != " ".join(match_cluster.log_template):
                            self.remove_seq_from_prefix_tree(root_node, match_cluster)
                            match_cluster.log_template = new_template
                            self.add_seq_to_prefix_tree(root_node, match_cluster)
            if match_cluster:
                match_cluster.log_id_list.append(log_id)

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
        return "Spell"

    @staticmethod
    def description() -> str:
        return "Spell 是一种基于前缀树和LCS算法的日志解析方法。"
