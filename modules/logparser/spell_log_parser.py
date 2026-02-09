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

import regex as re

from .base_log_parser import BaseLogParser
from .parse_result import ParseResult
from .parser_factory import parser_register
from .utils import load_data, output_result


class LogCluster:
    """Class object to store a log group with the same template"""

    def __init__(self, log_template="", log_idl=None):
        self.logTemplate = log_template
        self.logIDL = log_idl if log_idl is not None else list()


class Node:
    """A node in prefix tree data structure"""

    def __init__(self, template_no=0):
        self.logClust = None
        self.templateNo = template_no
        self.childD = dict()


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

    def _output_result(self, log_clust_l):
        log_templates = [0] * self._df_log.height
        for log_clust in log_clust_l:
            template_str = " ".join(log_clust.logTemplate)
            for log_id in log_clust.logIDL:
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
    def add_seq_to_prefix_tree(rootn, new_cluster):
        parentn = rootn
        seq = new_cluster.logTemplate
        seq = [w for w in seq if w != "<*>"]

        for i in range(len(seq)):
            token_in_seq = seq[i]
            # Match
            if token_in_seq in parentn.childD:
                parentn.childD[token_in_seq].templateNo += 1
            # Do not Match
            else:
                parentn.childD[token_in_seq] = Node(1)
            parentn = parentn.childD[token_in_seq]

        if parentn.logClust is None:
            parentn.logClust = new_cluster

    @staticmethod
    def remove_seq_from_prefix_tree(rootn, new_cluster):
        parentn = rootn
        seq = new_cluster.logTemplate
        seq = [w for w in seq if w != "<*>"]

        for tokenInSeq in seq:
            if tokenInSeq in parentn.childD:
                matched_node = parentn.childD[tokenInSeq]
                if matched_node.templateNo == 1:
                    del parentn.childD[tokenInSeq]
                    break
                else:
                    matched_node.templateNo -= 1
                    parentn = matched_node

    @staticmethod
    def lcs(seq1, seq2):
        lengths = [[0 for j in range(len(seq2) + 1)] for i in range(len(seq1) + 1)]
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
                result.insert(0, seq1[len_of_seq1 - 1])
                len_of_seq1 -= 1
                len_of_seq2 -= 1
        return result

    @staticmethod
    def get_template(lcs, seq):
        ret_val = []
        if not lcs:
            return ret_val

        lcs = lcs[::-1]
        i = 0
        for token in seq:
            i += 1
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

    def lcs_match(self, log_clust_l, seq):
        ret_log_clust = None
        max_len = -1
        max_clust = None
        set_seq = set(seq)
        size_seq = len(seq)
        for logClust in log_clust_l:
            set_template = set(logClust.logTemplate)
            if len(set_seq & set_template) < 0.5 * size_seq:
                continue
            lcs = self.lcs(seq, logClust.logTemplate)
            if len(lcs) > max_len or (len(lcs) == max_len and len(logClust.logTemplate) < len(max_clust.logTemplate)):
                max_len = len(lcs)
                max_clust = logClust

        # LCS should be large then tau * len(itself)
        if float(max_len) >= self.tau * size_seq:
            ret_log_clust = max_clust

        return ret_log_clust

    @staticmethod
    def simple_loop_match(log_clust_l, seq):
        for logClust in log_clust_l:
            if float(len(logClust.logTemplate)) < 0.5 * len(seq):
                continue
            # Check the template is a subsequence of seq (we use set checking as a proxy here for speedup since
            # incorrect-ordering bad cases rarely occur in logs)
            token_set = set(seq)
            if all(token in token_set or token == "<*>" for token in logClust.logTemplate):
                return logClust
        return None

    def prefix_tree_match(self, parentn, seq, idx):
        ret_log_clust = None
        length = len(seq)
        for i in range(idx, length):
            if seq[i] in parentn.childD:
                childn = parentn.childD[seq[i]]
                if childn.logClust is not None:
                    const_lm = [w for w in childn.logClust.logTemplate if w != "<*>"]
                    if float(len(const_lm)) >= self.tau * length:
                        return childn.logClust
                else:
                    return self.prefix_tree_match(childn, seq, i + 1)

        return ret_log_clust

    def parse(self) -> ParseResult:
        print(f"Parsing file: {self._log_file}")
        start_time = datetime.now()
        root_node = Node()
        log_clust_l = list()

        self._df_log = load_data(self._log_file, self._log_format, self._regex, self._should_stop)

        for idx, line in enumerate(self._df_log.iter_rows(named=True)):
            log_id = line["LineId"]
            logmessage_l = list(
                filter(
                    lambda x: x != "",
                    re.split(r"[\s]", line["Content"]),
                    # line["Content"],
                )
            )
            const_log_mess_l = [w for w in logmessage_l if w != "<*>"]

            # Find an existing matched log cluster
            match_cluster = self.prefix_tree_match(root_node, const_log_mess_l, 0)

            if match_cluster is None:
                match_cluster = self.simple_loop_match(log_clust_l, const_log_mess_l)

                if match_cluster is None:
                    match_cluster = self.lcs_match(log_clust_l, logmessage_l)

                    # Match no existing log cluster
                    if match_cluster is None:
                        new_cluster = LogCluster(logmessage_l, [log_id])
                        log_clust_l.append(new_cluster)
                        self.add_seq_to_prefix_tree(root_node, new_cluster)
                    # Add the new log message to the existing cluster
                    else:
                        new_template = self.get_template(
                            self.lcs(logmessage_l, match_cluster.logTemplate),
                            match_cluster.logTemplate,
                        )
                        if " ".join(new_template) != " ".join(match_cluster.logTemplate):
                            self.remove_seq_from_prefix_tree(root_node, match_cluster)
                            match_cluster.logTemplate = new_template
                            self.add_seq_to_prefix_tree(root_node, match_cluster)
            if match_cluster:
                match_cluster.logIDL.append(log_id)

            if idx % 10000 == 0 or idx == self._df_log.height - 1:
                progress = idx * 100.0 / self._df_log.height
                print(f"Processed {progress:.1f}% of log lines.")
                if self._progress_callback:
                    self._progress_callback(int(progress))

        self._output_result(log_clust_l)

        print(f"Parsing done. [Time taken: {datetime.now() - start_time}]")
        return ParseResult(self._log_file, self._df_log.height, self._log_structured_file, self._log_templates_file)

    @staticmethod
    def name() -> str:
        return "Spell"

    @staticmethod
    def description() -> str:
        return "Spell 是一种基于前缀树和LCS算法的日志解析方法。"
