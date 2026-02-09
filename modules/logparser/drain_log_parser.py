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


class Logcluster:
    def __init__(self, log_template="", log_idl=None):
        self.logTemplate = log_template
        if log_idl is None:
            log_idl = list()
        self.logIDL = log_idl


class Node:
    def __init__(self, child_d=None):
        if child_d is None:
            child_d = dict()
        self.childD = child_d


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
            keep_para : whether to keep parameter list in structured log file
        """
        super().__init__(log_id, log_file, log_format, regex, should_stop, progress_callback, keep_para)
        self._depth = depth - 2
        self._st = st
        self._max_child = max_child
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
    def _get_template(seq1, seq2):
        assert len(seq1) == len(seq2)
        return [w1 if w1 == w2 else "<*>" for w1, w2 in zip(seq1, seq2)]

    @staticmethod
    def _has_numbers(s):
        return any(char.isdigit() for char in s)

    def _add_seq_to_prefix_tree(self, rn, log_clust):
        seq_len = len(log_clust.logTemplate)
        if seq_len not in rn.childD:
            first_layer_node = Node()
            rn.childD[seq_len] = first_layer_node
        else:
            first_layer_node = rn.childD[seq_len]

        parentn = first_layer_node

        current_depth = 1
        for token in log_clust.logTemplate:
            # Add current log cluster to the leaf node
            if current_depth >= self._depth or current_depth > seq_len:
                if len(parentn.childD) == 0:
                    parentn.childD = [log_clust]
                else:
                    parentn.childD.append(log_clust)
                break

            # If token not matched in this layer of existing tree.
            if token not in parentn.childD:
                if not self._has_numbers(token):
                    if "<*>" in parentn.childD:
                        if len(parentn.childD) < self._max_child:
                            new_node = Node()
                            parentn.childD[token] = new_node
                            parentn = new_node
                        else:
                            parentn = parentn.childD["<*>"]
                    else:
                        if len(parentn.childD) + 1 < self._max_child:
                            new_node = Node()
                            parentn.childD[token] = new_node
                            parentn = new_node
                        elif len(parentn.childD) + 1 == self._max_child:
                            new_node = Node()
                            parentn.childD["<*>"] = new_node
                            parentn = new_node
                        else:
                            parentn = parentn.childD["<*>"]

                else:
                    if "<*>" not in parentn.childD:
                        new_node = Node()
                        parentn.childD["<*>"] = new_node
                        parentn = new_node
                    else:
                        parentn = parentn.childD["<*>"]

            # If the token is matched
            else:
                parentn = parentn.childD[token]

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

        ret_val = float(sim_tokens) / len(seq1)

        return ret_val, num_of_par

    def _fast_match(self, log_clust_l, seq):
        ret_log_clust = None

        max_sim = -1
        max_num_of_para = -1
        max_clust = None

        for log_clust in log_clust_l:
            cur_sim, cur_num_of_para = self._seq_dist(log_clust.logTemplate, seq)
            if cur_sim > max_sim or (cur_sim == max_sim and cur_num_of_para > max_num_of_para):
                max_sim = cur_sim
                max_num_of_para = cur_num_of_para
                max_clust = log_clust

        if max_sim >= self._st:
            ret_log_clust = max_clust

        return ret_log_clust

    def _tree_search(self, rn, seq):
        ret_log_clust = None

        seq_len = len(seq)
        if seq_len not in rn.childD:
            return ret_log_clust

        parentn = rn.childD[seq_len]

        current_depth = 1
        for token in seq:
            if current_depth >= self._depth or current_depth > seq_len:
                break

            if token in parentn.childD:
                parentn = parentn.childD[token]
            elif "<*>" in parentn.childD:
                parentn = parentn.childD["<*>"]
            else:
                return ret_log_clust
            current_depth += 1

        log_clust_l = parentn.childD

        ret_log_clust = self._fast_match(log_clust_l, seq)

        return ret_log_clust

    def parse(self) -> ParseResult:
        print(f"Parsing file: {self._log_file}")
        start_time = datetime.now()
        rootNode = Node()
        log_clust_l = list()

        self._df_log = load_data(self._log_file, self._log_format, self._regex, self._should_stop)

        for idx, line in enumerate(self._df_log.iter_rows(named=True)):
            if self._should_stop():
                raise InterruptedError

            log_id = line["LineId"]
            log_message_l = line["Content"].strip().split()
            match_cluster = self._tree_search(rootNode, log_message_l)

            # Match no existing log cluster
            if match_cluster is None:
                new_cluster = Logcluster(log_template=log_message_l, log_idl=[log_id])
                log_clust_l.append(new_cluster)
                self._add_seq_to_prefix_tree(rootNode, new_cluster)

            # Add the new log message to the existing cluster
            else:
                new_template = self._get_template(log_message_l, match_cluster.logTemplate)
                match_cluster.logIDL.append(log_id)
                if " ".join(new_template) != " ".join(match_cluster.logTemplate):
                    match_cluster.logTemplate = new_template

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
        return "Drain"

    @staticmethod
    def description() -> str:
        return "Drain 是一种基于树结构的高效日志模板提取算法"
