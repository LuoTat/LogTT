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
    def __init__(self, child_d=None, depth=0, digit_or_token=None):
        if child_d is None:
            child_d = dict()
        self.childD = child_d
        self.depth = depth
        self.digitOrtoken = digit_or_token


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
        depth=4,
        st=0.4,
        max_child=100,
        keep_para=False,
    ):
        """
        Attributes
        ----------
            depth : depth of all leaf nodes
            st : similarity threshold
            max_child : max number of children of an internal node
            keep_para : whether to keep parameter list in structured log file
        """
        super().__init__(log_id, log_file, log_format, regex, should_stop, progress_callback)
        self._depth = depth - 2
        self._st = st
        self._max_child = max_child
        self._keep_para = keep_para
        self._df_log = None

    def _output_result(self, log_clust_l):
        log_templates = [0] * self._df_log.height
        for logClust in log_clust_l:
            template_str = " ".join(logClust.logTemplate)
            for logID in logClust.logIDL:
                log_templates[logID - 1] = template_str

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
        retVal = list()

        i = 0
        for word in seq1:
            if word == seq2[i]:
                retVal.append(word)
            else:
                retVal.append("<*>")

            i += 1

        return retVal

    @staticmethod
    def _has_numbers(s):
        return any(char.isdigit() for char in s)

    def _add_seq_to_prefix_tree(self, rn, log_clust):
        seqLen = len(log_clust.logTemplate)
        if seqLen not in rn.childD:
            firtLayerNode = Node(depth=1, digit_or_token=seqLen)
            rn.childD[seqLen] = firtLayerNode
        else:
            firtLayerNode = rn.childD[seqLen]

        parentn = firtLayerNode

        currentDepth = 1
        for token in log_clust.logTemplate:
            # Add current log cluster to the leaf node
            if currentDepth >= self._depth or currentDepth > seqLen:
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
                            newNode = Node(depth=currentDepth + 1, digit_or_token=token)
                            parentn.childD[token] = newNode
                            parentn = newNode
                        else:
                            parentn = parentn.childD["<*>"]
                    else:
                        if len(parentn.childD) + 1 < self._max_child:
                            newNode = Node(depth=currentDepth + 1, digit_or_token=token)
                            parentn.childD[token] = newNode
                            parentn = newNode
                        elif len(parentn.childD) + 1 == self._max_child:
                            newNode = Node(depth=currentDepth + 1, digit_or_token="<*>")
                            parentn.childD["<*>"] = newNode
                            parentn = newNode
                        else:
                            parentn = parentn.childD["<*>"]

                else:
                    if "<*>" not in parentn.childD:
                        newNode = Node(depth=currentDepth + 1, digit_or_token="<*>")
                        parentn.childD["<*>"] = newNode
                        parentn = newNode
                    else:
                        parentn = parentn.childD["<*>"]

            # If the token is matched
            else:
                parentn = parentn.childD[token]

            currentDepth += 1

    @staticmethod
    def _seq_dist(seq1, seq2):
        # seq1 is template
        assert len(seq1) == len(seq2)
        simTokens = 0
        numOfPar = 0

        for token1, token2 in zip(seq1, seq2):
            if token1 == "<*>":
                numOfPar += 1
                continue
            if token1 == token2:
                simTokens += 1

        retVal = float(simTokens) / len(seq1)

        return retVal, numOfPar

    def _fast_match(self, log_clust_l, seq):
        retLogClust = None

        maxSim = -1
        maxNumOfPara = -1
        maxClust = None

        for logClust in log_clust_l:
            curSim, curNumOfPara = self._seq_dist(logClust.logTemplate, seq)
            if curSim > maxSim or (curSim == maxSim and curNumOfPara > maxNumOfPara):
                maxSim = curSim
                maxNumOfPara = curNumOfPara
                maxClust = logClust

        if maxSim >= self._st:
            retLogClust = maxClust

        return retLogClust

    def _tree_search(self, rn, seq):
        retLogClust = None

        seqLen = len(seq)
        if seqLen not in rn.childD:
            return retLogClust

        parentn = rn.childD[seqLen]

        currentDepth = 1
        for token in seq:
            if currentDepth >= self._depth or currentDepth > seqLen:
                break

            if token in parentn.childD:
                parentn = parentn.childD[token]
            elif "<*>" in parentn.childD:
                parentn = parentn.childD["<*>"]
            else:
                return retLogClust
            currentDepth += 1

        logClustL = parentn.childD

        retLogClust = self._fast_match(logClustL, seq)

        return retLogClust

    def parse(self) -> ParseResult:
        print(f"Parsing file: {self._log_file}")
        start_time = datetime.now()
        rootNode = Node()
        logCluL = list()

        self._df_log = load_data(self._log_file, self._log_format, self._regex, self._should_stop)

        for idx, line in enumerate(self._df_log.iter_rows(named=True)):
            if self._should_stop():
                raise InterruptedError

            logID = line["LineId"]
            logmessageL = line["Content"].strip().split()
            matchCluster = self._tree_search(rootNode, logmessageL)

            # Match no existing log cluster
            if matchCluster is None:
                newCluster = Logcluster(log_template=logmessageL, log_idl=[logID])
                logCluL.append(newCluster)
                self._add_seq_to_prefix_tree(rootNode, newCluster)

            # Add the new log message to the existing cluster
            else:
                newTemplate = self._get_template(logmessageL, matchCluster.logTemplate)
                matchCluster.logIDL.append(logID)
                if " ".join(newTemplate) != " ".join(matchCluster.logTemplate):
                    matchCluster.logTemplate = newTemplate

            if idx % 10000 == 0 or idx == self._df_log.height - 1:
                progress = idx * 100.0 / self._df_log.height
                print(f"Processed {progress:.1f}% of log lines.")
                if self._progress_callback:
                    self._progress_callback(int(progress))

        self._output_result(logCluL)

        print(f"Parsing done. [Time taken: {datetime.now() - start_time}]")
        return ParseResult(self._log_file, self._df_log.height, self._log_structured_file, self._log_templates_file)

    @staticmethod
    def name() -> str:
        return "Drain"

    @staticmethod
    def description() -> str:
        return "Drain 是一种基于树结构的高效日志模板提取算法"
