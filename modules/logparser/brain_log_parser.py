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
from datetime import datetime

import regex as re

from .base_log_parser import BaseLogParser
from .parse_result import ParseResult
from .parser_factory import parser_register
from .utils import load_data, output_result


class TupleTree:
    """
    TupleTree(sorted_tuple_vector[key], word_combinations[key], word_combinations_reverse[key], tuple_vector[key], group_len[key])

    """

    def __init__(
        self,
        sorted_tuple_vector,
        word_combinations,
        word_combinations_reverse,
        tuple_vector,
        group_len,
    ):
        self._sorted_tuple_vector = sorted_tuple_vector
        self._word_combinations = word_combinations
        self._word_combinations_reverse = word_combinations_reverse
        self._tuple_vector = tuple_vector
        self._group_len = group_len

    def find_root(self, threshold_per):
        root_set_detail_ID = dict()
        root_set_detail = dict()
        root_set = dict()
        for idx, fc in enumerate(self._word_combinations):
            threshold = (max(fc, key=lambda tup: tup[0])[0]) * threshold_per
            m = 0
            candidate = fc[0]
            for fc_w in fc:
                if fc_w[0] >= threshold:
                    root_set_detail_ID.setdefault(fc_w, list()).append(self._sorted_tuple_vector[idx])
                    root_set.setdefault(fc_w, list()).append(self._word_combinations_reverse[idx])
                    root_set_detail.setdefault(fc_w, list()).append(self._tuple_vector[idx])
                    break
                if fc_w[0] >= m:
                    candidate = fc_w
                    m = fc_w[0]
                if fc_w == fc[-1]:
                    root_set_detail_ID.setdefault(candidate, list()).append(self._sorted_tuple_vector[idx])
                    root_set.setdefault(candidate, list()).append(self._word_combinations_reverse[idx])
                    root_set_detail.setdefault(fc_w, list()).append(self._tuple_vector[idx])
        return root_set_detail_ID, root_set, root_set_detail

    @staticmethod
    def up_split(root_set_detail, root_set):
        for key in root_set.keys():
            tree_node = root_set[key]
            father_count = list()
            for node in tree_node:
                pos = node.index(key)
                for i in range(pos):
                    father_count.append(node[i])
            father_set = set(father_count)
            for father in father_set:
                if father_count.count(father) == key[0]:
                    continue
                else:
                    for i in range(len(root_set_detail[key])):
                        for k in range(len(root_set_detail[key][i])):
                            if father[0] == root_set_detail[key][i][k]:
                                root_set_detail[key][i][k] = (
                                    root_set_detail[key][i][k][0],
                                    "<*>",
                                    root_set_detail[key][i][k][2],
                                )
                    break
        return root_set_detail

    @staticmethod
    def down_split(root_set_detail_id, threshold, root_set_detail):
        for key in root_set_detail_id.keys():
            detail_order = root_set_detail[key]
            m = list()
            child = dict()
            variable = set()
            m_count = 0
            first_sentence = detail_order[0]
            for det in first_sentence:
                if det[0] != key[0]:
                    m.append(m_count)
                m_count += 1
            for i in m:
                for node in detail_order:
                    if i < len(node):
                        child.setdefault(i, list()).append(node[i][1])
            for i in m:
                result = set(child[i])
                if len(result) >= threshold:
                    variable = variable.union(result)
            for i, row in enumerate(root_set_detail_id[key]):
                for j, item in enumerate(row):
                    if isinstance(item, tuple) and item[1] in variable:
                        root_set_detail_id[key][i][j] = (item[0], "<*>", item[2])
        return root_set_detail_id


@parser_register
class BrainLogParser(BaseLogParser):
    def __init__(
        self,
        log_id,
        log_file,
        log_format,
        regex,
        should_stop,
        progress_callback=None,
        keep_para=False,
        threshold=2,
        delimeter=None,
    ):
        """
        Attributes
        ----------
            threshold : similarity threshold
            delimeter : list of delimiters to split log messages
        """
        super().__init__(log_id, log_file, log_format, regex, should_stop, progress_callback, keep_para)
        self._threshold = threshold
        self._delimeter = delimeter if delimeter is not None else list()
        self._df_log = None

    def _output_result(self, template_set):
        log_templates = [0] * self._df_log.height
        for k1, indices in template_set.items():
            for i in indices:
                log_templates[i] = " ".join(k1)

        output_result(
            self._df_log,
            log_templates,
            self._output_dir,
            self._log_structured_file,
            self._log_templates_file,
            self._keep_para,
        )

    @staticmethod
    def _exclude_digits(string):
        """
        exclude the digits-domain words from partial constant
        """
        pattern = r"\d"
        digits = re.findall(pattern, string)
        if len(digits) == 0:
            return False
        return len(digits) / len(string) >= 0.3

    def _extract_templates(self, parse_result):
        template_set = dict()
        for key in parse_result.keys():
            for pr in parse_result[key]:
                sorted_pr = sorted(pr, key=lambda tup: tup[2])
                template = list()
                for item in sorted_pr[1:]:
                    if self._should_stop():
                        raise InterruptedError

                    word = item[1]
                    if "<*>" in word or BrainLogParser._exclude_digits(word):
                        template.append("<*>")
                    else:
                        template.append(word)
                template = tuple(template)
                template_set.setdefault(template, list()).append(pr[-1][0])
        return template_set

    def _tuple_generate(self, group_len, tuple_vector, frequency_vector):
        """
        Generate word combinations
        Output:
            sorted_tuple_vector: each tuple in the tuple_vector will be sorted according their frequencies.
            word_combinations:  words in the log with the same frequency will be grouped as word combinations and will
                                be arranged in descending order according to their frequencies.
            word_combinations_reverse:  The word combinations in the log will be arranged in ascending order according
                                        to their frequencies.

        """
        sorted_tuple_vector = dict()
        word_combinations = dict()
        word_combinations_reverse = dict()
        for key in group_len.keys():
            for fre in tuple_vector[key]:
                if self._should_stop():
                    raise InterruptedError

                sorted_fre_reverse = sorted(fre, key=lambda tup: tup[0], reverse=True)
                sorted_tuple_vector.setdefault(key, list()).append(sorted_fre_reverse)
            for fc in frequency_vector[key]:
                if self._should_stop():
                    raise InterruptedError

                number = Counter(fc)
                result = number.most_common()
                sorted_result = sorted(result, key=lambda tup: tup[1], reverse=True)
                sorted_fre = sorted(result, key=lambda tup: tup[0], reverse=True)
                word_combinations.setdefault(key, list()).append(sorted_result)
                word_combinations_reverse.setdefault(key, list()).append(sorted_fre)
        return sorted_tuple_vector, word_combinations, word_combinations_reverse

    def _get_frequecy_vector(self, contents, delimiter):
        """
        Counting each word's frequency in the dataset and convert each log into frequency vector
        Output:
            wordlist: log groups based on length
            tuple_vector: the word in the log will be converted into a tuple (word_frequency, word_character, word_position)
            frequency_vector: the word in the log will be converted into its frequency

        """
        group_len = dict()
        word_set = dict()
        for idx, c in enumerate(contents):  # using delimiters to get split words
            if self._should_stop():
                raise InterruptedError

            for de in delimiter:
                c = re.sub(de, "", c)
            c = re.sub(",", ", ", c)
            c = re.sub(" +", " ", c).split(" ")
            c.insert(0, str(idx))
            for pos, token in enumerate(c):
                word_set.setdefault(str(pos), list()).append(token)
            group_len.setdefault(len(c), list()).append(c)  # first grouping: logs with the same length
        tuple_vector = dict()
        frequency_vector = dict()
        max_len = max(group_len.keys())  # the biggest length of the log in this dataset
        fre_set = dict()  # saving each word's frequency
        for i in range(max_len):
            for word in word_set[str(i)]:  # counting each word's frequency
                if self._should_stop():
                    raise InterruptedError

                word = str(i) + " " + word
                fre_set[word] = fre_set.get(word, 0) + 1
        for key in group_len.keys():  # using fre_set to generate frequency vector for the log
            for c in group_len[key]:  # in each log group with the same length
                fre = list()
                fre_common = list()
                for position, word_character in enumerate(c[1:]):
                    if self._should_stop():
                        raise InterruptedError

                    frequency_word = fre_set[str(position + 1) + " " + word_character]
                    fre.append((frequency_word, word_character, position))
                    fre_common.append(frequency_word)
                tuple_vector.setdefault(key, list()).append(fre)
                frequency_vector.setdefault(key, list()).append(fre_common)
        return group_len, tuple_vector, frequency_vector

    def parse(self) -> ParseResult:
        print(f"Parsing file: {self._log_file}")
        start_time = datetime.now()

        self._df_log = load_data(self._log_file, self._log_format, self._regex, self._should_stop)

        contents = self._df_log["Content"].to_list()

        group_len, tuple_vector, frequency_vector = self._get_frequecy_vector(contents, self._delimeter)

        (
            sorted_tuple_vector,
            word_combinations,
            word_combinations_reverse,
        ) = self._tuple_generate(group_len, tuple_vector, frequency_vector)

        template_set = dict()
        for key in group_len.keys():
            if self._should_stop():
                raise InterruptedError

            Tree = TupleTree(
                sorted_tuple_vector[key],
                word_combinations[key],
                word_combinations_reverse[key],
                tuple_vector[key],
                group_len[key],
            )
            root_set_detail_ID, root_set, root_set_detail = Tree.find_root(0)

            root_set_detail_ID = Tree.up_split(root_set_detail_ID, root_set)
            parse_result = Tree.down_split(root_set_detail_ID, self._threshold, root_set_detail)
            template_set.update(BrainLogParser._extract_templates(parse_result))

        self._output_result(template_set)

        print(f"Parsing done. [Time taken: {datetime.now() - start_time}]")
        return ParseResult(self._log_file, self._df_log.height, self._log_structured_file, self._log_templates_file)

    @staticmethod
    def name() -> str:
        return "Brain"

    @staticmethod
    def description() -> str:
        return "Brain 是一种基于双向树结构的高效日志模板提取算法"
