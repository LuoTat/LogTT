# distutils: language=c++

# SPDX-License-Identifier: MIT
# This file implements the Drain algorithm for log parsing.
# Based on https://github.com/logpai/logparser/blob/master/logparser/Drain/Drain.py by LogPAI team


cdef object datetime
cdef object ParseResult
cdef object parser_register

from datetime import datetime

from cython.operator cimport dereference as deref
from libc.stdint cimport uint16_t
from libcpp.memory cimport make_shared, nullptr, shared_ptr
from libcpp.string cimport to_string
from libcpp.unordered_map cimport unordered_map

from .base_log_parser cimport BaseLogParser
from .parse_result import ParseResult
from .parser_factory import parser_register
from .utils cimport (
    Content,
    Token,
    load_data,
    mask_log_df,
    to_table,
    pair,
    split_log_df,
    string,
    vector,
)

cdef struct LogCluster:
    Content content

cdef struct Node:
    vector[shared_ptr[LogCluster]] clusters
    unordered_map[Token, shared_ptr[Node]] children_node

cdef string _get_template(shared_ptr[LogCluster] cluster):
    cdef string template
    cdef Content content = deref(cluster).content
    cdef size_t length = content.size()

    cdef size_t idx
    for idx in range(length - 1):
        template.append(content[idx])
        template.append(" ")
    template.append(content.back())

    return template

cdef _to_table(
    object log_df,
    vector[shared_ptr[LogCluster]]& cluster_results,
    string& structured_table_name,
    string& templates_table_name,
    bint keep_para,
):
    cdef vector[string] log_templates
    cdef size_t length = cluster_results.size()
    log_templates.resize(length)

    cdef size_t idx
    for idx in range(length):
        log_templates[idx] = _get_template(cluster_results[idx])

    to_table(
        log_df,
        log_templates,
        structured_table_name,
        templates_table_name,
        keep_para,
    )

cdef Content _create_template(
    Content& content1,
    Content& content2,
):
    cdef size_t length = content1.size()
    cdef const char * wildcard = "<#*#>"

    cdef Content template_content
    template_content.resize(length)

    cdef size_t idx
    cdef Token token1
    cdef Token token2
    for idx in range(length):
        token1 = content1[idx]
        token2 = content2[idx]
        if token1 == token2:
            template_content[idx] = token1
        else:
            template_content[idx] = wildcard

    return template_content

cdef void _add_to_prefix_tree(
    shared_ptr[LogCluster] cluster,
    shared_ptr[Node] root_node,
    uint16_t depth,
    uint16_t children,
):
    cdef const char * wird_card = "<#*#>"
    cdef size_t length = deref(cluster).content.size()
    cdef shared_ptr[Node] cur_node = root_node
    cdef Content new_content
    new_content.resize(length + 1)
    new_content[0] = to_string(length)

    cdef size_t idx
    for idx in range(length):
        new_content[idx + 1] = deref(cluster).content[idx]

    cdef uint16_t cur_node_depth = 1
    cdef Token token
    for token in new_content:
        # if at max depth or this is last token in template - add current log cluster to the leaf node
        if cur_node_depth == depth or cur_node_depth == length + 2:
            deref(cur_node).clusters.push_back(cluster)
            return

        # if token not matched in this layer of existing tree.
        if not deref(cur_node).children_node.contains(token):
            if deref(cur_node).children_node.size() + 1 < children:
                # 如果当前节点不是最后一个节点，就添加一个新的节点
                deref(cur_node).children_node.insert(
                    pair[Token, shared_ptr[Node]](token, make_shared[Node]())
                )
                cur_node = deref(cur_node).children_node[token]
            elif deref(cur_node).children_node.size() + 1 == children:
                # 如果当前节点是最后一个节点，就添加一个新的通配符节点
                # 注意由于 _parametrize_numeric_tokens 的存在，可能已经存在一个通配符节点了，所以需要先检查一下
                deref(cur_node).children_node.insert(
                    pair[Token, shared_ptr[Node]](wird_card, make_shared[Node]())
                )
                cur_node = deref(cur_node).children_node[wird_card]
            else:
                # 如果当前节点已满，就直接使用通配符节点
                cur_node = deref(cur_node).children_node[wird_card]

        # if the token is matched
        else:
            cur_node = deref(cur_node).children_node[token]

        cur_node_depth += 1

cdef pair[float, uint16_t] _get_distance(
    Content& content1,
    Content& content2,
    bint include_params,
):
    cdef size_t length = content1.size()

    # list are empty - full match
    if length == 0:
        return pair[float, uint16_t](<float>1.0, <uint16_t>0)

    cdef uint16_t sim_tokens = 0
    cdef uint16_t param_count = 0

    cdef size_t idx
    cdef Token token1
    cdef Token token2
    for idx in range(length):
        token1 = content1[idx]
        token2 = content2[idx]
        if token1 == "<#*#>":
            param_count += 1
            continue
        if token1 == token2:
            sim_tokens += 1

    if include_params:
        # 参数位也当匹配贡献
        sim_tokens += param_count

    return pair[float, uint16_t](<float> sim_tokens / length, param_count)

cdef shared_ptr[LogCluster] _fast_match(
    vector[shared_ptr[LogCluster]]& clusters,
    Content& content,
    bint include_params,
    float sim_thr,
):
    cdef float max_sim = -1.0
    cdef uint16_t max_param_count = 0
    cdef shared_ptr[LogCluster] max_cluster
    cdef size_t length = clusters.size()

    cdef size_t idx
    cdef shared_ptr[LogCluster] cluster
    cdef pair[float, uint16_t] result
    cdef float cur_sim
    cdef uint16_t param_count
    for idx in range(length):
        cluster = clusters[idx]
        result = _get_distance(
            deref(cluster).content,
            content,
            include_params,
        )
        cur_sim = result.first
        param_count = result.second
        if cur_sim > max_sim or (
            cur_sim == max_sim and param_count > max_param_count
        ):
            max_sim = cur_sim
            max_param_count = param_count
            max_cluster = cluster

        if max_sim > sim_thr:
            return max_cluster

    return shared_ptr[LogCluster]()

cdef shared_ptr[LogCluster] _tree_search(
    Content& content,
    bint include_params,
    shared_ptr[Node] root_node,
    uint16_t depth,
    float sim_thr,
):
    cdef size_t length = content.size()
    cdef shared_ptr[Node] cur_node = root_node

    cdef Content new_content
    new_content.resize(length + 1)
    new_content[0] = to_string(length)
    cdef size_t idx
    for idx in range(length):
        new_content[idx + 1] = content[idx]

    cdef uint16_t cur_node_depth = 1
    cdef Token token
    cdef unordered_map[Token, shared_ptr[Node]].iterator it
    for token in new_content:
        # at max depth or this is last token
        if cur_node_depth == depth or cur_node_depth == length + 2:
            # get best match among all clusters with same prefix, or None if no match is above sim_th
            return _fast_match(
                deref(cur_node).clusters, content, include_params, sim_thr
            )

        it = deref(cur_node).children_node.find(token)
        if it == deref(cur_node).children_node.end():
            it = deref(cur_node).children_node.find("<#*#>")
            if it == deref(cur_node).children_node.end():
                # no wildcard node exist
                return shared_ptr[LogCluster]()

        cur_node = deref(it).second
        cur_node_depth += 1

cdef shared_ptr[LogCluster] _add_content(
    Content& content,
    shared_ptr[Node] root_node,
    uint16_t depth,
    uint16_t children,
    float sim_thr,
):
    cdef shared_ptr[LogCluster] match_cluster = _tree_search(
        content,
        False,
        root_node,
        depth,
        sim_thr,
    )
    cdef Content new_content

    # Match no existing log cluster
    if match_cluster.get() == nullptr:
        match_cluster = make_shared[LogCluster](content)
        _add_to_prefix_tree(match_cluster, root_node, depth, children)
        return match_cluster

    # Add the new log message to the existing cluster
    new_content = _create_template(content, deref(match_cluster).content)
    if new_content != deref(match_cluster).content:
        deref(match_cluster).content = new_content

    return match_cluster

cdef class DrainLogParser(BaseLogParser):
    """Drain算法"""

    cdef uint16_t _depth
    cdef uint16_t _children
    cdef float _sim_thr
    cdef shared_ptr[Node] _root_node

    def __init__(
        self,
        string log_format,
        object masking=None,
        object delimiters=None,
        uint16_t depth=4,
        uint16_t children=100,
        float sim_thr=0.4,
    ):
        """
        Args:
            depth: Depth of prefix tree (minimum 3).
            children: Max children per tree node.
            sim_thr: Similarity threshold (0-1).
        """
        super().__init__(log_format, masking, delimiters)

        if depth < 3:
            raise ValueError("depth argument must be at least 3")

        self._depth = depth
        self._children = children
        self._sim_thr = sim_thr
        self._root_node = make_shared[Node]()

    # def add_log_message(self, log: str)-> LogCluster:
    #     # 预处理日志内容：掩码处理 + 分词
    #     mask_log = self._mask_log(log)
    #     content = self._split_log(mask_log)
    #
    #     return self._add_content(content)

    def parse(
        self,
        string log_file,
        string structured_table_name,
        string templates_table_name,
        bint keep_para = False,
        object should_stop = None,
        object progress_callback = None,
    ) -> ParseResult:
        print(f"Parsing file: {log_file}")
        cdef object start_time = datetime.now()

        cdef object log_df = load_data(log_file, self._log_format)
        # 预处理日志内容：掩码处理 + 分词
        log_df = mask_log_df(log_df, self._maskings)
        log_df = split_log_df(log_df, self._delimiters)
        log_df = log_df.drop("raw").collect()

        cdef object[::1] contents = log_df["Tokens"].to_numpy()
        cdef size_t length = contents.shape[0]

        # cdef vector[Content] contents = log_df["Tokens"].to_numpy()
        # cdef size_t length = contents.size()

        cdef vector[shared_ptr[LogCluster]] cluster_results
        cluster_results.resize(length)

        cdef size_t idx = 1
        cdef Content content
        cdef shared_ptr[LogCluster] match_cluster
        cdef float progress
        for content in contents:
            # if should_stop():
            #     raise InterruptedError

            match_cluster = _add_content(
                content,
                self._root_node,
                self._depth,
                self._children,
                self._sim_thr,
            )
            cluster_results[idx - 1] = match_cluster
            # if idx % 10000 == 0 or idx == log_df.height:
            #     progress = idx * 100.0 / length
            #     print(f"Processed {progress:.1f}% of log lines.")
            #     if progress_callback:
            #         progress_callback(int(progress))
            idx += 1

        _to_table(
            log_df,
            cluster_results,
            structured_table_name,
            templates_table_name,
            keep_para,
        )

        print(f"Parsing done. [Time taken: {datetime.now() - start_time}]")
        return ParseResult(
            log_file,
            length,
            structured_table_name,
            templates_table_name,
        )

    @staticmethod
    def name() -> str:
        return "Drain"

    @staticmethod
    def description() -> str:
        return "Drain 是一种基于树结构的高效日志模板提取算法。"

parser_register(DrainLogParser)
