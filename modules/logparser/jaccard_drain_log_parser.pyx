# distutils: language=c++

# SPDX-License-Identifier: MIT
# This file implements the Drain algorithm for log parsing.
# Based on https://github.com/logpai/logparser/blob/master/logparser/Drain/Drain.py by LogPAI team


cdef object ParseResult
cdef object parser_register

from cython.operator cimport dereference as deref
from libc.stdint cimport uint16_t
from libcpp.memory cimport make_shared, nullptr, shared_ptr
from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set

from .base_log_parser cimport BaseLogParser
from .parse_result import ParseResult
from .parser_factory import parser_register
from .utils cimport (
    Content,
    Token,
    load_data,
    mask_log_df,
    pair,
    split_log_df,
    string,
    to_table,
    vector
)

cdef struct LogCluster:
    Content content

cdef struct Node:
    vector[shared_ptr[LogCluster]] clusters
    unordered_map[Token, shared_ptr[Node]] children_node

cdef string _get_template(shared_ptr[LogCluster] cluster)noexcept nogil:
    cdef string template
    cdef Content content = deref(cluster).content
    cdef size_t length = content.size()

    cdef size_t idx
    for idx in range(length - 1):
        template.append(content[idx])
        template.append(" ")
    template.append(content.back())

    return template

cdef void _to_table(
    object log_df,
    vector[shared_ptr[LogCluster]]& cluster_results,
    string& structured_table_name,
    string& templates_table_name,
    bint keep_para,
):
    cdef size_t length = cluster_results.size()
    cdef vector[string] log_templates = vector[string](length)

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
)noexcept nogil:
    # Update param_str at different positions with the same length
    cdef size_t length1 = content1.size()
    cdef size_t length2 = content2.size()
    cdef const char* wildcard = "<#*#>"
    cdef Content template_content
    cdef size_t idx
    cdef Token token
    cdef unordered_set[Token] tmp_set

    if length1 == length2:
        template_content = content1
        for idx in range(length1):
            if template_content[idx] != content2[idx]:
                template_content[idx] = wildcard

    # param_str is updated at the new position with different length
    # Take the template with long length
    elif length1 > length2:
        for token in content2:
            tmp_set.insert(token)
        template_content = content1
        for idx in range(length1):
            if not tmp_set.contains(content1[idx]):
                template_content[idx] = wildcard

    else:
        for token in content1:
            tmp_set.insert(token)
        template_content = content2
        for idx in range(length2):
            if not tmp_set.contains(content2[idx]):
                template_content[idx] = wildcard

    return template_content

cdef void _add_to_prefix_tree(
    shared_ptr[LogCluster] cluster,
    shared_ptr[Node] root_node,
    uint16_t depth,
    uint16_t children,
)noexcept nogil:
    cdef const char* wird_card = "<#*#>"
    cdef size_t length = deref(cluster).content.size()
    cdef shared_ptr[Node] cur_node = root_node

    cdef uint16_t cur_node_depth = 1
    cdef Token token
    for token in deref(cluster).content:
        # if at max depth or this is last token in template - add current log cluster to the leaf node
        if cur_node_depth == depth or cur_node_depth == length + 1:
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
)noexcept nogil:
    cdef const char* wird_card = "<#*#>"
    cdef size_t length1 = content1.size()
    cdef size_t length2 = content2.size()

    # list are empty - full match
    if length1 == 0:
        return pair[float, uint16_t](<float>1.0, <uint16_t>0)

    cdef float sim
    cdef uint16_t param_count = 0

    cdef Token token
    for token in content1:
        if token == wird_card:
            param_count += 1

    cdef Content new_content1
    # If there are param_str, they are removed from the coefficient calculation
    if include_params:
        # 参数位不参与惩罚
        for token in content1:
            if token != wird_card:
                new_content1.push_back(token)
    else:
        new_content1 = content1

    cdef Content new_content2
    cdef size_t idx
    # If the token and the data have the same length, and there are param_str in the token
    if length1 == length2 and param_count > 0:
        # content2 removes the param_str position
        for idx in range(length2):
            if content1[idx] != wird_card:
                new_content2.push_back(content2[idx])
    else:
        new_content2 = content2

    # Calculate the Jaccard coefficient
    cdef unordered_set[Token] set1
    cdef unordered_set[Token] set2
    for token in new_content1:
        set1.insert(token)
    for token in new_content2:
        set2.insert(token)

    cdef size_t inter_size = 0
    if set1.size() <= set2.size():
        for token in set1:
            if set2.contains(token):
                inter_size += 1
    else:
        for token in set2:
            if set1.contains(token):
                inter_size += 1

    cdef size_t set1_size = set1.size()
    cdef size_t set2_size = set2.size()
    cdef size_t union_size = set1_size + set2_size - inter_size

    if union_size == 0:
        sim = <float>1.0
    else:
        sim = (<float>inter_size) / union_size

    # Jaccard coefficient calculated under the same conditions has a low simSep value
    # So gain is applied to the calculated value
    sim = <float>min(sim * 1.3, 1.0)

    return pair[float, uint16_t](sim, param_count)

cdef shared_ptr[LogCluster] _fast_match(
    vector[shared_ptr[LogCluster]]& clusters,
    Content& content,
    bint include_params,
    float sim_thr,
)noexcept nogil:
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
)noexcept nogil:
    cdef size_t length = content.size()
    cdef shared_ptr[Node] cur_node = root_node

    cdef uint16_t cur_node_depth = 1
    cdef Token token
    cdef unordered_map[Token, shared_ptr[Node]].iterator it
    for token in content:
        # at max depth or this is last token
        if cur_node_depth == depth or cur_node_depth == length + 1:
            # get best match among all clusters with same prefix, or None if no match is above sim_th
            break

        it = deref(cur_node).children_node.find(token)
        if it == deref(cur_node).children_node.end():
            it = deref(cur_node).children_node.find("<#*#>")
            if it == deref(cur_node).children_node.end():
                # no wildcard node exist
                return shared_ptr[LogCluster]()

        cur_node = deref(it).second
        cur_node_depth += 1

    return _fast_match(deref(cur_node).clusters, content, include_params, sim_thr)

cdef shared_ptr[LogCluster] _add_content(
    Content& content,
    shared_ptr[Node] root_node,
    uint16_t depth,
    uint16_t children,
    float sim_thr,
)noexcept nogil:
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

cdef class JaccardDrainLogParser(BaseLogParser):
    """JaccardDrain算法"""

    cdef uint16_t _depth
    cdef uint16_t _children
    cdef float _sim_thr
    cdef shared_ptr[Node] _root_node

    def __init__(
        self,
        string log_format,
        object maskings=None,
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
        super().__init__(log_format, maskings, delimiters)

        if depth < 3:
            raise ValueError("depth argument must be at least 3")

        self._depth = depth
        self._children = children
        self._sim_thr = sim_thr
        self._root_node = make_shared[Node]()

    def parse(
        self,
        string log_file,
        string structured_table_name,
        string templates_table_name,
        bint keep_para = False,
    ) -> ParseResult:
        cdef object log_df = load_data(log_file, self._log_format)
        # 预处理日志内容：掩码处理 + 分词
        log_df = mask_log_df(log_df, self._maskings)
        log_df = split_log_df(log_df, self._delimiters)
        log_df = log_df.drop("raw").collect()

        cdef object[::1] contents = log_df["Tokens"].to_numpy()
        cdef size_t length = contents.shape[0]
        cdef vector[shared_ptr[LogCluster]] cluster_results = (
            vector[shared_ptr[LogCluster]](length)
        )

        cdef size_t idx = 0
        cdef Content content
        cdef shared_ptr[LogCluster] match_cluster
        for content in contents:
            match_cluster = _add_content(
                content,
                self._root_node,
                self._depth,
                self._children,
                self._sim_thr,
            )
            cluster_results[idx] = match_cluster
            idx += 1

        _to_table(
            log_df,
            cluster_results,
            structured_table_name,
            templates_table_name,
            keep_para,
        )

        return ParseResult(
            log_file,
            length,
            structured_table_name,
            templates_table_name,
        )

    @staticmethod
    def name() -> str:
        return "JaccardDrain"

    @staticmethod
    def description() -> str:
        return "JaccardDrain 是一种基于 Drain 和 Jaccard 相似度的高效日志模板提取算法。"

parser_register(JaccardDrainLogParser)
