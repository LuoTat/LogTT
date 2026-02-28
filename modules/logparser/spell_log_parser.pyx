# distutils: language=c++

# SPDX-License-Identifier: Apache-2.0
# This file implements the Spell algorithm for log parsing.
# Based on https://github.com/logpai/logparser/blob/master/logparser/Spell/Spell.py by LogPAI team


cdef object ParseResult
cdef object parser_register

from cython.operator cimport dereference as deref
from libc.math cimport ceilf
from libcpp.memory cimport make_shared, nullptr, shared_ptr
from libcpp.unordered_map cimport unordered_map

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
    vector,
)

cdef struct LogCluster:
    Content content

cdef struct Node:
    int template_no
    shared_ptr[LogCluster] cluster
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

cdef Content _merge_wildcards(Content& content)noexcept nogil:
    cdef const char* wildcard = "<#*#>"

    cdef Content merged_content
    cdef Token token
    for token in content:
        if (
            token == wildcard
            and merged_content.size() > 0
            and merged_content.back() == wildcard
        ):
            continue
        merged_content.push_back(token)

    return merged_content

cdef Content _create_template(
    const Content& lcs,
    const Content& content,
)noexcept nogil:
    cdef Content ret_val
    cdef size_t lcs_length = lcs.size()
    cdef size_t content_length = content.size()

    if lcs_length == 0:
        return ret_val

    cdef const char* wildcard = "<#*#>"

    cdef size_t idx
    cdef Token token
    cdef size_t lcs_idx = 0
    for idx in range(content_length):
        token = content[idx]
        if lcs_idx < lcs_length and token == lcs[lcs_idx]:
            ret_val.push_back(token)
            lcs_idx += 1
        else:
            ret_val.push_back(wildcard)

        if lcs_idx == lcs_length:
            if idx < content_length - 1:
                ret_val.push_back(wildcard)
            break

    return _merge_wildcards(ret_val)

cdef void _remove_seq_from_prefix_tree(
    shared_ptr[LogCluster] cluster,
    shared_ptr[Node] root_node,
)noexcept nogil:
    cdef shared_ptr[Node] cur_node = root_node

    cdef Token token
    cdef unordered_map[Token, shared_ptr[Node]].iterator it
    cdef shared_ptr[Node] matched_node
    for token in deref(cluster).content:
        if token == "<#*#>":
            continue

        it = deref(cur_node).children_node.find(token)
        if it == deref(cur_node).children_node.end():
            return

        matched_node = deref(it).second
        if deref(matched_node).template_no == 1:
            deref(cur_node).children_node.erase(token)
            return

        deref(matched_node).template_no -= 1
        cur_node = matched_node

cdef Content _lcs_content(
    Content& content1,
    Content& content2,
)noexcept nogil:
    cdef size_t length1 = content1.size()
    cdef size_t length2 = content2.size()

    cdef vector[vector[size_t]] lengths = (
        vector[vector[size_t]](length1 + 1, vector[size_t](length2 + 1))
    )

    cdef size_t i
    cdef size_t j
    for i in range(length1):
        for j in range(length2):
            if content1[i] == content2[j]:
                lengths[i + 1][j + 1] = lengths[i][j] + 1
            elif lengths[i + 1][j] >= lengths[i][j + 1]:
                lengths[i + 1][j + 1] = lengths[i + 1][j]
            else:
                lengths[i + 1][j + 1] = lengths[i][j + 1]

    cdef size_t lcs_length = lengths[length1][length2]
    cdef Content lcs = Content(lcs_length)
    cdef size_t lcs_idx = lcs_length

    i = length1
    j = length2
    while i != 0 and j != 0:
        if lengths[i][j] == lengths[i - 1][j]:
            i -= 1
        elif lengths[i][j] == lengths[i][j - 1]:
            j -= 1
        else:
            if lcs_idx > 0:
                lcs_idx -= 1
                lcs[lcs_idx] = content1[i - 1]
            i -= 1
            j -= 1

    return lcs

cdef void _add_seq_to_prefix_tree(
    shared_ptr[LogCluster] cluster,
    shared_ptr[Node] root_node,
)noexcept nogil:
    cdef shared_ptr[Node] cur_node = root_node

    cdef Token token
    cdef unordered_map[Token, shared_ptr[Node]].iterator it
    cdef shared_ptr[Node] matched_node
    for token in deref(cluster).content:
        if token == "<#*#>":
            continue

        it = deref(cur_node).children_node.find(token)
        if it != deref(cur_node).children_node.end():
            matched_node = deref(it).second
        else:
            matched_node = make_shared[Node](0)
            deref(cur_node).children_node.insert(
                pair[Token, shared_ptr[Node]](token, matched_node)
            )
        deref(matched_node).template_no += 1
        cur_node = matched_node

    deref(cur_node).cluster = cluster

cdef size_t _lcs_length(
    Content& content1,
    Content& content2,
    size_t min_required_lcs,
)noexcept nogil:
    cdef size_t length1 = content1.size()
    cdef size_t length2 = content2.size()

    # 确定长短序列
    cdef size_t long_length
    cdef size_t short_length
    cdef Content* long_content
    cdef Content* short_content
    if length1 >= length2:
        long_length = length1
        short_length = length2
        long_content = &content1
        short_content = &content2
    else:
        long_length = length2
        short_length = length1
        long_content = &content2
        short_content = &content1

    if short_length == 0 or min_required_lcs > short_length:
        return 0

    # 单行滚动数组
    cdef vector[size_t] dp = vector[size_t](short_length + 1)

    cdef size_t i
    cdef size_t j
    cdef size_t prev_diag
    cdef Token long_token
    cdef size_t prev_up
    cdef Token short_token
    cdef size_t cur_lcs
    cdef size_t remain_rows
    for i in range(long_length):
        prev_diag = 0
        long_token = deref(long_content)[i]
        for j in range(short_length):
            prev_up = dp[j + 1]
            short_token = deref(short_content)[j]
            if long_token == short_token:
                dp[j + 1] = prev_diag + 1
            elif dp[j] >= dp[j + 1]:
                dp[j + 1] = dp[j]
            prev_diag = prev_up

        cur_lcs = dp[short_length]
        remain_rows = long_length - i - 1
        # 阈值剪枝
        if cur_lcs + remain_rows < min_required_lcs:
            return 0

    return dp[short_length]

cdef shared_ptr[LogCluster] _lcs_match(
    Content& content,
    float sim_thr,
    vector[shared_ptr[LogCluster]]& clusters,
)noexcept nogil:
    cdef size_t content_length = content.size()
    cdef size_t required_content_lcs = <size_t> ceilf(sim_thr * content_length)
    cdef size_t max_lcs_length = 0
    cdef shared_ptr[LogCluster] max_cluster

    cdef shared_ptr[LogCluster] cluster
    cdef size_t required_cluster_lcs
    cdef size_t lcs_length
    for cluster in clusters:
        required_cluster_lcs = <size_t> ceilf(sim_thr * deref(cluster).content.size())
        lcs_length = _lcs_length(
            content,
            deref(cluster).content,
            max(required_content_lcs, required_cluster_lcs),
        )
        if lcs_length == 0:
            continue
        if lcs_length > max_lcs_length or (
            lcs_length == max_lcs_length
            and deref(cluster).content.size() < deref(max_cluster).content.size()
        ):
            max_cluster = cluster
            max_lcs_length = lcs_length

    return max_cluster

cdef bint _is_subsequence(Content& source, Content& target)noexcept nogil:
    # 判断 target 是不是 source 的子序列
    cdef size_t source_length = source.size()
    cdef size_t target_length = target.size()

    cdef size_t source_idx
    cdef size_t target_idx = 0
    for source_idx in range(source_length):
        if source[source_idx] == target[target_idx]:
            target_idx += 1

    return target_idx == target_length

cdef shared_ptr[LogCluster] _subseq_match(
    Content& content,
    float sim_thr,
    vector[shared_ptr[LogCluster]]& clusters,
)noexcept nogil:
    cdef size_t content_length = content.size()
    cdef float required_length = sim_thr * content_length

    cdef shared_ptr[LogCluster] cluster
    for cluster in clusters:
        if deref(cluster).content.size() < required_length:
            continue

        if _is_subsequence(content, deref(cluster).content):
            return cluster

    return shared_ptr[LogCluster]()

cdef shared_ptr[LogCluster] _tree_subseq_match(
    Content& content,
    shared_ptr[Node] root_node,
    float sim_thr,
)noexcept nogil:
    cdef size_t content_length = content.size()
    cdef float required_length = sim_thr * content_length
    cdef shared_ptr[Node] cur_node = root_node
    # 这个用来记录已经匹配的常量token数量，用来快速匹配的cluster
    # 这个值和也就是对应节点的深度-1
    cdef size_t matched_const_count = 0

    cdef Token token
    cdef unordered_map[Token, shared_ptr[Node]].iterator it
    for token in content:
        # 如果当前节点挂载了一个cluster，且常量token数量超过阈值，就直接返回这个cluster
        if (
            deref(cur_node).cluster.get() != nullptr
            and matched_const_count >= required_length
        ):
            return deref(cur_node).cluster

        it = deref(cur_node).children_node.find(token)
        if it != deref(cur_node).children_node.end():
            cur_node = deref(it).second
            matched_const_count += 1

    return shared_ptr[LogCluster]()

cdef shared_ptr[LogCluster] _add_content(
    Content& content,
    shared_ptr[Node] root_node,
    float sim_thr,
    vector[shared_ptr[LogCluster]]& clusters,
)noexcept nogil:
    cdef shared_ptr[LogCluster] match_cluster = (
        _tree_subseq_match(content, root_node, sim_thr)
        or _subseq_match(content, sim_thr, clusters)
        or _lcs_match(content, sim_thr, clusters)
    )

    # Match no existing log cluster
    if match_cluster.get() == nullptr:
        match_cluster = make_shared[LogCluster](content)
        clusters.push_back(match_cluster)
        _add_seq_to_prefix_tree(match_cluster, root_node)
        return match_cluster

    # Add the new log message to the existing cluster
    cdef Content new_content = _create_template(
        _lcs_content(content, deref(match_cluster).content),
        deref(match_cluster).content,
    )
    if new_content != deref(match_cluster).content:
        _remove_seq_from_prefix_tree(match_cluster, root_node)
        deref(match_cluster).content = new_content
        _add_seq_to_prefix_tree(match_cluster, root_node)

    return match_cluster

cdef class SpellLogParser(BaseLogParser):
    """Spell算法"""

    cdef float _sim_thr
    cdef shared_ptr[Node] _root_node
    cdef vector[shared_ptr[LogCluster]] _clusters

    def __init__(
        self,
        string log_format,
        object masking=None,
        object delimiters=None,
        float sim_thr=0.5,
    ):
        """
        Args:
            sim_thr: Similarity threshold (0-1).
        """
        super().__init__(log_format, masking, delimiters)

        self._sim_thr = sim_thr
        self._root_node = make_shared[Node]()

    def parse(
        self,
        string log_file,
        string structured_table_name,
        string templates_table_name,
        bint keep_para=False,
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
                self._sim_thr,
                self._clusters,
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
        return "Spell"

    @staticmethod
    def description() -> str:
        return "Spell 是一种基于前缀树和LCS算法的日志解析方法。"

parser_register(SpellLogParser)
