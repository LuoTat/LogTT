# distutils: language=c++

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


cdef object ParseResult
cdef object parser_register
cdef object pl

import polars as pl
from cython.operator cimport dereference as deref
from libcpp.memory cimport make_shared, shared_ptr
from libcpp.numeric cimport reduce
from libcpp.unordered_map cimport unordered_map

from .base_log_parser cimport BaseLogParser
from .parse_result import ParseResult
from .parser_factory import parser_register
from .utils cimport (
    Content,
    load_data,
    mask_log_df,
    pair,
    split_log_df,
    string,
    to_table,
    vector,
)

cdef extern from *:
    """
    #include <functional>
    #include <utility>

    namespace std {
        template<>
        struct hash<std::pair<size_t, size_t>>
        {
            size_t operator()(const std::pair<size_t,size_t>& p) const noexcept
            {
                size_t seed = p.first;
                seed ^= p.second + 0x9e3779b97f4a7c15ULL
                        + (seed << 6)
                        + (seed >> 2);
                return seed;
            }
        };
    }
    """

cdef struct LogCluster:
    Content content
    vector[size_t] rows
    bint merged

# 这里第一个size_t是token_count，第二个size_t是para_count
ctypedef pair[size_t, size_t] LogBinKey
ctypedef unordered_map[LogBinKey, vector[shared_ptr[LogCluster]]] LogBin

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
    size_t log_length,
    vector[shared_ptr[LogCluster]]& merged_log_clusters,
    string& structured_table_name,
    string& templates_table_name,
    bint keep_para,
):
    cdef vector[string] log_templates = vector[string](log_length)

    cdef shared_ptr[LogCluster] log_cluster
    cdef string log_template
    cdef size_t row
    for log_cluster in merged_log_clusters:
        log_template = _get_template(log_cluster)
        for row in deref(log_cluster).rows:
            log_templates[row] = log_template

    to_table(
        log_df,
        log_templates,
        structured_table_name,
        templates_table_name,
        keep_para,
    )

cdef shared_ptr[LogCluster] _merge_log_cluster(
    shared_ptr[LogCluster] cluster1,
    shared_ptr[LogCluster] cluster2,
)noexcept nogil:
    cdef const char* wildcard = "<#*#>"

    cdef size_t idx
    for idx in range(deref(cluster1).content.size()):
        if deref(cluster1).content[idx] != deref(cluster2).content[idx]:
            deref(cluster1).content[idx] = wildcard

    deref(cluster1).rows.insert(
        deref(cluster1).rows.end(),
        deref(cluster2).rows.begin(),
        deref(cluster2).rows.end(),
    )

    return cluster1

cdef bint _has_diff(
    shared_ptr[LogCluster] cluster1,
    shared_ptr[LogCluster] cluster2,
    float merge_thr,
)noexcept nogil:
    cdef size_t length = deref(cluster1).content.size()
    cdef size_t diff = 0

    cdef size_t idx
    for idx in range(length):
        if deref(cluster1).content[idx] != deref(cluster2).content[idx]:
            diff += 1

    return 0 < <float> diff / length <= merge_thr

cdef vector[shared_ptr[LogCluster]] _reconcile(
    LogBin& log_bin,
    size_t log_length,
    size_t cluster_thr,
    float merge_thr
)noexcept nogil:
    cdef vector[shared_ptr[LogCluster]] merged_log_clusters
    merged_log_clusters.reserve(log_length)

    cdef pair[LogBinKey, vector[shared_ptr[LogCluster]]] pair
    cdef vector[shared_ptr[LogCluster]] value
    cdef vector[vector[shared_ptr[LogCluster]]] log_cluster_groups
    cdef size_t idx
    cdef size_t idy
    cdef shared_ptr[LogCluster] log_cluster1
    cdef shared_ptr[LogCluster] log_cluster2
    cdef vector[shared_ptr[LogCluster]] log_cluster_group
    cdef shared_ptr[LogCluster] merged_log_cluster
    for pair in log_bin:
        value = pair.second
        if (value.size() <= cluster_thr):
            merged_log_clusters.insert(
                merged_log_clusters.end(),
                value.begin(),
                value.end(),
            )
            continue

        log_cluster_groups.clear()
        for idx in range(value.size()):
            log_cluster1 = value[idx]
            if deref(log_cluster1).merged:
                continue

            deref(log_cluster1).merged = True
            log_cluster_groups.push_back(
                vector[shared_ptr[LogCluster]](1, log_cluster1)
            )

            for idy in range(idx + 1, value.size()):
                log_cluster2 = value[idy]
                if deref(log_cluster2).merged:
                    continue

                if _has_diff(log_cluster1, log_cluster2, merge_thr):
                    deref(log_cluster2).merged = True
                    log_cluster_groups.back().push_back(log_cluster2)

        for log_cluster_group in log_cluster_groups:
            merged_log_cluster = reduce(
                log_cluster_group.begin() + 1,
                log_cluster_group.end(),
                log_cluster_group.front(),
                _merge_log_cluster,
            )
            merged_log_clusters.push_back(merged_log_cluster)

    return merged_log_clusters

cdef LogBin _get_log_bins(object log_df):
    cdef LogBin log_bin
    cdef object groups = (
        log_df.select(
            pl.col("Tokens"),
            pl.col("Tokens").list.len().alias("token_count"),
            pl.col("Tokens")
            .list.eval(pl.element().str.count_matches(r"<#.*#>"))
            .list.sum()
            .alias("para_count"),
        )
        .with_row_index("idx")
        .group_by(["token_count", "para_count", "Tokens"])
        .agg(pl.col("idx"))
    )

    cdef object row
    cdef LogBinKey key
    cdef Content content
    cdef vector[size_t] rows
    cdef bint merged
    for row in groups.iter_rows():
        key = LogBinKey(row[0], row[1])
        content = row[2]
        rows = row[3]
        merged = False
        log_bin[key].push_back(make_shared[LogCluster](content, rows, merged))

    return log_bin

cdef class AELLogParser(BaseLogParser):
    """AEL算法"""

    cdef size_t _cluster_thr
    cdef float _merge_thr

    def __init__(
        self,
        string log_format,
        object maskings=None,
        object delimiters=None,
        size_t cluster_thr=2,
        float merge_thr=1,
    ):
        """
        Args:
            log_cluster_thr: Minimum number of log clusters to trigger reconciliation.
            merge_thr: Maximum percentage of difference to merge two log clusters.
        """
        super().__init__(log_format, maskings, delimiters)

        self._cluster_thr = cluster_thr
        self._merge_thr = merge_thr

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

        cdef LogBin log_bin = _get_log_bins(log_df)
        cdef size_t log_length = log_df.height
        cdef vector[shared_ptr[LogCluster]] merged_log_clusters = _reconcile(
            log_bin,
            log_length,
            self._cluster_thr,
            self._merge_thr,
        )

        _to_table(
            log_df,
            log_length,
            merged_log_clusters,
            structured_table_name,
            templates_table_name,
            keep_para,
        )

        return ParseResult(
            log_file,
            log_length,
            structured_table_name,
            templates_table_name,
        )

    @staticmethod
    def name() -> str:
        return "AEL"

    @staticmethod
    def description() -> str:
        return "AEL 是一种通过分桶与相似度合并来自动抽取日志模板的解析方法。"

parser_register(AELLogParser)
