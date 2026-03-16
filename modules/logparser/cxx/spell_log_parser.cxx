#include "spell_log_parser.hxx"
#include "utils.hxx"
#include <algorithm>
#include <cmath>
#include <ranges>

namespace logparser
{

SpellLogParser::SpellLogParser(
    std::string              log_regex,
    std::vector<std::string> named_fields,
    std::vector<Mask>        maskings,
    std::vector<char>        delimiters,
    float                    sim_thr
):
    BaseLogParser {std::move(log_regex), std::move(named_fields), std::move(maskings), std::move(delimiters)},
    m_sim_thr {sim_thr}
{}

std::size_t SpellLogParser::parse(const std::string& log_file, const std::string& structured_table_name, const std::string& templates_table_name, bool keep_para)
{
    // 初始化前缀树和日志簇池
    this->m_root = std::make_unique<Node>();
    this->m_cluster_pool.clear();
    std::vector<LogCluster*> cluster_results;
    std::vector<std::string> templates;
    // 初始化内存数据库
    duckdb::DuckDB     db {DB_PATH};
    duckdb::Connection conn {db};
    conn.EnableProfiling();

    auto rel {load_data(conn, log_file, this->m_log_regex, this->m_named_fields)};
    rel = mask_log_rel(rel, this->m_maskings);
    rel = split_log_rel(rel, this->m_delimiters);

    // 缓存分词结果，避免重复计算
    rel = rel->Project("* EXCLUDE MaskedContent");
    rel->Create("_tmp", true);
    rel = conn.Table("_tmp");

    auto        result {to_materialized_query_result(rel->Project("Tokens")->Execute())};
    std::size_t log_length {result->RowCount()};
    for (const auto& data_chunk : result->Collection().Chunks())
    {
        auto data_length {data_chunk.size()};

        const auto& tokens_col {data_chunk.data[0]};
        const auto& tokens_child {duckdb::ListVector::GetEntry(tokens_col)};

        const auto tokens_data {duckdb::FlatVector::GetData<duckdb::list_entry_t>(tokens_col)};
        const auto child_data {duckdb::FlatVector::GetData<duckdb::string_t>(tokens_child)};

        for (auto row : std::views::iota(0UL, data_length))
        {
            TContent    content;
            const auto& entry {tokens_data[row]};
            for (auto i : std::views::iota(0UL, entry.length))
            {
                const auto& token {child_data[entry.offset + i]};
                content.emplace_back(token.GetData(), token.GetSize());
            }
            cluster_results.push_back(this->_add_content(content));
        }
    }

    templates.reserve(log_length);
    for (const auto cluster : cluster_results)
    {
        templates.push_back(cluster->get_template());
    }

    // 移除多余列
    rel = rel->Project("* EXCLUDE Tokens");
    to_table(conn, rel, templates, structured_table_name, templates_table_name, keep_para);

    return log_length;
}

SpellLogParser::LogCluster* SpellLogParser::_add_content(const TContent& content)
{
    auto match_cluster {
        this->_tree_subseq_match(content)
            ?: (this->_subseq_match(content)
                    ?: this->_lcs_match(content))
    };

    if (!match_cluster)
    {
        this->m_cluster_pool.emplace_back(content);
        match_cluster = &this->m_cluster_pool.back();
        this->_add_seq_to_prefix_tree(match_cluster);
        return match_cluster;
    }

    auto lcs {SpellLogParser::_lcs_content(content, match_cluster->content)};
    auto new_content {SpellLogParser::_create_template(lcs, match_cluster->content)};
    if (new_content != match_cluster->content)
    {
        this->_remove_seq_from_prefix_tree(match_cluster);
        match_cluster->content = std::move(new_content);
        this->_add_seq_to_prefix_tree(match_cluster);
    }

    return match_cluster;
}

SpellLogParser::LogCluster* SpellLogParser::_tree_subseq_match(const TContent& content)
{
    auto required_length {this->m_sim_thr * content.size()};
    auto cur_node {this->m_root.get()};
    // 这个用来记录已经匹配的常量token数量，用来快速匹配的cluster
    // 这个值和也就是对应节点的深度-1
    std::size_t matched_const_count {0};

    for (const auto& token : content)
    {
        // 如果当前节点挂载了一个cluster，且常量token数量超过阈值，就直接返回这个cluster
        if (cur_node->cluster && matched_const_count >= required_length)
        {
            return cur_node->cluster;
        }

        if (auto it {cur_node->children_node.find(token)}; it != cur_node->children_node.end())
        {
            cur_node = it->second.get();
            ++matched_const_count;
        }
    }

    return nullptr;
}

SpellLogParser::LogCluster* SpellLogParser::_subseq_match(const TContent& content)
{
    auto required_length {this->m_sim_thr * content.size()};

    for (const auto& cluster : this->m_cluster_pool)
    {
        if (cluster.content.size() < required_length)
        {
            continue;
        }

        if (SpellLogParser::_is_subsequence(content, cluster.content))
        {
            return const_cast<LogCluster*>(&cluster);
        }
    }

    return nullptr;
}

SpellLogParser::LogCluster* SpellLogParser::_lcs_match(const TContent& content)
{
    auto        required_content_lcs {static_cast<std::size_t>(std::ceil(this->m_sim_thr * content.size()))};
    std::size_t max_lcs_length {0};
    LogCluster* max_cluster {nullptr};

    for (const auto& cluster : this->m_cluster_pool)
    {
        auto required_cluster_lcs {static_cast<std::size_t>(std::ceil(this->m_sim_thr * cluster.content.size()))};
        auto lcs_length {SpellLogParser::_lcs_length(content, cluster.content, std::max(required_content_lcs, required_cluster_lcs))};

        if (lcs_length == 0)
        {
            continue;
        }

        if (lcs_length > max_lcs_length || (lcs_length == max_lcs_length && cluster.content.size() < max_cluster->content.size()))
        {
            max_cluster    = const_cast<LogCluster*>(&cluster);
            max_lcs_length = lcs_length;
        }
    }

    return max_cluster;
}

void SpellLogParser::_add_seq_to_prefix_tree(LogCluster* cluster)
{
    auto cur_node {this->m_root.get()};

    for (const auto& token : cluster->content)
    {
        if (token == WILDCARD)
        {
            continue;
        }

        if (auto it {cur_node->children_node.find(token)}; it != cur_node->children_node.end())
        {
            cur_node = it->second.get();
        }
        else
        {
            auto [it2, _] {cur_node->children_node.emplace(token, std::make_unique<Node>())};
            cur_node = it2->second.get();
        }

        ++cur_node->template_no;
    }

    cur_node->cluster = cluster;
}

void SpellLogParser::_remove_seq_from_prefix_tree(const LogCluster* cluster)
{
    auto cur_node {this->m_root.get()};

    for (const auto& token : cluster->content)
    {
        if (token == WILDCARD)
        {
            continue;
        }

        auto it {cur_node->children_node.find(token)};
        if (it == cur_node->children_node.end())
        {
            return;
        }

        auto matched_node {it->second.get()};
        if (matched_node->template_no == 1)
        {
            cur_node->children_node.erase(token);
            return;
        }

        --matched_node->template_no;
        cur_node = matched_node;
    }
}

bool SpellLogParser::_is_subsequence(const TContent& source, const TContent& target)
{
    // 判断 target 是不是 source 的子序列
    auto source_length {source.size()};
    auto target_length {target.size()};

    std::size_t target_idx {0};
    for (auto source_idx : std::views::iota(0UL, source_length))
    {
        if (source[source_idx] == target[target_idx])
        {
            ++target_idx;
        }
    }

    return target_idx == target_length;
}

std::size_t SpellLogParser::_lcs_length(const TContent& content1, const TContent& content2, std::size_t min_required_lcs)
{
    auto length1 {content1.size()};
    auto length2 {content2.size()};

    std::reference_wrapper<const TContent> long_content {content1};
    std::reference_wrapper<const TContent> short_content {content2};
    std::size_t                            long_length {length1};
    std::size_t                            short_length {length2};

    // 确定长短序列
    if (length2 > length1)
    {
        long_content  = content2;
        short_content = content1;
        long_length   = length2;
        short_length  = length1;
    }

    if (short_length == 0 || min_required_lcs > short_length)
    {
        return 0;
    }

    // 单行滚动数组
    std::vector<std::size_t> dp(short_length + 1);
    for (auto i : std::views::iota(0UL, long_length))
    {
        std::size_t prev_diag {0};

        for (auto j : std::views::iota(0UL, short_length))
        {
            auto prev_up {dp[j + 1]};
            if (long_content.get()[i] == short_content.get()[j])
            {
                dp[j + 1] = prev_diag + 1;
            }
            else if (dp[j] >= dp[j + 1])
            {
                dp[j + 1] = dp[j];
            }
            prev_diag = prev_up;
        }

        auto cur_lcs {dp[short_length]};
        auto remain_rows {long_length - i - 1};
        // 阈值剪枝
        if (cur_lcs + remain_rows < min_required_lcs)
        {
            return 0;
        }
    }

    return dp[short_length];
}

TContent SpellLogParser::_lcs_content(const TContent& content1, const TContent& content2)
{
    auto length1 {content1.size()};
    auto length2 {content2.size()};

    std::vector<std::vector<std::size_t>> lengths(length1 + 1, std::vector<std::size_t>(length2 + 1));

    for (auto i : std::views::iota(0UL, length1))
    {
        for (auto j : std::views::iota(0UL, length2))
        {
            if (content1[i] == content2[j])
            {
                lengths[i + 1][j + 1] = lengths[i][j] + 1;
            }
            else if (lengths[i + 1][j] >= lengths[i][j + 1])
            {
                lengths[i + 1][j + 1] = lengths[i + 1][j];
            }
            else
            {
                lengths[i + 1][j + 1] = lengths[i][j + 1];
            }
        }
    }

    auto     lcs_length {lengths[length1][length2]};
    TContent lcs {lcs_length};
    auto     lcs_idx {lcs_length};

    auto i {length1};
    auto j {length2};
    while (i != 0 && j != 0)
    {
        if (lengths[i][j] == lengths[i - 1][j])
        {
            --i;
        }
        else if (lengths[i][j] == lengths[i][j - 1])
        {
            --j;
        }
        else
        {
            if (lcs_idx > 0)
            {
                --lcs_idx;
                lcs[lcs_idx] = content1[i - 1];
            }
            --i;
            --j;
        }
    }

    return lcs;
}

TContent SpellLogParser::_create_template(const TContent& lcs, const TContent& content)
{
    TContent new_content;
    auto     lcs_length {lcs.size()};
    auto     content_length {content.size()};

    if (lcs_length == 0)
    {
        return new_content;
    }

    std::size_t lcs_idx {0};
    for (auto i : std::views::iota(0UL, content_length))
    {
        const auto& token {content[i]};
        if (lcs_idx < lcs_length && token == lcs[lcs_idx])
        {
            new_content.push_back(token);
            ++lcs_idx;
        }
        else
        {
            new_content.push_back(WILDCARD);
        }

        if (lcs_idx == lcs_length)
        {
            if (i < content_length - 1)
            {
                new_content.push_back(WILDCARD);
            }
            break;
        }
    }

    return SpellLogParser::_merge_wildcards(new_content);
}

TContent SpellLogParser::_merge_wildcards(const TContent& content)
{
    TContent merged_content;

    for (const auto& token : content)
    {
        if (token == WILDCARD && !merged_content.empty() && merged_content.back() == WILDCARD)
        {
            continue;
        }

        merged_content.push_back(token);
    }

    return merged_content;
}

std::string SpellLogParser::LogCluster::get_template() const
{
    return this->content |
           std::views::join_with(' ') |
           std::ranges::to<std::string>();
}

}    // namespace logparser
