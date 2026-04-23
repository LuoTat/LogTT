#include "jaccard_drain_log_parser.hxx"
#include "duckdb_service.hxx"
#include "utils.hxx"
#include <ranges>

namespace logtt
{

JaccardDrainLogParser::JaccardDrainLogParser(
    std::string              log_regex,
    std::vector<std::string> named_fields,
    std::vector<std::string> timestamp_fields,
    std::string              timestamp_format,
    std::vector<Mask>        maskings,
    std::vector<char>        delimiters,
    std::uint16_t            depth,
    std::uint16_t            children,
    float                    sim_thr
):
    BaseLogParser {std::move(log_regex), std::move(named_fields), std::move(timestamp_fields), std::move(timestamp_format), std::move(maskings), std::move(delimiters)},
    m_depth {depth},
    m_children {children},
    m_sim_thr {sim_thr}
{}

std::uint32_t JaccardDrainLogParser::parse(const std::string& log_file, const std::string& structured_table_name, const std::string& templates_table_name, bool keep_para)
{
    // 初始化前缀树和日志簇池
    this->m_root = std::make_unique<Node>();
    this->m_cluster_pool.clear();
    std::vector<LogCluster*> cluster_results;
    std::vector<std::string> templates;
    // 获取数据库连接
    auto& conn {get_connection()};

    auto rel {load_data(conn, log_file, this->m_log_regex, this->m_named_fields, this->m_timestamp_fields, this->m_timestamp_format)};
    rel = mask_log_rel(rel, this->m_maskings);
    rel = split_log_rel(rel, this->m_delimiters);

    // 缓存分词结果，避免重复计算
    auto star_expr_1 {duckdb::make_uniq<duckdb::StarExpression>()};
    star_expr_1->exclude_list.emplace("MaskedContent");
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> project_exprs_1;
    project_exprs_1.push_back(std::move(star_expr_1));
    rel = rel->Project(std::move(project_exprs_1), {});
    rel = get_tmp(conn, rel);

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> project_exprs_2;
    project_exprs_2.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("Tokens"));
    auto result {to_m_result(rel->Project(std::move(project_exprs_2), {})->Execute())};
    auto log_length {result->RowCount()};
    for (auto&& data_chunk : result->Collection().Chunks())
    {
        const auto& tokens_col {data_chunk.data[0]};
        const auto& tokens_child {duckdb::ListVector::GetEntry(tokens_col)};

        const auto tokens_data {duckdb::FlatVector::GetData<duckdb::list_entry_t>(tokens_col)};
        const auto child_data {duckdb::FlatVector::GetData<duckdb::string_t>(tokens_child)};

        for (auto&& row : std::views::iota(0UL, data_chunk.size()))
        {
            TContent    content;
            const auto& entry {tokens_data[row]};
            for (auto&& i : std::views::iota(0UL, entry.length))
            {
                const auto& token {child_data[entry.offset + i]};
                content.emplace_back(token.GetData(), token.GetSize());
            }
            cluster_results.push_back(this->_add_content(content));
        }
    }

    templates.reserve(log_length);
    for (auto&& cluster : cluster_results)
    {
        templates.push_back(cluster->get_template());
    }

    // 移除多余列
    auto star_expr_2 {duckdb::make_uniq<duckdb::StarExpression>()};
    star_expr_2->exclude_list.emplace("Tokens");
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> project_exprs_3;
    project_exprs_3.push_back(std::move(star_expr_2));
    rel = rel->Project(std::move(project_exprs_3), {});
    to_table(conn, rel, templates, structured_table_name, templates_table_name, keep_para);

    return log_length;
}

JaccardDrainLogParser::LogCluster* JaccardDrainLogParser::_add_content(const TContent& content)
{
    auto match_cluster {this->_tree_search(content)};

    if (!match_cluster)
    {
        this->m_cluster_pool.emplace_back(content);
        match_cluster = &this->m_cluster_pool.back();
        this->_add_to_prefix_tree(match_cluster);
    }
    else
    {
        auto new_content {JaccardDrainLogParser::_create_template(content, match_cluster->content)};
        if (new_content != match_cluster->content)
        {
            match_cluster->content = std::move(new_content);
        }
    }

    return match_cluster;
}

JaccardDrainLogParser::LogCluster* JaccardDrainLogParser::_tree_search(const TContent& content, bool include_params)
{
    auto length {content.size()};
    auto cur_node {this->m_root.get()};

    for (auto&& [i, token] : std::views::enumerate(content))
    {
        auto cur_node_depth {i + 1};

        if (cur_node_depth == this->m_depth || std::cmp_equal(cur_node_depth, length + 1))
        {
            break;
        }

        if (auto it {cur_node->children_node.find(token)}; it == cur_node->children_node.end())
        {
            it = cur_node->children_node.find(WILDCARD);
            if (it == cur_node->children_node.end())
            {
                return nullptr;
            }
            cur_node = it->second.get();
        }
        else
        {
            cur_node = it->second.get();
        }
    }

    return this->_fast_match(content, cur_node, include_params);
}

JaccardDrainLogParser::LogCluster* JaccardDrainLogParser::_fast_match(const TContent& content, const Node* node, bool include_params)
{
    float         max_sim {-1.0F};
    std::uint16_t max_param_count {0};
    LogCluster*   max_cluster {nullptr};

    for (auto&& cluster : node->clusters)
    {
        auto [cur_sim, param_count] {this->_get_distance(content, cluster->content, include_params)};
        if (cur_sim > max_sim || (cur_sim == max_sim && param_count > max_param_count))
        {
            max_sim         = cur_sim;
            max_param_count = param_count;
            max_cluster     = cluster;
        }
    }

    if (max_sim > this->m_sim_thr)
    {
        return max_cluster;
    }

    return nullptr;
}

void JaccardDrainLogParser::_add_to_prefix_tree(LogCluster* cluster)
{
    auto length {cluster->content.size()};
    auto cur_node {this->m_root.get()};

    for (auto&& [i, token] : std::views::enumerate(cluster->content))
    {
        auto cur_node_depth {i + 1};

        if (cur_node_depth == this->m_depth || std::cmp_equal(cur_node_depth, length + 1))
        {
            cur_node->clusters.push_back(cluster);
            return;
        }

        if (auto it {cur_node->children_node.find(token)}; it == cur_node->children_node.end())
        {
            if (cur_node->children_node.size() + 1 < this->m_children)
            {
                // 如果当前节点不是最后一个节点，就添加一个新的节点
                auto [it, _] {cur_node->children_node.emplace(token, std::make_unique<Node>())};
                cur_node = it->second.get();
            }
            else if (cur_node->children_node.size() + 1 == this->m_children)
            {
                // 如果当前节点是最后一个节点，就添加一个新的通配符节点
                auto [it, _] {cur_node->children_node.emplace(WILDCARD, std::make_unique<Node>())};
                cur_node = it->second.get();
            }
            else
            {
                // 如果当前节点已满，就直接使用通配符节点
                cur_node = cur_node->children_node[WILDCARD].get();
            }
        }
        else
        {
            cur_node = it->second.get();
        }
    }
}

std::pair<float, std::uint16_t> JaccardDrainLogParser::_get_distance(const TContent& content1, const TContent& content2, bool include_params)
{
    // content1 是原始日志内容，content2 是模板内容
    if (content1.empty())
    {
        return {1.0F, 0};
    }

    std::uint16_t param_count {0};
    for (auto&& token : content2)
    {
        if (token == WILDCARD)
        {
            ++param_count;
        }
    }

    TContent filtered_content1;
    TContent filtered_content2;

    if (include_params && content1.size() == content2.size() && param_count > 0)
    {
        filtered_content1.reserve(content1.size() - param_count);
        filtered_content2.reserve(content2.size() - param_count);

        for (auto&& [token1, token2] : std::views::zip(content1, content2))
        {
            if (token2 == WILDCARD)
            {
                continue;
            }

            filtered_content1.push_back(token1);
            filtered_content2.push_back(token2);
        }
    }
    else
    {
        filtered_content1 = content1;
        filtered_content2.reserve(content2.size());

        for (auto&& token : content2)
        {
            if (token != WILDCARD)
            {
                filtered_content2.push_back(token);
            }
        }
    }

    std::unordered_set<Token> set1 {filtered_content1.begin(), filtered_content1.end()};
    std::unordered_set<Token> set2 {filtered_content2.begin(), filtered_content2.end()};
    std::uint32_t             inter_size {0};

    if (set1.size() <= set2.size())
    {
        for (auto&& token : set1)
        {
            if (set2.contains(token))
            {
                ++inter_size;
            }
        }
    }
    else
    {
        for (auto&& token : set2)
        {
            if (set1.contains(token))
            {
                ++inter_size;
            }
        }
    }

    auto union_size {set1.size() + set2.size() - inter_size};
    auto sim {union_size == 0 ? 1.0F : static_cast<float>(inter_size) / static_cast<float>(union_size)};

    sim = std::min(sim * 1.3F, 1.0F);
    return {sim, param_count};
}

TContent JaccardDrainLogParser::_create_template(const TContent& content1, const TContent& content2)
{
    TContent new_content;
    auto     length1 {content1.size()};
    auto     length2 {content2.size()};

    if (length1 == length2)
    {
        new_content = content1;
        for (auto&& [token1, token2] : std::views::zip(new_content, content2))
        {
            if (token1 != token2)
            {
                token1 = WILDCARD;
            }
        }
    }
    else if (length1 > length2)
    {
        new_content = content1;
        std::unordered_set<Token> tmp_set {content2.begin(), content2.end()};
        for (auto&& token : new_content)
        {
            if (!tmp_set.contains(token))
            {
                token = WILDCARD;
            }
        }
    }
    else
    {
        new_content = content2;
        std::unordered_set<Token> tmp_set {content1.begin(), content1.end()};
        for (auto&& token : new_content)
        {
            if (!tmp_set.contains(token))
            {
                token = WILDCARD;
            }
        }
    }

    return new_content;
}

std::string JaccardDrainLogParser::LogCluster::get_template() const
{
    return this->content |
           std::views::join_with(' ') |
           std::ranges::to<std::string>();
}

}    // namespace logtt