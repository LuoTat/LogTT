#include "drain_log_parser.hxx"
#include "duckdb_service.hxx"
#include "utils.hxx"
#include <ranges>

namespace logtt
{

DrainLogParser::DrainLogParser(
    std::string              log_regex,
    std::vector<std::string> named_fields,
    std::vector<Mask>        maskings,
    std::vector<char>        delimiters,
    std::uint16_t            depth,
    std::uint16_t            children,
    float                    sim_thr
):
    BaseLogParser {std::move(log_regex), std::move(named_fields), std::move(maskings), std::move(delimiters)},
    m_depth {depth},
    m_children {children},
    m_sim_thr {sim_thr}
{}

std::uint32_t DrainLogParser::parse(const std::string& log_file, const std::string& structured_table_name, const std::string& templates_table_name, bool keep_para)
{
    // 初始化前缀树和日志簇池
    this->m_root = std::make_unique<Node>();
    this->m_cluster_pool.clear();
    std::vector<LogCluster*> cluster_results;
    std::vector<std::string> templates;
    // 获取数据库连接
    auto& conn {get_connection()};

    auto rel {load_data(conn, log_file, this->m_log_regex, this->m_named_fields)};
    rel = mask_log_rel(rel, this->m_maskings);
    rel = split_log_rel(rel, this->m_delimiters);

    // 缓存分词结果，避免重复计算
    rel = rel->Project("* EXCLUDE MaskedContent");
    rel = get_tmp(conn, rel);

    auto result {to_materialized_query_result(rel->Project("Tokens")->Execute())};
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
    rel = rel->Project("* EXCLUDE Tokens");
    to_table(conn, rel, templates, structured_table_name, templates_table_name, keep_para);

    return log_length;
}

DrainLogParser::LogCluster* DrainLogParser::_add_content(const TContent& content)
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
        auto new_content {DrainLogParser::_create_template(content, match_cluster->content)};
        if (new_content != match_cluster->content)
        {
            match_cluster->content = std::move(new_content);
        }
    }

    return match_cluster;
}

DrainLogParser::LogCluster* DrainLogParser::_tree_search(const TContent& content, bool include_params)
{
    auto length {content.size()};
    auto length_token {std::to_string(length)};
    auto cur_node {this->m_root.get()};

    for (auto&& [i, token] : std::views::enumerate(std::views::concat(std::views::single(length_token), content)))
    {
        auto cur_node_depth {i + 1};

        if (cur_node_depth == this->m_depth || std::cmp_equal(cur_node_depth, length + 2))
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

DrainLogParser::LogCluster* DrainLogParser::_fast_match(const TContent& content, const Node* node, bool include_params)
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

void DrainLogParser::_add_to_prefix_tree(LogCluster* cluster)
{
    auto length {cluster->content.size()};
    auto length_token {std::to_string(length)};
    auto cur_node {this->m_root.get()};

    for (auto&& [i, token] : std::views::enumerate(std::views::concat(std::views::single(length_token), cluster->content)))
    {
        auto cur_node_depth {i + 1};

        if (cur_node_depth == this->m_depth || std::cmp_equal(cur_node_depth, length + 2))
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

std::pair<float, std::uint16_t> DrainLogParser::_get_distance(const TContent& content1, const TContent& content2, bool include_params)
{
    if (content1.size() == 0)
    {
        return {1.0F, 0};
    }

    std::uint16_t sim_tokens {0};
    std::uint16_t param_count {0};

    for (auto&& [token1, token2] : std::views::zip(content1, content2))
    {
        // 这里的 content2 是模板，所以这里要用 token2 来判断是否是参数位
        if (token2 == WILDCARD)
        {
            ++param_count;
            continue;
        }

        if (token1 == token2)
        {
            ++sim_tokens;
        }
    }

    if (include_params)
    {
        // 参数位也当匹配贡献
        sim_tokens += param_count;
    }

    return {static_cast<float>(sim_tokens) / content1.size(), param_count};
}

TContent DrainLogParser::_create_template(const TContent& content1, const TContent& content2)
{
    TContent new_content {content1};
    for (auto&& [token1, token2] : std::views::zip(new_content, content2))
    {
        if (token1 != token2)
        {
            token1 = WILDCARD;
        }
    }

    return new_content;
}

std::string DrainLogParser::LogCluster::get_template() const
{
    return this->content |
           std::views::join_with(' ') |
           std::ranges::to<std::string>();
}

}    // namespace logtt