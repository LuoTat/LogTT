#include "drain_log_parser.hxx"
#include "utils.hxx"
#include <ranges>

namespace logparser
{

DrainLogParser::DrainLogParser(uint16_t depth, uint16_t children, float sim_thr):
    m_depth(depth), m_children(children), m_sim_thr(sim_thr)
{}

// std::vector<std::string> DrainLogParser::parse(const std::vector<TContent>& logs)
// {
//     // Initialize the prefix tree
//     this->m_root = std::make_unique<Node>();
//     this->m_cluster_pool.clear();

//     std::vector<LogCluster*> cluster_results;
//     cluster_results.reserve(logs.size());

//     for (const auto& log : logs)
//     {
//         cluster_results.push_back(this->_add_content(log));
//     }

//     std::vector<std::string> templates;
//     templates.reserve(logs.size());

//     for (const auto& cluster : cluster_results)
//     {
//         templates.push_back(cluster->get_template());
//     }

//     return templates;
// }

void DrainLogParser::parse(const std::string& log_file, const std::string& log_format, const std::vector<std::string>& group_names)
{
    auto relation = load_data(log_file, log_format, group_names);
}

DrainLogParser::LogCluster* DrainLogParser::_add_content(const TContent& content)
{
    auto match_cluster {this->_tree_search(content)};

    // Match no existing log cluster
    if (!match_cluster)
    {
        this->m_cluster_pool.emplace_back(content);
        match_cluster = &this->m_cluster_pool.back();
        this->_add_to_prefix_tree(match_cluster);
    }
    else
    {
        // Add the new log message to the existing cluster
        auto new_content {DrainLogParser::_create_template(content, match_cluster->content)};
        if (new_content != match_cluster->content)
        {
            match_cluster->content = new_content;
        }
    }

    return match_cluster;
}

DrainLogParser::LogCluster* DrainLogParser::_tree_search(const TContent& content, bool include_params)
{
    auto length {content.size()};
    auto length_token = std::to_string(length);
    auto cur_node {this->m_root.get()};

    // for (const auto& [i, token] : std::views::enumerate(std::views::concat(std::views::single(length_token), content)))
    // {
    //     auto cur_node_depth {static_cast<size_t>(i) + 1};

    //     // at max depth or this is last token
    //     if (cur_node_depth == this->m_depth || cur_node_depth == length + 2)
    //     {
    //         // get best match among all clusters with same prefix, or None if no match is above sim_th
    //         break;
    //     }

    //     if (auto it {cur_node->children_node.find(token)}; it == cur_node->children_node.end())
    //     {
    //         it = cur_node->children_node.find(WILDCARD);
    //         if (it == cur_node->children_node.end())
    //         {
    //             // no wildcard node exist
    //             return nullptr;
    //         }
    //         cur_node = it->second.get();
    //     }
    //     else
    //     {
    //         cur_node = it->second.get();
    //     }
    // }

    return this->_fast_match(content, cur_node, include_params);
}

DrainLogParser::LogCluster* DrainLogParser::_fast_match(const TContent& content, const Node* node, bool include_params)
{
    float       max_sim {-1.0F};
    uint16_t    max_param_count {0};
    LogCluster* max_cluster {};

    for (const auto& cluster : node->clusters)
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
    auto length_token = std::to_string(length);
    auto cur_node {this->m_root.get()};

    // for (const auto& [i, token] : std::views::enumerate(std::views::concat(std::views::single(length_token), cluster->content)))
    // {
    //     auto cur_node_depth {static_cast<size_t>(i) + 1};

    //     // if at max depth or this is last token in template - add current log cluster to the leaf node
    //     if (cur_node_depth == this->m_depth || cur_node_depth == length + 2)
    //     {
    //         // at max depth or this is last token
    //         cur_node->clusters.push_back(cluster);
    //         return;
    //     }

    //     if (auto it {cur_node->children_node.find(token)}; it == cur_node->children_node.end())
    //     {
    //         // if token not matched in this layer of existing tree.
    //         if (cur_node->children_node.size() + 1 < this->m_children)
    //         {
    //             // 如果当前节点不是最后一个节点，就添加一个新的节点
    //             auto [it, _] = cur_node->children_node.emplace(token, std::make_unique<Node>());
    //             cur_node     = it->second.get();
    //         }
    //         else if (cur_node->children_node.size() + 1 == this->m_children)
    //         {
    //             // 如果当前节点是最后一个节点，就添加一个新的通配符节点
    //             auto [it, _] = cur_node->children_node.emplace(WILDCARD, std::make_unique<Node>());
    //             cur_node     = it->second.get();
    //         }
    //         else
    //         {
    //             // 如果当前节点已满，就直接使用通配符节点
    //             cur_node = cur_node->children_node[WILDCARD].get();
    //         }
    //     }
    //     else
    //     {
    //         // if the token is matched
    //         cur_node = it->second.get();
    //     }
    // }
}

std::pair<float, uint16_t> DrainLogParser::_get_distance(const TContent& content1, const TContent& content2, bool include_params)
{
    // list are empty - full match
    if (content1.size() == 0)
    {
        return {1.0F, 0};
    }

    uint16_t sim_tokens {0};
    uint16_t param_count {0};

    for (const auto& [token1, token2] : std::views::zip(content1, content2))
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
    TContent new_content {};
    new_content.reserve(content1.size());

    for (const auto& [token1, token2] : std::views::zip(content1, content2))
    {
        if (token1 == token2)
        {
            new_content.push_back(token1);
        }
        else
        {
            new_content.push_back(WILDCARD);
        }
    }

    return new_content;
}

std::string DrainLogParser::LogCluster::get_template() const
{
    auto view = this->content | std::views::join_with(' ');
    return std::string(view.begin(), view.end());
}

}    // namespace logparser