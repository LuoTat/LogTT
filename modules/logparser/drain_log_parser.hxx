#pragma once

#include "precomp.hxx"
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace logparser
{

class DrainLogParser
{
public:
    DrainLogParser() = default;
    DrainLogParser(uint16_t depth, uint16_t children, float sim_thr);

    DrainLogParser(const DrainLogParser&)            = delete;
    DrainLogParser& operator=(const DrainLogParser&) = delete;

    DrainLogParser(DrainLogParser&&) noexcept            = default;
    DrainLogParser& operator=(DrainLogParser&&) noexcept = default;

    ~DrainLogParser() = default;

    // 返回解析后的日志模板列表
    void parse(const std::string& log_file, const std::string& log_format, const std::vector<std::string>& group_names);

private:
    struct LogCluster
    {
        TContent content;

        std::string get_template() const;
    };

    struct Node
    {
        std::vector<LogCluster*>                         clusters;
        std::unordered_map<Token, std::unique_ptr<Node>> children_node;
    };

    LogCluster* _add_content(const TContent& content);
    LogCluster* _tree_search(const TContent& content, bool include_params = false);
    LogCluster* _fast_match(const TContent& content, const Node* node, bool include_params = false);
    void        _add_to_prefix_tree(LogCluster* cluster);

    static std::pair<float, uint16_t> _get_distance(const TContent& content1, const TContent& content2, bool include_params = false);
    static TContent                   _create_template(const TContent& content1, const TContent& content2);

    uint16_t               m_depth {4};
    uint16_t               m_children {100};
    float                  m_sim_thr {0.4F};
    std::unique_ptr<Node>  m_root {};
    std::deque<LogCluster> m_cluster_pool {};
};

}    // namespace logparser