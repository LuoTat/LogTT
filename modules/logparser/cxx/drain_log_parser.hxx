#pragma once

#include "base_log_parser.hxx"
#include "precomp.hxx"
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace logparser
{

class DrainLogParser: BaseLogParser
{
public:
    DrainLogParser() = default;
    DrainLogParser(
        std::string              log_regex,
        std::vector<std::string> named_fields,
        std::vector<Mask>        maskings,
        std::vector<char>        delimiters,
        uint16_t                 depth,
        uint16_t                 children,
        float                    sim_thr
    );

    DrainLogParser(const DrainLogParser&)            = delete;
    DrainLogParser& operator=(const DrainLogParser&) = delete;

    DrainLogParser(DrainLogParser&&) noexcept            = default;
    DrainLogParser& operator=(DrainLogParser&&) noexcept = default;

    std::size_t parse(
        const std::string& log_file,
        const std::string& structured_table_name,
        const std::string& templates_table_name,
        bool               keep_para
    ) override;

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
    std::unique_ptr<Node>  m_root;
    std::deque<LogCluster> m_cluster_pool;
};

}    // namespace logparser