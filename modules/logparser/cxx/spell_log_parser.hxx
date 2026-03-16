#pragma once

#include "base_log_parser.hxx"
#include "precomp.hxx"
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace logparser
{

class SpellLogParser: BaseLogParser
{
public:
    SpellLogParser() = default;
    SpellLogParser(
        std::string              log_regex,
        std::vector<std::string> named_fields,
        std::vector<Mask>        maskings,
        std::vector<char>        delimiters,
        float                    sim_thr
    );

    SpellLogParser(const SpellLogParser&)            = delete;
    SpellLogParser& operator=(const SpellLogParser&) = delete;

    SpellLogParser(SpellLogParser&&) noexcept            = default;
    SpellLogParser& operator=(SpellLogParser&&) noexcept = default;

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
        LogCluster*                                      cluster;
        std::size_t                                      template_no {0};
        std::unordered_map<Token, std::unique_ptr<Node>> children_node;
    };

    LogCluster* _add_content(const TContent& content);
    LogCluster* _tree_subseq_match(const TContent& content);
    LogCluster* _subseq_match(const TContent& content);
    LogCluster* _lcs_match(const TContent& content);
    void        _add_seq_to_prefix_tree(LogCluster* cluster);
    void        _remove_seq_from_prefix_tree(const LogCluster* cluster);

    static bool        _is_subsequence(const TContent& source, const TContent& target);
    static std::size_t _lcs_length(const TContent& content1, const TContent& content2, std::size_t min_required_lcs);
    static TContent    _lcs_content(const TContent& content1, const TContent& content2);
    static TContent    _create_template(const TContent& lcs, const TContent& content);
    static TContent    _merge_wildcards(const TContent& content);

    float                  m_sim_thr {0.5F};
    std::unique_ptr<Node>  m_root;
    std::deque<LogCluster> m_cluster_pool;
};

}    // namespace logparser
