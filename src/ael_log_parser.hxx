#pragma once

#include "base_log_parser.hxx"
#include "precomp.hxx"
#include <boost/container_hash/hash.hpp>
#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

namespace logtt
{

class AELLogParser: BaseLogParser
{
public:
    AELLogParser() = default;
    AELLogParser(
        std::string              log_regex,
        std::vector<std::string> named_fields,
        std::vector<std::string> timestamp_fields,
        std::string              timestamp_format,
        std::vector<Mask>        maskings,
        std::vector<char>        delimiters,
        std::uint32_t            cluster_thr,
        float                    merge_thr
    );

    AELLogParser(const AELLogParser&)            = delete;
    AELLogParser& operator=(const AELLogParser&) = delete;

    AELLogParser(AELLogParser&&) noexcept            = default;
    AELLogParser& operator=(AELLogParser&&) noexcept = default;

    std::uint32_t parse(
        const std::string& log_file,
        const std::string& structured_table_name,
        const std::string& templates_table_name,
        bool               keep_para
    ) override;

private:
    struct LogCluster
    {
        TContent                  content;
        std::vector<std::int64_t> rows;
        bool                      merged {false};

        std::string get_template() const;
    };

    struct LogBinKey
    {
        std::int64_t m_token_count {0};
        std::int64_t m_para_count {0};

        bool operator==(const LogBinKey&) const = default;
    };

    inline friend std::size_t hash_value(const LogBinKey& k)
    {
        std::size_t seed {0};
        boost::hash_combine(seed, k.m_token_count);
        boost::hash_combine(seed, k.m_para_count);
        return seed;
    }

    using LogBin = std::unordered_map<LogBinKey, std::vector<LogCluster*>, boost::hash<LogBinKey>>;

    LogBin                   _get_log_bins(const shared_ptr<Relation>& rel);
    std::vector<LogCluster*> _reconcile(LogBin& log_bin);
    bool                     _has_diff(const LogCluster* cluster1, const LogCluster* cluster2);
    static LogCluster*       _merge_log_cluster(LogCluster* cluster1, const LogCluster* cluster2);

    std::uint32_t          m_cluster_thr {2};
    float                  m_merge_thr {1.0F};
    std::deque<LogCluster> m_cluster_pool;
};


}    // namespace logtt
