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
        std::vector<Mask>        masks,
        std::vector<char>        delimiters,
        std::uint32_t            cluster_thr,
        float                    merge_thr
    );

    AELLogParser(const AELLogParser&)            = delete;
    AELLogParser& operator=(const AELLogParser&) = delete;

    AELLogParser(AELLogParser&&) noexcept            = default;
    AELLogParser& operator=(AELLogParser&&) noexcept = default;

    virtual ~AELLogParser() = default;

    std::int32_t parse(
        const std::string& log_file, const std::string& structured_table_name, const std::string& templates_table_name
    ) override;

private:
    struct LogCluster
    {
        TContent                  content;
        std::vector<std::int64_t> rows;
        bool                      merged {false};

        [[nodiscard]]
        std::string get_template() const;
    };

    struct LogBinKey
    {
        std::int64_t token_count {0};
        std::int64_t para_count {0};

        bool operator==(const LogBinKey&) const = default;
    };

    friend std::size_t hash_value(const LogBinKey& key)
    {
        std::size_t seed {0};
        boost::hash_combine(seed, key.token_count);
        boost::hash_combine(seed, key.para_count);
        return seed;
    }

    using LogBin = std::unordered_map<LogBinKey, std::vector<LogCluster*>, boost::hash<LogBinKey>>;

    LogBin                   _get_log_bins(const shared_ptr<Relation>& rel);
    std::vector<LogCluster*> _reconcile(LogBin& log_bin);
    bool                     _has_diff(const LogCluster* cluster1, const LogCluster* cluster2) const;
    static LogCluster*       _merge_log_cluster(LogCluster* cluster1, const LogCluster* cluster2);

    std::uint32_t          m_cluster_thr {2};
    float                  m_merge_thr {1.0F};
    std::deque<LogCluster> m_cluster_pool;
};


}    // namespace logtt
