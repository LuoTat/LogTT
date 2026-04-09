#pragma once

#include "base_log_parser.hxx"
#include "precomp.hxx"
#include <boost/container_hash/hash.hpp>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace logtt
{

class BrainLogParser: BaseLogParser
{
public:
    BrainLogParser() = default;
    BrainLogParser(
        std::string              log_regex,
        std::vector<std::string> named_fields,
        std::vector<Mask>        maskings,
        std::vector<char>        delimiters,
        std::uint16_t            var_thr
    );

    BrainLogParser(const BrainLogParser&)            = delete;
    BrainLogParser& operator=(const BrainLogParser&) = delete;

    BrainLogParser(BrainLogParser&&) noexcept            = default;
    BrainLogParser& operator=(BrainLogParser&&) noexcept = default;

    std::uint32_t parse(
        const std::string& log_file,
        const std::string& structured_table_name,
        const std::string& templates_table_name,
        bool               keep_para
    ) override;

private:
    // 带频率信息的 Token，记录该 token 在同组同列中出现的次数
    struct FToken
    {
        std::uint32_t row;
        std::uint16_t col;
        Token         token;
        std::uint32_t freq {1};
    };

    // 频率-计数对：freq 为某列中 token 出现的频率，count 为该频率在一行中出现的次数
    struct FTuple
    {
        std::uint32_t freq;
        std::uint16_t count;

        bool operator==(const FTuple&) const = default;
    };

    inline friend std::size_t hash_value(const FTuple& t)
    {
        std::size_t seed {0};
        boost::hash_combine(seed, t.freq);
        boost::hash_combine(seed, t.count);
        return seed;
    }

    using FContent = std::vector<FToken>;

    // 统计一行 FContent 中各频率出现的次数
    struct FCounter
    {
        std::vector<FTuple> freq_counter;

        explicit FCounter(const FContent& fcontent);

        void          sort_by_count();
        std::uint32_t get_max_freq() const;
    };

    using FContentsGroup = std::unordered_map<std::uint16_t, std::vector<FContent>>;
    using FCountersGroup = std::unordered_map<std::uint16_t, std::vector<FCounter>>;
    using RootRows       = std::unordered_map<FTuple, std::vector<std::uint32_t>, boost::hash<FTuple>>;

    static FContentsGroup _get_fcontents_group(const std::vector<TContent>& contents);
    static FCountersGroup _get_fcounters_group(const FContentsGroup& fcontents_group);
    static RootRows       _find_root(std::vector<FCounter>& fcounters, float alpha = 0.5F);
    static void           _up_split(const RootRows& root_rows, std::vector<FContent>& fcontents);
    static void           _down_split(const RootRows& root_rows, std::vector<FContent>& fcontents, std::uint16_t var_thr);

    std::uint16_t m_var_thr {2};
};

}    // namespace logtt
