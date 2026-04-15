#include "brain_log_parser.hxx"
#include "duckdb_service.hxx"
#include "utils.hxx"
#include <algorithm>
#include <ranges>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace logtt
{

BrainLogParser::FCounter::FCounter(const FContent& fcontent)
{
    // 用桶计数代替哈希表：freq 值的上界 = 同组行数，通常不大，连续内存访问更快
    std::uint32_t max_freq {0};
    for (auto&& ftoken : fcontent)
    {
        max_freq = std::max(max_freq, ftoken.freq);
    }

    // bucket[f] = freq 值为 f 的 token 数量
    std::vector<std::uint16_t> bucket(max_freq + 1, 0);
    for (auto&& ftoken : fcontent)
    {
        ++bucket[ftoken.freq];
    }

    // 收集非零项
    for (auto&& [freq, count] : std::views::enumerate(bucket))
    {
        if (count > 0)
        {
            freq_counter.emplace_back(freq, count);
        }
    }
}

void BrainLogParser::FCounter::sort_by_count()
{
    // 默认按 count 降序排列
    std::ranges::sort(freq_counter, std::greater {}, &FTuple::count);
}

std::uint32_t BrainLogParser::FCounter::get_max_freq() const
{
    return std::ranges::max(freq_counter, {}, &FTuple::freq).freq;
}

BrainLogParser::BrainLogParser(
    std::string              log_regex,
    std::vector<std::string> named_fields,
    std::vector<std::string> timestamp_fields,
    std::string              timestamp_format,
    std::vector<Mask>        maskings,
    std::vector<char>        delimiters,
    std::uint16_t            var_thr
):
    BaseLogParser {std::move(log_regex), std::move(named_fields), std::move(timestamp_fields), std::move(timestamp_format), std::move(maskings), std::move(delimiters)},
    m_var_thr {var_thr}
{}

std::uint32_t BrainLogParser::parse(const std::string& log_file, const std::string& structured_table_name, const std::string& templates_table_name, bool keep_para)
{
    std::vector<std::string> templates;
    // 获取数据库连接
    auto& conn {get_connection()};

    auto rel {load_data(conn, log_file, this->m_log_regex, this->m_named_fields, this->m_timestamp_fields, this->m_timestamp_format)};
    rel = mask_log_rel(rel, this->m_maskings);
    rel = split_log_rel(rel, this->m_delimiters);

    // 缓存分词结果，避免重复计算
    rel = rel->Project("* EXCLUDE MaskedContent");
    rel = get_tmp(conn, rel);

    // 从 DuckDB 读取所有分词结果
    auto                  result {to_materialized_query_result(rel->Project("Tokens")->Execute())};
    auto                  log_length {result->RowCount()};
    std::vector<TContent> contents;
    contents.reserve(log_length);
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
            contents.push_back(std::move(content));
        }
    }

    // Brain 核心算法
    auto fcontents_group {BrainLogParser::_get_fcontents_group(contents)};
    auto fcounters_group {BrainLogParser::_get_fcounters_group(fcontents_group)};

    // 通过 key 显式对齐两个 group
    for (auto&& [length, fcontents] : fcontents_group)
    {
        auto root_rows {BrainLogParser::_find_root(fcounters_group[length], 0.5F)};

        BrainLogParser::_up_split(root_rows, fcontents);
        BrainLogParser::_down_split(root_rows, fcontents, this->m_var_thr);
    }

    // 输出结果：将每行的 FToken 拼接成模板字符串
    templates.resize(log_length);
    for (auto&& [_, fcontents] : fcontents_group)
    {
        for (auto&& fcontent : fcontents)
        {
            // fcontent[0].row 是该行在原始日志中的行号（从 0 开始）
            templates[fcontent[0].row] = fcontent |
                                         std::views::transform(&FToken::token) |
                                         std::views::join_with(' ') |
                                         std::ranges::to<std::string>();
        }
    }

    // 移除多余列
    rel = rel->Project("* EXCLUDE Tokens");
    to_table(conn, rel, templates, structured_table_name, templates_table_name, keep_para);

    return log_length;
}

BrainLogParser::FContentsGroup BrainLogParser::_get_fcontents_group(const std::vector<TContent>& contents)
{
    FContentsGroup fcontents_group;

    // 按 token 数量分组，每行生成一个 FContent
    for (auto&& [row, content] : std::views::enumerate(contents))
    {
        auto     length {static_cast<std::uint16_t>(content.size())};
        FContent fcontent;
        fcontent.reserve(content.size());
        for (auto&& [col, token] : std::views::enumerate(content))
        {
            fcontent.emplace_back(row, static_cast<std::uint16_t>(col), token);
        }
        fcontents_group[length].push_back(std::move(fcontent));
    }

    // 统计同组内每列每个 token 的出现次数，写回 freq
    for (auto&& [length, fcontents] : fcontents_group)
    {
        auto num_cols {length};
        auto num_rows {fcontents.size()};

        // 逐列遍历所有行，用 unordered_map<string_view, uint32_t> 计数
        for (auto&& col : std::views::iota(0U, num_cols))
        {
            std::unordered_map<std::string_view, std::uint32_t> col_counter;
            col_counter.reserve(num_rows);

            for (auto&& fcontent : fcontents)
            {
                ++col_counter[fcontent[col].token];
            }

            // 将频率写回每个 FToken
            for (auto&& fcontent : fcontents)
            {
                fcontent[col].freq = col_counter[fcontent[col].token];
            }
        }
    }

    return fcontents_group;
}

BrainLogParser::FCountersGroup BrainLogParser::_get_fcounters_group(const FContentsGroup& fcontents_group)
{
    FCountersGroup fcounters_group;

    for (auto&& [length, fcontents] : fcontents_group)
    {
        auto& fcounters {fcounters_group[length]};
        fcounters.reserve(fcontents.size());
        for (auto&& fcontent : fcontents)
        {
            fcounters.emplace_back(fcontent);
        }
    }

    return fcounters_group;
}

BrainLogParser::RootRows BrainLogParser::_find_root(std::vector<FCounter>& fcounters, float alpha)
{
    RootRows root_rows;

    for (auto&& [idx, fcounter] : std::views::enumerate(fcounters))
    {
        fcounter.sort_by_count();
        auto freq_thr {fcounter.get_max_freq() * alpha};

        // 默认选出现次数最多的 FTuple
        auto matched {fcounter.freq_counter[0]};
        for (auto&& ftuple : fcounter.freq_counter)
        {
            if (ftuple.freq >= freq_thr)
            {
                matched = ftuple;
                break;
            }
        }
        root_rows[matched].push_back(idx);
    }

    return root_rows;
}

void BrainLogParser::_up_split(const RootRows& root_rows, std::vector<FContent>& fcontents)
{
    for (auto&& [root, rows] : root_rows)
    {
        std::unordered_map<std::uint16_t, std::uint32_t>             col_max_freq;
        std::unordered_map<std::uint16_t, std::unordered_set<Token>> col_tokens;

        for (auto&& row : rows)
        {
            for (auto&& ftoken : fcontents[row])
            {
                col_max_freq[ftoken.col] = std::max(col_max_freq[ftoken.col], ftoken.freq);
                col_tokens[ftoken.col].insert(ftoken.token);
            }
        }

        // 获取父节点列：最大频率大于 root.freq 的列
        std::unordered_set<std::uint16_t> variable_parent_cols;
        for (auto&& [col, max_freq] : col_max_freq)
        {
            // 父节点列中有多个不同词的列 → 变量列
            if (max_freq > root.freq && col_tokens[col].size() > 1)
            {
                variable_parent_cols.insert(col);
            }
        }

        // 将变量列的词置为 <#*#>
        for (auto&& row : rows)
        {
            for (auto&& ftoken : fcontents[row])
            {
                if (variable_parent_cols.contains(ftoken.col))
                {
                    ftoken.token = WILDCARD;
                }
            }
        }
    }
}

void BrainLogParser::_down_split(const RootRows& root_rows, std::vector<FContent>& fcontents, std::uint16_t var_thr)
{
    for (auto&& [root, rows] : root_rows)
    {
        std::unordered_map<std::uint16_t, std::uint32_t>             col_max_freq;
        std::unordered_map<std::uint16_t, std::unordered_set<Token>> col_tokens;

        for (auto&& row : rows)
        {
            for (auto&& ftoken : fcontents[row])
            {
                col_max_freq[ftoken.col] = std::max(col_max_freq[ftoken.col], ftoken.freq);
                col_tokens[ftoken.col].insert(ftoken.token);
            }
        }

        // 获取子节点列：最大频率小于 root.freq 的列
        std::vector<std::uint16_t> child_cols;
        for (auto&& [col, max_freq] : col_max_freq)
        {
            if (max_freq < root.freq)
            {
                child_cols.push_back(col);
            }
        }

        // 按不同词数量降序排列子节点列，优先处理不同词数量多的列，更快达到变量列条件
        std::ranges::sort(
            child_cols,
            {},
            [&col_tokens](std::uint16_t col)
            {
                auto it {col_tokens.find(col)};
                return it != col_tokens.end() ? it->second.size() : 0U;
            }
        );

        // 获取变量列：子节点列中不同词数量 >= var_thr 的列
        std::unordered_set<std::uint16_t> variable_child_cols;
        for (auto&& col : child_cols)
        {
            if (col_tokens[col].size() >= var_thr)
            {
                variable_child_cols.insert(col);
            }
        }

        // 将变量列的词置为 <#*#>
        for (auto&& row : rows)
        {
            for (auto&& ftoken : fcontents[row])
            {
                if (variable_child_cols.contains(ftoken.col))
                {
                    ftoken.token = WILDCARD;
                }
            }
        }
    }
}

}    // namespace logtt
