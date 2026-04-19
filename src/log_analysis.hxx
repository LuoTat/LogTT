#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace logtt
{

// ==================== 日志分析相关函数 ====================

bool has_column(const std::string& structured_table_name, const std::string& column_name);

std::vector<std::pair<std::string, std::uint32_t>> get_level_distribution(const std::string& structured_table_name);

std::vector<std::pair<std::int64_t, std::uint32_t>> get_log_frequency_distribution(
    const std::string& structured_table_name,
    std::uint32_t       months,
    std::uint32_t       days,
    std::uint64_t       micros
);

}    // namespace logtt
