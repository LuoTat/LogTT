#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace logtt
{

// ==================== 日志分析相关函数 ====================

std::pair<std::vector<std::string>, std::vector<std::int64_t>>
get_level_distribution(const std::string& structured_table_name);

std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> get_log_frequency_distribution(
    const std::string& structured_table_name, std::int32_t months, std::int32_t days, std::int64_t micros
);

std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> get_template_frequency_distribution(
    const std::string& structured_table_name, std::int32_t months, std::int32_t days, std::int64_t micros
);

std::unordered_map<std::string, std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>>>
get_log_level_frequency_distribution(
    const std::string& structured_table_name, std::int32_t months, std::int32_t days, std::int64_t micros
);

std::pair<std::int64_t, std::vector<std::vector<std::int64_t>>>
get_template_transition_matrix(const std::string& structured_table_name, const std::string& template_table_name);

std::pair<std::int64_t, std::vector<std::vector<std::int64_t>>> get_template_cooccurrence_matrix(
    const std::string& structured_table_name,
    const std::string& template_table_name,
    std::int32_t       months,
    std::int32_t       days,
    std::int64_t       micros
);

}    // namespace logtt
