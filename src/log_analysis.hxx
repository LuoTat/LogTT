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

}    // namespace logtt
