#pragma once

#include <string>
#include <vector>

namespace logparser
{

using Mask     = std::pair<std::string, std::string>;
using Token    = std::string;
using TContent = std::vector<Token>;

inline constexpr std::string WILDCARD {"<#*#>"};
inline constexpr std::string DB_PATH {"logtt.duckdb"};

}    // namespace logparser