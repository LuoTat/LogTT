#pragma once

#include <string>
#include <vector>

namespace logparser
{

using Token    = std::string;
using TContent = std::vector<Token>;

inline constexpr std::string WILDCARD {"<#*#>"};

}    // namespace logparser